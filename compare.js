      
#!/usr/bin/env node

const { spawn } = require('child_process');
const fs = require('fs').promises;
const path = require('path');
const { pathToFileURL } = require('url');

// Configuration
const CONFIG = {
  pollInterval: 2000, // 2 seconds
  maxRetries: 300, // 10 minutes max wait time
  maxQueryLength: 250000 // Athena max is 256KB, use 250KB for safety
};

// Parse command line arguments
function parseArgs() {
  const args = process.argv.slice(2);

  if (args.length < 4) {
    console.error('Usage: node compare-athena-tables.js <tableA> <tableB> <join_columns_file> <compare_columns_file> [workgroup] [filter] [--no-adjustment]');
    console.error('Example: node compare-athena-tables.js database_x.table_a database_y.table_b join_cols.txt compare_cols.txt primary "date >= DATE \'2024-01-01\'"');
    console.error('Note: Tables should be in format "database.table" (e.g., X.A means database X, table A)');
    console.error('Note: Adjustment filtering is ON by default. Use --no-adjustment to disable.');
    process.exit(1);
  }

  // Check if last argument is --no-adjustment flag
  const skipAdjustment = args[6] === '--no-adjustment';

  return {
    tableA: args[0],
    tableB: args[1],
    joinColumnsFile: args[2],
    compareColumnsFile: args[3],
    workgroup: args[4] || 'primary',
    filter: args[5] || null,
    skipAdjustment: skipAdjustment
  };
}

// Read file and return lines as array
async function readColumnFile(filePath) {
  try {
    const content = await fs.readFile(filePath, 'utf-8');
    return content.split('\n')
      .map(line => line.trim())
      .filter(line => line.length > 0);
  } catch (error) {
    console.error(`Error reading file ${filePath}:`, error.message);
    process.exit(1);
  }
}

// Execute AWS CLI command
function executeAwsCommand(args) {
  return new Promise((resolve, reject) => {
    const awsProcess = spawn('aws', args);
    let stdout = '';
    let stderr = '';

    awsProcess.stdout.on('data', (data) => {
      stdout += data.toString();
    });

    awsProcess.stderr.on('data', (data) => {
      stderr += data.toString();
    });

    awsProcess.on('close', (code) => {
      if (code !== 0) {
        reject(new Error(`AWS CLI command failed: ${stderr}`));
      } else {
        try {
          resolve(JSON.parse(stdout));
        } catch (e) {
          resolve(stdout);
        }
      }
    });

    awsProcess.on('error', (error) => {
      reject(new Error(`Failed to execute AWS CLI: ${error.message}`));
    });
  });
}

// Build the comparison SQL query
function buildComparisonQuery(tableA, tableB, joinColumns, compareColumns, filter = null, skipAdjustment = false) {
  const joinColumnsStr = joinColumns.join(', ');
  const joinCondition = joinColumns.map(col => `tbl_a.${col} = tbl_b.${col}`).join(' AND ');
  const joinConditionLeft = joinColumns.map(col => `tbl_a.${col} = duplicates_a.${col}`).join(' AND ');
  const joinConditionRight = joinColumns.map(col => `tbl_b.${col} = duplicates_b.${col}`).join(' AND ');

  // Build filter clause for CTEs
  const filterClauseWhere = filter ? `WHERE ${filter}` : '';

  // Extract table name from tableA (format: database.table)
  const tableAName = tableA.split('.')[1];
  const adjustmentTable = `ecidtas_adjustment_data.${tableAName}`;

  // Build adjustment join condition
  const adjustmentJoinCondition = joinColumns.map(col => `tbl_source.${col} = adj.${col}`).join(' AND ');

  // For Union 4: Create a condensed diff_columns output instead of individual CASE statements
  // This dramatically reduces query length
  // Use COALESCE to display 'NULL' string instead of NULL values in output
  const diffColumnsArray = compareColumns.map(col => {
    return `IF(tbl_a.${col} = tbl_b.${col} OR (tbl_a.${col} IS NULL AND tbl_b.${col} IS NULL), NULL,
      CONCAT('${col}:', COALESCE(CAST(tbl_a.${col} AS VARCHAR), 'NULL'), ' X ', COALESCE(CAST(tbl_b.${col} AS VARCHAR), 'NULL')))`;
  }).join(',\n      ');

  const compareColumnsNull = compareColumns.map(col => `NULL AS ${col}`).join(', ');

  // Build adjustment CTE if adjustment filtering is enabled
  const adjustmentCTE = skipAdjustment ? '' : `,
-- Filter adjustment table
filtered_adjustment AS (
  SELECT ${joinColumnsStr}
  FROM ${adjustmentTable}
  ${filterClauseWhere}
)`;

  const query = `
-- Filter tables BEFORE any joins or comparisons
WITH filtered_a AS (
  SELECT * FROM ${tableA}
  ${filterClauseWhere}
),
filtered_b AS (
  SELECT * FROM ${tableB}
  ${filterClauseWhere}
),
-- Pre-calculate duplicates from filtered tables
duplicates_a AS (
  SELECT ${joinColumnsStr}
  FROM filtered_a
  GROUP BY ${joinColumnsStr}
  HAVING COUNT(*) > 1
),
duplicates_b AS (
  SELECT ${joinColumnsStr}
  FROM filtered_b
  GROUP BY ${joinColumnsStr}
  HAVING COUNT(*) > 1
)${adjustmentCTE}

-- Union 1: Records in A but not in B
SELECT
  ${joinColumns.map(col => `tbl_a.${col}`).join(', ')},
  'missing in ${tableB}' AS remarks,
  NULL AS diff_columns
FROM filtered_a tbl_a
WHERE NOT EXISTS (
  SELECT 1 FROM filtered_b tbl_b
  WHERE ${joinCondition}
)
AND NOT EXISTS (
  SELECT 1 FROM duplicates_a
  WHERE ${joinConditionLeft}
)${skipAdjustment ? '' : `
AND NOT EXISTS (
  SELECT 1 FROM filtered_adjustment adj
  WHERE ${joinColumns.map(col => `tbl_a.${col} = adj.${col}`).join(' AND ')}
)`}

UNION ALL

-- Union 2: Records in B but not in A
SELECT
  ${joinColumns.map(col => `tbl_b.${col}`).join(', ')},
  'missing in ${tableA}' AS remarks,
  NULL AS diff_columns
FROM filtered_b tbl_b
WHERE NOT EXISTS (
  SELECT 1 FROM filtered_a tbl_a
  WHERE ${joinCondition}
)
AND NOT EXISTS (
  SELECT 1 FROM duplicates_b
  WHERE ${joinConditionRight}
)${skipAdjustment ? '' : `
AND NOT EXISTS (
  SELECT 1 FROM filtered_adjustment adj
  WHERE ${joinColumns.map(col => `tbl_b.${col} = adj.${col}`).join(' AND ')}
)`}

UNION ALL

-- Union 3a: Duplicate keys in table A
SELECT
  ${joinColumns.map(col => `tbl_a.${col}`).join(', ')},
  'duplicate key in ${tableA}' AS remarks,
  NULL AS diff_columns
FROM filtered_a tbl_a
WHERE EXISTS (
  SELECT 1 FROM duplicates_a
  WHERE ${joinConditionLeft}
)${skipAdjustment ? '' : `
AND NOT EXISTS (
  SELECT 1 FROM filtered_adjustment adj
  WHERE ${joinColumns.map(col => `tbl_a.${col} = adj.${col}`).join(' AND ')}
)`}

UNION ALL

-- Union 3b: Duplicate keys in table B
SELECT
  ${joinColumns.map(col => `tbl_b.${col}`).join(', ')},
  'duplicate key in ${tableB}' AS remarks,
  NULL AS diff_columns
FROM filtered_b tbl_b
WHERE EXISTS (
  SELECT 1 FROM duplicates_b
  WHERE ${joinConditionRight}
)${skipAdjustment ? '' : `
AND NOT EXISTS (
  SELECT 1 FROM filtered_adjustment adj
  WHERE ${joinColumns.map(col => `tbl_b.${col} = adj.${col}`).join(' AND ')}
)`}

UNION ALL

-- Union 4: Matched records with column differences
SELECT
  ${joinColumns.map(col => `tbl_a.${col}`).join(', ')},
  'matched' AS remarks,
  ARRAY_JOIN(
    ARRAY[
      ${diffColumnsArray}
    ],
    '; '
  ) AS diff_columns
FROM filtered_a tbl_a
INNER JOIN filtered_b tbl_b ON ${joinCondition}
WHERE NOT EXISTS (
  SELECT 1 FROM duplicates_a
  WHERE ${joinConditionLeft}
)
AND NOT EXISTS (
  SELECT 1 FROM duplicates_b
  WHERE ${joinConditionRight}
)${skipAdjustment ? '' : `
AND NOT EXISTS (
  SELECT 1 FROM filtered_adjustment adj
  WHERE ${joinColumns.map(col => `tbl_a.${col} = adj.${col}`).join(' AND ')}
)`}
AND (
  ${compareColumns.map(col => `tbl_a.${col} != tbl_b.${col} OR (tbl_a.${col} IS NULL AND tbl_b.${col} IS NOT NULL) OR (tbl_a.${col} IS NOT NULL AND tbl_b.${col} IS NULL)`).join('\n  OR ')}
)

ORDER BY ${joinColumnsStr}
`;

  return query.trim();
}

// Start Athena query execution
async function startQueryExecution(query, workgroup) {
  console.log('Starting Athena query execution...');

  // Write query to temp file to avoid command-line length limits (ENAMETOOLONG)
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  const tempQueryFile = path.resolve(`temp_query_${timestamp}.sql`);
  await fs.writeFile(tempQueryFile, query, 'utf-8');

  try {
    // AWS CLI expects file:// followed by path
    // On Windows: file://C:\path\to\file or file://C:/path/to/file
    // On Unix: file:///path/to/file
    // Use file:// with forward slashes for cross-platform compatibility
    const normalizedPath = tempQueryFile.replace(/\\/g, '/');
    const fileParam = `file://${normalizedPath}`;

    const args = [
      'athena',
      'start-query-execution',
      '--query-string', fileParam,
      '--work-group', workgroup
    ];

    const result = await executeAwsCommand(args);
    return result.QueryExecutionId;
  } finally {
    // Clean up temp file
    await fs.unlink(tempQueryFile).catch(() => {});
  }
}

// Poll query status
async function waitForQueryCompletion(queryExecutionId) {
  console.log(`Waiting for query ${queryExecutionId} to complete...`);

  let retries = 0;

  while (retries < CONFIG.maxRetries) {
    const args = [
      'athena',
      'get-query-execution',
      '--query-execution-id', queryExecutionId
    ];

    const result = await executeAwsCommand(args);
    const status = result.QueryExecution.Status.State;

    process.stdout.write(`\rStatus: ${status} (${retries * CONFIG.pollInterval / 1000}s elapsed)`);

    if (status === 'SUCCEEDED') {
      console.log('\nQuery completed successfully!');
      return result.QueryExecution;
    } else if (status === 'FAILED' || status === 'CANCELLED') {
      const reason = result.QueryExecution.Status.StateChangeReason;
      throw new Error(`Query ${status}: ${reason}`);
    }

    await new Promise(resolve => setTimeout(resolve, CONFIG.pollInterval));
    retries++;
  }

  throw new Error('Query execution timeout');
}

// Download query results
async function downloadResults(queryExecutionId, outputFile) {
  console.log('Downloading query results...');

  const args = [
    'athena',
    'get-query-results',
    '--query-execution-id', queryExecutionId,
    '--output', 'text'
  ];

  let allResults = '';
  let nextToken = null;
  let isFirstPage = true;

  do {
    const currentArgs = [...args];
    if (nextToken) {
      currentArgs.push('--next-token', nextToken);
    }

    const output = await executeAwsCommand(currentArgs);

    // For text output, we get tab-separated values
    if (isFirstPage) {
      allResults += output;
      isFirstPage = false;
    } else {
      // Skip header on subsequent pages
      const lines = output.split('\n');
      allResults += lines.slice(1).join('\n');
    }

    // Check if there's a next token (this is simplified, actual implementation may vary)
    nextToken = null; // AWS CLI text output doesn't paginate the same way
    break;

  } while (nextToken);

  // Convert tab-separated to CSV
  const csvContent = allResults.split('\n').map(line => {
    return line.split('\t').map(field => {
      // Escape quotes and wrap in quotes if contains comma or quote
      if (field.includes(',') || field.includes('"') || field.includes('\n')) {
        return `"${field.replace(/"/g, '""')}"`;
      }
      return field;
    }).join(',');
  }).join('\n');

  await fs.writeFile(outputFile, csvContent, 'utf-8');
  console.log(`Results saved to: ${outputFile}`);
}

// Parse diff_columns string into a map of column -> value difference
function parseDiffColumns(diffColumnsStr) {
  const result = {};

  if (!diffColumnsStr || diffColumnsStr.trim() === '' || diffColumnsStr === 'NULL') {
    return result;
  }

  // Split by '; ' to get individual column differences
  const differences = diffColumnsStr.split('; ').filter(d => d.trim() !== '');

  for (const diff of differences) {
    // Parse format: "columnName:valueA X valueB"
    const colonIndex = diff.indexOf(':');
    if (colonIndex === -1) continue;

    const columnName = diff.substring(0, colonIndex).trim();
    const values = diff.substring(colonIndex + 1).trim();

    result[columnName] = values;
  }

  return result;
}

// Post-process CSV to expand diff_columns into separate columns
async function expandDiffColumnsInCsv(csvFilePath, compareColumns) {
  console.log('Expanding diff_columns into separate columns...');

  const content = await fs.readFile(csvFilePath, 'utf-8');
  const lines = content.split('\n').filter(line => line.trim() !== '');

  if (lines.length === 0) {
    console.log('No results to process.');
    return;
  }

  // Parse CSV header
  const headerLine = lines[0];
  const headers = parseCSVLine(headerLine);

  // Find indices
  const diffColumnsIndex = headers.indexOf('diff_columns');
  if (diffColumnsIndex === -1) {
    console.log('Warning: diff_columns column not found, skipping expansion.');
    return;
  }

  // Create new headers: remove diff_columns, add individual compare columns
  const newHeaders = [
    ...headers.slice(0, diffColumnsIndex),
    ...compareColumns,
    ...headers.slice(diffColumnsIndex + 1)
  ];

  // Process each data row
  const newLines = [newHeaders.join(',')];

  for (let i = 1; i < lines.length; i++) {
    const fields = parseCSVLine(lines[i]);
    if (fields.length !== headers.length) continue;

    const diffColumnsValue = fields[diffColumnsIndex];
    const diffMap = parseDiffColumns(diffColumnsValue);

    // Build new row
    const newFields = [
      ...fields.slice(0, diffColumnsIndex),
      ...compareColumns.map(col => {
        const diffValue = diffMap[col];
        if (diffValue) {
          // Escape CSV if needed
          return escapeCSVField(diffValue);
        }
        return '';
      }),
      ...fields.slice(diffColumnsIndex + 1)
    ];

    newLines.push(newFields.join(','));
  }

  // Write back to file
  await fs.writeFile(csvFilePath, newLines.join('\n') + '\n', 'utf-8');
  console.log('Diff columns expanded successfully.');
}

// Simple CSV line parser (handles quoted fields)
function parseCSVLine(line) {
  const fields = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const char = line[i];

    if (char === '"') {
      if (inQuotes && line[i + 1] === '"') {
        // Escaped quote
        current += '"';
        i++;
      } else {
        // Toggle quote mode
        inQuotes = !inQuotes;
      }
    } else if (char === ',' && !inQuotes) {
      fields.push(current);
      current = '';
    } else {
      current += char;
    }
  }

  fields.push(current);
  return fields;
}

// Escape CSV field if needed
function escapeCSVField(field) {
  if (field.includes(',') || field.includes('"') || field.includes('\n')) {
    return `"${field.replace(/"/g, '""')}"`;
  }
  return field;
}

// Alternative: Download results from S3 directly
async function downloadResultsFromS3(queryExecution, outputFile) {
  const s3OutputLocation = queryExecution.ResultConfiguration.OutputLocation;
  console.log(`Downloading results from S3: ${s3OutputLocation}`);

  const args = [
    's3',
    'cp',
    s3OutputLocation,
    outputFile
  ];

  await executeAwsCommand(args);
  console.log(`Results saved to: ${outputFile}`);
}

// Check if query length exceeds Athena limit
function isQueryTooLong(query) {
  const queryLength = Buffer.byteLength(query, 'utf-8');
  console.log(`Query length: ${queryLength.toLocaleString()} bytes`);
  return queryLength > CONFIG.maxQueryLength;
}

// Execute query with a subset of comparison columns and return raw CSV content
async function executeQueryWithSubset(config, joinColumns, compareColumnsSubset, subsetIndex, totalSubsets) {
  console.log(`\n=== Executing query for column subset ${subsetIndex}/${totalSubsets} ===`);
  console.log(`Columns in this subset: ${compareColumnsSubset.join(', ')}`);

  const query = buildComparisonQuery(
    config.tableA,
    config.tableB,
    joinColumns,
    compareColumnsSubset,
    config.filter,
    config.skipAdjustment
  );

  if (isQueryTooLong(query)) {
    throw new Error('QUERY_TOO_LONG');
  }

  console.log('Generated SQL Query:');
  console.log('-------------------');
  console.log(query);
  console.log('-------------------');
  console.log('');

  // Start query execution
  const queryExecutionId = await startQueryExecution(query, config.workgroup);
  console.log(`Query Execution ID: ${queryExecutionId}`);

  // Wait for completion
  const queryExecution = await waitForQueryCompletion(queryExecutionId);
  console.log('');

  // Download to temporary file
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  const tempFile = `temp_subset_${subsetIndex}_${timestamp}.csv`;

  try {
    await downloadResultsFromS3(queryExecution, tempFile);
  } catch (error) {
    console.log('S3 download failed, trying direct API download...');
    await downloadResults(queryExecutionId, tempFile);
  }

  // Expand diff_columns
  await expandDiffColumnsInCsv(tempFile, compareColumnsSubset);

  // Read and return content
  const content = await fs.readFile(tempFile, 'utf-8');

  // Clean up temp file
  await fs.unlink(tempFile).catch(() => {});

  return { content, compareColumns: compareColumnsSubset };
}

// Bisection strategy: recursively split comparison columns if query is too long
async function executeWithBisection(config, joinColumns, compareColumns, depth = 0) {
  const indent = '  '.repeat(depth);
  console.log(`${indent}=== Bisection level ${depth}: ${compareColumns.length} comparison columns ===`);

  // Build query to check length
  const query = buildComparisonQuery(
    config.tableA,
    config.tableB,
    joinColumns,
    compareColumns,
    config.filter,
    config.skipAdjustment
  );

  // If query is within limits, execute directly
  if (!isQueryTooLong(query)) {
    console.log(`${indent}Query length is acceptable, executing query...`);
    // Use depth as a simple counter for subset numbering
    const subsetId = Date.now() + Math.random();
    return [await executeQueryWithSubset(config, joinColumns, compareColumns, subsetId, 1)];
  }

  // Query is too long, split columns in half
  if (compareColumns.length === 1) {
    throw new Error(`Query is too long even with a single comparison column: ${compareColumns[0]}`);
  }

  console.log(`${indent}Query too long (${Buffer.byteLength(query, 'utf-8')} bytes), splitting columns...`);

  const mid = Math.floor(compareColumns.length / 2);
  const leftColumns = compareColumns.slice(0, mid);
  const rightColumns = compareColumns.slice(mid);

  console.log(`${indent}Splitting into: [${leftColumns.length}] + [${rightColumns.length}] columns`);

  // Recursively execute both halves
  const leftResults = await executeWithBisection(config, joinColumns, leftColumns, depth + 1);
  const rightResults = await executeWithBisection(config, joinColumns, rightColumns, depth + 1);

  return [...leftResults, ...rightResults];
}

// Merge multiple CSV results using FULL OUTER JOIN logic
async function mergeResults(results, joinColumns, allCompareColumns, outputFile) {
  console.log('\n=== Merging results from multiple queries ===');
  console.log(`Total result sets to merge: ${results.length}`);

  if (results.length === 0) {
    throw new Error('No results to merge');
  }

  if (results.length === 1) {
    // Only one result, write directly
    await fs.writeFile(outputFile, results[0].content, 'utf-8');
    console.log(`Single result saved to: ${outputFile}`);
    return;
  }

  // Parse all result sets
  const parsedResults = results.map((result, index) => {
    const lines = result.content.split('\n').filter(line => line.trim() !== '');
    const headers = parseCSVLine(lines[0]);
    const rows = [];

    for (let i = 1; i < lines.length; i++) {
      const fields = parseCSVLine(lines[i]);
      if (fields.length === headers.length) {
        const row = {};
        headers.forEach((header, idx) => {
          row[header] = fields[idx];
        });
        rows.push(row);
      }
    }

    return { headers, rows, compareColumns: result.compareColumns };
  });

  // Build merged row map: key = join columns + remarks
  const mergedMap = new Map();

  for (const result of parsedResults) {
    for (const row of result.rows) {
      // Build composite key from join columns + remarks
      const keyParts = joinColumns.map(col => row[col] || '');
      keyParts.push(row.remarks || '');
      const key = keyParts.join('|||');

      if (!mergedMap.has(key)) {
        // Initialize with join columns and remarks
        const newRow = {};
        joinColumns.forEach(col => {
          newRow[col] = row[col] || '';
        });
        newRow.remarks = row.remarks || '';

        // Initialize all compare columns as empty
        allCompareColumns.forEach(col => {
          newRow[col] = '';
        });

        mergedMap.set(key, newRow);
      }

      // Merge compare column values
      const mergedRow = mergedMap.get(key);
      result.compareColumns.forEach(col => {
        if (row[col] !== undefined && row[col] !== '') {
          mergedRow[col] = row[col];
        }
      });
    }
  }

  // Build output CSV
  const finalHeaders = [...joinColumns, 'remarks', ...allCompareColumns];
  const outputLines = [finalHeaders.join(',')];

  // Sort by join columns for consistent output
  const sortedEntries = Array.from(mergedMap.entries()).sort((a, b) => {
    return a[0].localeCompare(b[0]);
  });

  for (const [, row] of sortedEntries) {
    const fields = finalHeaders.map(header => {
      const value = row[header] || '';
      return escapeCSVField(value);
    });
    outputLines.push(fields.join(','));
  }

  await fs.writeFile(outputFile, outputLines.join('\n') + '\n', 'utf-8');
  console.log(`Merged results saved to: ${outputFile}`);
  console.log(`Total merged rows: ${sortedEntries.length}`);
}

// Main function
async function main() {
  try {
    // Parse arguments
    const config = parseArgs();

    console.log('Configuration:');
    console.log(`  Table A: ${config.tableA}`);
    console.log(`  Table B: ${config.tableB}`);
    console.log(`  Join Columns File: ${config.joinColumnsFile}`);
    console.log(`  Compare Columns File: ${config.compareColumnsFile}`);
    console.log(`  Workgroup: ${config.workgroup}`);
    if (config.filter) {
      console.log(`  Filter: ${config.filter}`);
    }
    console.log(`  Adjustment Filtering: ${config.skipAdjustment ? 'OFF' : 'ON'}`);
    if (!config.skipAdjustment) {
      const tableAName = config.tableA.split('.')[1];
      console.log(`  Adjustment Table: ecidtas_adjustment_data.${tableAName}`);
    }
    console.log('');

    // Read column files
    console.log('Reading column configuration files...');
    const joinColumns = await readColumnFile(config.joinColumnsFile);
    const compareColumns = await readColumnFile(config.compareColumnsFile);

    console.log(`Join columns: ${joinColumns.join(', ')}`);
    console.log(`Compare columns (${compareColumns.length} total): ${compareColumns.join(', ')}`);
    console.log('');

    // Execute with bisection strategy (automatically splits if query is too long)
    const results = await executeWithBisection(config, joinColumns, compareColumns);

    // Merge results if multiple queries were executed
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const outputFile = `comparison_results_${timestamp}.csv`;

    await mergeResults(results, joinColumns, compareColumns, outputFile);

    console.log('');
    console.log('Comparison completed successfully!');

  } catch (error) {
    console.error('Error:', error.message);
    process.exit(1);
  }
}

// Run main function
main();

    
