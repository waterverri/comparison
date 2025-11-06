      
#!/usr/bin/env node

const { spawn } = require('child_process');
const fs = require('fs').promises;
const path = require('path');

// Configuration
const CONFIG = {
  pollInterval: 2000, // 2 seconds
  maxRetries: 300 // 10 minutes max wait time
};

// Parse command line arguments
function parseArgs() {
  const args = process.argv.slice(2);

  if (args.length < 4) {
    console.error('Usage: node compare-athena-tables.js <tableA> <tableB> <join_columns_file> <compare_columns_file> [workgroup] [filter]');
    console.error('Example: node compare-athena-tables.js database_x.table_a database_y.table_b join_cols.txt compare_cols.txt primary "date >= DATE \'2024-01-01\'"');
    console.error('Note: Tables should be in format "database.table" (e.g., X.A means database X, table A)');
    process.exit(1);
  }

  return {
    tableA: args[0],
    tableB: args[1],
    joinColumnsFile: args[2],
    compareColumnsFile: args[3],
    workgroup: args[4] || 'primary',
    filter: args[5] || null
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
function buildComparisonQuery(tableA, tableB, joinColumns, compareColumns, filter = null) {
  const joinColumnsStr = joinColumns.join(', ');
  const joinCondition = joinColumns.map(col => `tbl_a.${col} = tbl_b.${col}`).join(' AND ');
  const joinConditionLeft = joinColumns.map(col => `tbl_a.${col} = duplicates_a.${col}`).join(' AND ');
  const joinConditionRight = joinColumns.map(col => `tbl_b.${col} = duplicates_b.${col}`).join(' AND ');

  // Build filter clause for CTEs
  const filterClauseWhere = filter ? `WHERE ${filter}` : '';

  // For Union 4: Create a condensed diff_columns output instead of individual CASE statements
  // This dramatically reduces query length
  // Use COALESCE to display 'NULL' string instead of NULL values in output
  const diffColumnsArray = compareColumns.map(col => {
    return `IF(tbl_a.${col} = tbl_b.${col} OR (tbl_a.${col} IS NULL AND tbl_b.${col} IS NULL), NULL,
      CONCAT('${col}:', COALESCE(CAST(tbl_a.${col} AS VARCHAR), 'NULL'), ' X ', COALESCE(CAST(tbl_b.${col} AS VARCHAR), 'NULL')))`;
  }).join(',\n      ');

  const compareColumnsNull = compareColumns.map(col => `NULL AS ${col}`).join(', ');

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
)

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
)

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
)

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
)

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
)

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
)
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

  const args = [
    'athena',
    'start-query-execution',
    '--query-string', query,
    '--work-group', workgroup
  ];

  const result = await executeAwsCommand(args);
  return result.QueryExecutionId;
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
    console.log('');

    // Read column files
    console.log('Reading column configuration files...');
    const joinColumns = await readColumnFile(config.joinColumnsFile);
    const compareColumns = await readColumnFile(config.compareColumnsFile);

    console.log(`Join columns: ${joinColumns.join(', ')}`);
    console.log(`Compare columns: ${compareColumns.join(', ')}`);
    console.log('');

    // Build query
    const query = buildComparisonQuery(
      config.tableA,
      config.tableB,
      joinColumns,
      compareColumns,
      config.filter
    );

    console.log('Generated SQL Query:');
    console.log('-------------------');
    console.log(query);
    console.log('-------------------');
    console.log('');

    // Start query execution
    const queryExecutionId = await startQueryExecution(
      query,
      config.workgroup
    );
    console.log(`Query Execution ID: ${queryExecutionId}`);
    console.log('');

    // Wait for completion
    const queryExecution = await waitForQueryCompletion(queryExecutionId);
    console.log('');

    // Download results
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const outputFile = `comparison_results_${timestamp}.csv`;

    // Try to download from S3 (more reliable for large results)
    try {
      await downloadResultsFromS3(queryExecution, outputFile);
    } catch (error) {
      console.log('S3 download failed, trying direct API download...');
      await downloadResults(queryExecutionId, outputFile);
    }

    console.log('');
    console.log('Comparison completed successfully!');

  } catch (error) {
    console.error('Error:', error.message);
    process.exit(1);
  }
}

// Run main function
main();

    
