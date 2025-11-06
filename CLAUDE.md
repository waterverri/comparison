# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Node.js CLI tool that compares two AWS Athena tables to identify differences including missing records, duplicates, and value mismatches. The tool executes complex SQL queries via AWS Athena and downloads results as CSV files.

## Running the Tool

```bash
# Basic usage
node compare.js database_x.table_a database_y.table_b join_columns.txt compare_columns.txt

# With workgroup
node compare.js database_x.table_a database_y.table_b join_columns.txt compare_columns.txt my_workgroup

# With filter
node compare.js database_x.table_a database_y.table_b join_columns.txt compare_columns.txt primary "date >= DATE '2024-01-01'"

# Disable adjustment filtering (by default, adjustment filtering is ON)
node compare.js database_x.table_a database_y.table_b join_columns.txt compare_columns.txt primary "date >= DATE '2024-01-01'" --no-adjustment
```

Arguments:
- `tableA`, `tableB`: Format is `database.table` (can be from different databases)
- `join_columns_file`: Text file with one column name per line (used to join tables)
- `compare_columns_file`: Text file with one column name per line (columns to compare values)
- `workgroup` (optional): Athena workgroup name (default: "primary")
- `filter` (optional): SQL WHERE clause applied to all unions
- `--no-adjustment` (optional): Disable adjustment table filtering (default: filtering is ON)

## Architecture

### Core SQL Query Logic

The tool generates a complex SQL query with 4 UNION ALL components:

1. **Union 1**: Records in Table A but not in Table B (excluding duplicates) - marked as "missing in table B"
2. **Union 2**: Records in Table B but not in Table A (excluding duplicates) - marked as "missing in table A"
3. **Union 3a/3b**: All duplicate keys from both tables - marked as "duplicate key in table A/B"
4. **Union 4**: Matched records with differences - uses ARRAY_JOIN to combine all column differences into a single semicolon-separated field

### Adjustment Table Filtering

**NEW FEATURE**: By default, the tool automatically filters out rows that have adjustments in the adjustment table.

**How it works:**
- The adjustment table is always at `ecidtas_adjustment_data.{tableA_name}` (uses the same table name as Table A)
- The adjustment table contains the same join columns as the comparison
- Any row that exists in the adjustment table (based on join columns) is automatically excluded from the comparison results
- This is because adjustments are expected to cause differences between prod and check tables

**Control:**
- **Default behavior**: Adjustment filtering is ON
- **To disable**: Pass `--no-adjustment` as the last argument
- When enabled, rows matching adjustment table keys are excluded from all comparison results (missing records, duplicates, and value differences)

**Why this is useful:**
- In prod vs check comparisons, adjustments are expected to cause differences
- Filtering these out lets you focus on unexpected differences that need investigation
- The adjustment table acts as a whitelist for expected differences

### Filter Application Strategy

**CRITICAL**: Filters are applied BEFORE any join or comparison operations using CTEs (Common Table Expressions):

```sql
WITH filtered_a AS (
  SELECT * FROM tableA WHERE ${filter}
),
filtered_b AS (
  SELECT * FROM tableB WHERE ${filter}
),
duplicates_a AS (
  SELECT join_columns FROM filtered_a GROUP BY join_columns HAVING COUNT(*) > 1
),
duplicates_b AS (
  SELECT join_columns FROM filtered_b GROUP BY join_columns HAVING COUNT(*) > 1
),
filtered_adjustment AS (
  SELECT join_columns FROM ecidtas_adjustment_data.{tableA_name} WHERE ${filter}
)
-- All subsequent unions use filtered_a, filtered_b, duplicates_a, duplicates_b, filtered_adjustment
```

This approach ensures:
- Both tables are filtered once at the beginning (performance optimization)
- The adjustment table is also filtered with the same WHERE clause
- All subsequent operations (joins, duplicate detection, comparisons) work on pre-filtered data
- Duplicate detection is performed on filtered datasets
- Rows in adjustment table are excluded from results (unless --no-adjustment is passed)
- No need to repeat filter logic across multiple unions

### Key Implementation Details

- **Adjustment Filtering**: Automatically excludes rows that exist in the adjustment table (default behavior, disable with `--no-adjustment`)
- **Duplicate Detection**: Uses subqueries with `COUNT(*) > 1` grouped by join columns
- **Value Comparison**: Uses ARRAY_JOIN with IF expressions to create a condensed diff_columns output (format: "col1:valueA X valueB; col2:valueA X valueB")
- **Query Optimization**: Query uses single diff_columns field to dramatically reduce query length (avoids Athena input length limits for tables with many comparison columns)
- **Output Post-Processing**: After download, the diff_columns field is automatically expanded into separate columns (one per compare column) for easier reading
- **NULL Handling**: Treats NULL values as equal when comparing (`a.col = b.col OR (a.col IS NULL AND b.col IS NULL)`)
- **Tuple Comparison Fix**: Uses `EXISTS` instead of `WHERE (columns) IN (SELECT columns)` to avoid TYPE_MISMATCH errors in Athena
- **AWS Integration**: Uses `child_process.spawn()` to execute AWS CLI commands, parsing JSON responses

### Execution Flow

1. Parse arguments and read column configuration files
2. Build SQL query using `buildComparisonQuery()`
3. Start Athena query via `aws athena start-query-execution`
4. Poll status every 2 seconds (max 10 minutes) via `aws athena get-query-execution`
5. Download results from S3 (preferred) or via API
6. Convert results to CSV and save with timestamp
7. Post-process CSV: expand `diff_columns` into separate columns for each compare column

### Output Format

CSV structure:
- First columns: Join columns (e.g., `id`, `customer_id`)
- Next column: `remarks` (describes the type of difference)
- Remaining columns: One column per compare column, showing the difference (e.g., `price`, `status`, `quantity`)

Remarks values:
- `missing in schema_y.table_b`
- `missing in schema_x.table_a`
- `duplicate key in schema_x.table_a`
- `duplicate key in schema_y.table_b`
- `matched`

Compare column format:
- Empty cell if no difference or not applicable (missing/duplicate records)
- For matched records with differences: "valueA X valueB" format
- Example row: price column shows "10.50 X 12.00", status column shows "active X inactive"

**Note**: The Athena query internally uses a single `diff_columns` field (to keep query length short), but the final CSV output automatically expands this into separate columns for easier reading and analysis.

## Prerequisites

- Node.js >= 14.0.0
- AWS CLI installed and configured with credentials
- Athena workgroup configured with S3 output location
- AWS permissions: `athena:*`, `glue:GetDatabase`, `glue:GetTable`, `s3:GetObject`, `s3:PutObject`, `s3:ListBucket`

## Configuration

Located in `compare.js:CONFIG`:
- `pollInterval: 2000` - Query status check interval (milliseconds)
- `maxRetries: 300` - Maximum polling attempts (10 minutes total)

## Adjustment Table

The adjustment table feature allows you to filter out expected differences:
- **Table location**: Always at `ecidtas_adjustment_data.{tableA_name}`
- **Structure**: Must contain the same join columns as the comparison tables
- **Filtering**: Default ON, use `--no-adjustment` to disable
- **Use case**: When comparing prod vs check tables with known adjustments applied to prod
