---
description: Run a DuckDB SQL query via the pixi-managed DuckDB CLI
argument-hint: [SQL query or natural language description]
---
Execute the following query or translate the description into DuckDB SQL and execute it:

$ARGUMENTS

Use `pixi run duckdb` to run queries. For natural language input, convert to DuckDB Friendly SQL first.
For spatial queries, ensure the spatial extension is loaded: `INSTALL spatial; LOAD spatial;`
For file-based queries, use `read_parquet()`, `read_csv_auto()`, or `ST_Read()`.

Show the results in a readable table format.

If the query fails, use the **duckdb-docs** skill to look up the correct syntax.
