---
paths:
  - "**/*.sql"
  - "**/*.py"
---
# DuckDB SQL Rules

- Run DuckDB via `pixi run duckdb` to use the project-managed version
- Use DuckDB SQL dialect, not PostgreSQL or MySQL syntax
- Friendly SQL: `FROM table` without SELECT, `GROUP BY ALL`, `ORDER BY ALL`, `EXCLUDE`, `REPLACE`
- For spatial queries: `INSTALL spatial; LOAD spatial;` must be called first
- Use `ST_*` functions for geometry operations (ST_Read, ST_Area, ST_Distance, etc.)
- Prefer `read_parquet()` and `read_csv_auto()` for file-based queries
- For GeoParquet: `SELECT * FROM ST_Read('file.parquet')`
- Use CTEs to break complex queries into readable parts
- Use `QUALIFY` for window function filtering
- Use `arg_max()`/`arg_min()` for "most recent" patterns
- JSON access: `col->>'key'` returns text, `col->'$.path'` returns JSON

## DuckDB → GeoParquet Best Practices
When writing GeoParquet from DuckDB, apply optimizations manually:
```sql
COPY (
    SELECT *, ST_Envelope(geometry) as bbox
    FROM read_parquet('input.parquet')
    ORDER BY ST_Hilbert(geometry)
) TO 'output.parquet' (
    FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 15, ROW_GROUP_SIZE 100000
);
```
Then validate: `pixi run gpio check all output.parquet`

## Session State
- Use the **duckdb-state** skill to initialize and manage `state.sql` (extensions, credentials, macros)
- State file location: `.duckdb-skills/state.sql` (project-local) or `~/.duckdb-skills/<project>/state.sql`
- Core extensions pre-loaded in state: spatial, httpfs, fts

## Cross-references
- **duckdb-query** skill → interactive SQL or natural language queries
- **duckdb-read-file** skill → explore any data file (CSV, Parquet, Excel, spatial, etc.)
- **duckdb-docs** skill → search DuckDB documentation when unsure about syntax
- **duckdb-state** skill → initialize/manage session state, extensions, credentials
- **spatial-analysis** skill → combined DuckDB spatial + GDAL workflows
- **geoparquet** skill → validate and optimize GeoParquet output from DuckDB
