---
description: Inspect any data file (Parquet, CSV, JSON, GeoJSON, GeoPackage, Shapefile, etc.) — shows schema, row count, sample rows, and spatial info if applicable
argument-hint: <file-path>
allowed-tools: Bash(pixi:*), Read, Glob
---
Inspect the data file: `$ARGUMENTS`

## Steps

1. **Detect file type** from the extension of `$0`

2. **Profile the file** using the appropriate tool:

   **Tabular / Parquet / CSV / JSON:**
   ```bash
   pixi run duckdb -csv -c "DESCRIBE SELECT * FROM read_parquet('$0')"
   pixi run duckdb -csv -c "SELECT count(*) AS row_count FROM read_parquet('$0')"
   pixi run duckdb -csv -c "SELECT * FROM read_parquet('$0') LIMIT 5"
   ```
   Use `read_csv_auto()` for CSV, `read_json_auto()` for JSON/NDJSON.

   **Spatial files (GeoPackage, Shapefile, GeoJSON, FlatGeobuf):**
   ```bash
   pixi run gdal info $0
   ```

   **GeoParquet (if gpio installed):**
   ```bash
   pixi run gpio inspect $0
   pixi run gpio check all $0
   ```

3. **Summarize**: file type, row count, columns (name + type), spatial info (CRS, geometry type, bbox) if applicable, and any data quality notes.
