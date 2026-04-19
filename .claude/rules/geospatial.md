---
paths:
  - "workspaces/**/*.py"
  - "workspaces/**/*.sql"
  - "workspaces/**/*.parquet"
  - "workspaces/**/*.geojson"
  - "workspaces/**/*.gpkg"
  - "workspaces/**/*.tif"
  - "workspaces/**/*.shp"
  - "workspaces/**/*.fgb"
  - "research/**/*.py"
  - "research/**/*.sql"
---
# Geospatial Data Rules

## GeoParquet as Standard Interchange
- **GeoParquet is the standard format** for all data exchange between tools and workspaces
- **Prefer GeoParquet 2.0 for new outputs**: DuckDB `COPY ... (GEOPARQUET_VERSION 'V2')` writes native Parquet `GEOMETRY`/`GEOGRAPHY` types with built-in column statistics, no manual `bbox` column needed
- For GeoParquet 1.x files, optimize with: `pixi run gpio sort hilbert` → `gpio add bbox` → `gpio check all`
- Best practices: Hilbert sorting, zstd level 15, row groups 50k-150k; bbox column only for GeoParquet 1.x (V2 has native bbox via column stats)

## Tool Selection
- **gpio** (`pixi run gpio`): GeoParquet optimization, validation, spatial indexing, partitioning
- **GDAL** (`pixi run gdal`): Unified CLI (v3.12+) for vector/raster I/O — NOT legacy `ogr2ogr`/`gdalinfo`
- **DuckDB** (`pixi run duckdb`): SQL-based spatial analysis with ST_* functions
- Route: format conversion → GDAL | SQL analysis → DuckDB | GeoParquet optimization → gpio

## CRS
- Default to EPSG:4326 (WGS84) unless specified
- Use projected CRS for metric distance/area calculations
- Validate CRS consistency: `pixi run gdal info <file>` or `pixi run gpio inspect <file>`

## Performance
- Large datasets (>100MB): prefer DuckDB spatial over GeoPandas
- GeoParquet >2GB: partition with `pixi run gpio partition --strategy kdtree`
- Ensure geometry column is WKB-encoded when writing spatial Parquet
