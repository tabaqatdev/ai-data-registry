---
paths:
  - "**/*.py"
  - "**/*.sql"
  - "**/*.parquet"
  - "**/*.geojson"
  - "**/*.gpkg"
  - "**/*.tif"
  - "**/*.shp"
  - "**/*.fgb"
---
# Geospatial Data Rules

## GeoParquet as Standard Interchange
- **GeoParquet is the standard format** for all data exchange between tools and workspaces
- Use `pixi run gpio convert geoparquet` for optimized conversion (Hilbert sort, zstd, bbox covering)
- Always validate with `pixi run gpio check all <file>` before publishing or sharing
- For GeoParquet optimization: Hilbert spatial sorting, zstd compression level 15, bbox covering metadata, row groups 50k-150k

## Tool Selection
- **gpio** (`pixi run gpio`): Preferred for GeoParquet creation, validation, optimization, partitioning, STAC
- **gdal** (`pixi run gdal`): Unified CLI (v3.11+) for general vector/raster I/O — NOT legacy `ogr2ogr`/`gdalinfo`/`ogrinfo`
- **DuckDB** (`pixi run duckdb`): SQL-based spatial analysis, joins, aggregations with ST_* functions
- Route: format conversion → gdal or gpio | SQL analysis → DuckDB | GeoParquet optimization → gpio

## CRS (Coordinate Reference System)
- Always check and declare CRS — default to EPSG:4326 (WGS84) unless specified
- Use projected CRS (not EPSG:4326) for metric distance/area calculations
- Validate CRS consistency across datasets before joins: `pixi run gdal info <file>` or `pixi run gpio inspect meta <file>`
- GDAL version must match libgdal-arrow-parquet version in root pixi.toml

## Performance
- For large datasets (>100MB), prefer DuckDB spatial over GeoPandas
- For GeoParquet >2GB, partition with `pixi run gpio partition kdtree`
- When writing Parquet with spatial data, ensure geometry column is WKB-encoded

## Cross-references
- **geoparquet** skill → gpio CLI for GeoParquet conversion, validation, optimization
- **gdal** skill → full unified CLI reference (all vector/raster/pipeline/vsi commands)
- **spatial-analysis** skill → DuckDB spatial queries combined with GDAL
- **duckdb-read-file** skill → quickly explore any spatial file via DuckDB
- **data-pipeline** skill → multi-step ETL with GDAL + DuckDB + gpio as pixi tasks
- **data-quality** agent → validate geometry, CRS consistency, spec compliance
