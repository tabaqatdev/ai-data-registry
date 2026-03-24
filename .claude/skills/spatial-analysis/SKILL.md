---
name: spatial-analysis
description: Perform geospatial analysis using DuckDB spatial extension and the new unified GDAL CLI. Use when the user asks about spatial queries, geometry operations, coordinate transformations, or map data processing.
allowed-tools: Read, Write, Edit, Glob, Grep, Bash
---
Perform geospatial analysis for this project.

All tools run via pixi: `pixi run duckdb`, `pixi run gdal`.

Key capabilities:

1. **DuckDB Spatial** (`pixi run duckdb`): 155+ ST_* functions for geometry operations
   - Load extension: `INSTALL spatial; LOAD spatial;`
   - Read spatial files: `SELECT * FROM ST_Read('file.parquet')`
   - Geometry ops: ST_Area, ST_Distance, ST_Buffer, ST_Intersection, ST_Contains
   - CRS transforms: ST_Transform(geom, 'EPSG:4326', 'EPSG:3857')
   - Spatial joins: ST_Contains, ST_Within, ST_Intersects
   - Aggregations: ST_Union, ST_Collect with GROUP BY
   - H3/S2 indexing for large-scale point data

2. **GDAL** (`pixi run gdal`): New unified CLI (v3.11+) — NOT legacy ogr2ogr/gdalinfo/ogrinfo
   - Inspect: `pixi run gdal info input.gpkg`
   - Convert vector: `pixi run gdal vector convert input.shp output.parquet`
   - Convert raster: `pixi run gdal raster convert input.tif output.cog.tif`
   - Reproject: `pixi run gdal vector reproject input.gpkg output.gpkg --dst-crs EPSG:4326`
   - Pipeline: `pixi run gdal vector pipeline read input.gpkg ! reproject --dst-crs EPSG:4326 ! write output.parquet`
   - Terrain: `pixi run gdal raster hillshade dem.tif hillshade.tif`

3. **GeoParquet**: Preferred interchange format between DuckDB and GDAL
   - Columnar, compressed, with embedded CRS metadata
   - Readable by DuckDB (`read_parquet`/`ST_Read`), GDAL (`gdal info`), and most modern GIS tools
   - Write from DuckDB: `COPY (...) TO 'output.parquet' (FORMAT PARQUET)`
   - Write from GDAL: `pixi run gdal vector convert input.gpkg output.parquet`

4. **Analysis patterns**:
   - Distance calculations: use projected CRS (not EPSG:4326) for metric accuracy
   - Large-scale point analysis: use H3 or S2 indexing
   - Raster + vector: rasterize vectors with `pixi run gdal vector rasterize`, or vectorize rasters with `pixi run gdal raster polygonize`
   - Zonal stats: `pixi run gdal raster zonal-stats <raster> <zones> <output>`

## Cross-references
- Use the **geoparquet** skill when writing GeoParquet — gpio adds Hilbert sorting, bbox covering, validation
- Use the **gdal** skill for the complete unified GDAL CLI reference (all subcommands and options)
- Use the **duckdb-query** skill for interactive DuckDB SQL queries
- Use the **duckdb-read-file** skill to quickly explore any spatial file
- Use the **duckdb-docs** skill to look up DuckDB spatial function syntax
- Use the **duckdb-state** skill to initialize DuckDB session with spatial extension pre-loaded
- Use the **data-pipeline** skill to chain spatial operations as pixi tasks
- Use the **data-quality** agent to validate geometry and CRS consistency
- Use the **data-explorer** agent to profile spatial datasets (CRS, geometry types, extent)
- Use the **pipeline-orchestrator** agent to plan multi-tool spatial workflows
