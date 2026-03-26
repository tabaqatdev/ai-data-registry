---
name: spatial-analysis
description: >
  Geospatial analysis with DuckDB spatial (155+ ST_* functions) and unified GDAL CLI.
  Use when the user asks about spatial queries, geometry ops, coordinate transforms,
  distance/area calculations, spatial joins, or any map data processing.
allowed-tools: Read, Write, Edit, Glob, Grep, Bash
---

All tools via pixi: `pixi run duckdb`, `pixi run gdal`.

## DuckDB Spatial (`pixi run duckdb`)
- Load: `INSTALL spatial; LOAD spatial;`
- Read: `SELECT * FROM ST_Read('file.parquet')`
- Ops: ST_Area, ST_Distance, ST_Buffer, ST_Intersection, ST_Contains, ST_Union
- CRS: `ST_Transform(geom, 'EPSG:4326', 'EPSG:3857')`
- Joins: ST_Contains, ST_Within, ST_Intersects
- Aggregations: ST_Union, ST_Collect with GROUP BY
- Indexing: H3/S2 for large-scale point data

## GDAL (`pixi run gdal`) — unified CLI v3.12+
- Info: `gdal info input.gpkg`
- Convert: `gdal vector convert input.shp output.parquet`
- Reproject: `gdal vector reproject input.gpkg output.gpkg -d EPSG:4326`
- Pipeline: `gdal vector pipeline read in.gpkg ! reproject --dst-crs EPSG:4326 ! write out.parquet`
- Terrain: `gdal raster hillshade dem.tif hillshade.tif`

## GeoParquet — preferred interchange format
- Columnar, compressed, embedded CRS metadata
- Read: DuckDB `read_parquet`/`ST_Read`, GDAL `gdal info`
- Write from DuckDB: `COPY (...) TO 'out.parquet' (FORMAT PARQUET)`
- Write from GDAL: `gdal vector convert in.gpkg out.parquet`

## Analysis patterns
- Distance: use projected CRS (not EPSG:4326) for metric accuracy
- Large-scale points: H3 or S2 indexing
- Raster+vector: `gdal vector rasterize` / `gdal raster polygonize`
- Zonal stats: `gdal raster zonal-stats <raster> <zones> <out>`

## Cross-references
- **geoparquet** skill — gpio adds Hilbert sorting, bbox covering, validation
- **gdal** skill — complete unified GDAL CLI reference
- **duckdb-query** skill — interactive DuckDB SQL queries
