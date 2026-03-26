---
description: Convert between geospatial formats (GeoParquet, GeoJSON, GeoPackage, Shapefile, CSV, etc.)
argument-hint: <input-file> <output-file>
allowed-tools: Bash(pixi:*), Read, Glob
---
Convert `$0` to `$1`.

## Decision flow

Choose the right tool based on the conversion:

**To GeoParquet** (preferred output format):
```bash
pixi run gdal convert --format Parquet --co COMPRESSION=ZSTD $0 $1
```
Then validate:
```bash
pixi run gpio check all $1
```

**From/to other spatial formats** (GeoPackage, Shapefile, GeoJSON, FlatGeobuf):
```bash
pixi run gdal convert $0 $1
```

**CSV with coordinates to GeoParquet** (via DuckDB):
```bash
pixi run duckdb -c "
  INSTALL spatial; LOAD spatial;
  COPY (
    SELECT *, ST_Point(longitude, latitude) AS geometry
    FROM read_csv_auto('$0')
  ) TO '$1' (FORMAT PARQUET, COMPRESSION ZSTD);
"
```
Ask the user for the longitude/latitude column names if not obvious.

**Reproject during conversion:**
```bash
pixi run gdal convert --dst-crs EPSG:4326 $0 $1
```

After conversion, show the output file size and a brief schema summary.
