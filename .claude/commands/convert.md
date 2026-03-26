---
description: Convert between geospatial formats (GeoParquet, GeoJSON, GeoPackage, Shapefile, CSV, etc.)
argument-hint: <input-file> <output-file>
allowed-tools: Bash(pixi:*), Read, Glob
---
Convert `$0` to `$1`.

## Decision flow

**To GeoParquet** (preferred output):
```bash
pixi run gdal vector convert $0 $1 --co COMPRESSION=ZSTD
```
Then validate (if gpio available): `pixi run gpio check all $1`

**Between spatial formats** (GeoPackage, Shapefile, GeoJSON, FlatGeobuf):
```bash
pixi run gdal vector convert $0 $1
```

**With reprojection** (use `reproject`, not `convert`):
```bash
pixi run gdal vector reproject $0 $1 --dst-crs EPSG:4326
```

**CSV with coordinates → GeoParquet** (via DuckDB):
```bash
pixi run duckdb -c "
  INSTALL spatial; LOAD spatial;
  COPY (
    SELECT *, ST_Point(longitude, latitude) AS geometry
    FROM read_csv_auto('$0')
  ) TO '$1' (FORMAT PARQUET, COMPRESSION ZSTD);
"
```
Ask the user for longitude/latitude column names if not obvious.

After conversion, show output file size and brief schema summary.
