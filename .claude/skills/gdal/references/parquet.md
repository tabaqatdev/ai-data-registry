# GeoParquet / Parquet Driver for GDAL

GDAL 3.12+ Parquet driver for reading/writing GeoParquet with Parquet 2.x native geometry types. Requires `libgdal-arrow-parquet` (already in root pixi.toml).

## Read

```bash
# Info on a Parquet/GeoParquet file
pixi run gdal info file.parquet

# Read partitioned directory (multiple .parquet files as one layer)
pixi run gdal info "PARQUET:/path/to/directory"

# Override CRS on read
pixi run gdal info file.parquet --oo CRS=EPSG:4326

# Non-GeoParquet files: specify geometry column names
pixi run gdal info file.parquet --oo GEOM_POSSIBLE_NAMES=wkb_geometry,the_geom
```

## Write (creation options)

Default command for writing GeoParquet:

```bash
pixi run gdal vector convert input.gpkg output.parquet \
  --lco USE_PARQUET_GEO_TYPES=YES \
  --lco WRITE_COVERING_BBOX=YES \
  --lco COMPRESSION=ZSTD \
  --lco SORT_BY_BBOX=YES
```

**Always use these defaults for new GeoParquet files:**
- `USE_PARQUET_GEO_TYPES=YES` - Parquet 2.x native Geometry/Geography logical types (requires libarrow >= 21)
- `WRITE_COVERING_BBOX=YES` - bbox columns for fast spatial filtering
- `COMPRESSION=ZSTD` - best compression ratio
- `SORT_BY_BBOX=YES` - spatial sorting for faster spatial reads (creates temp GeoPackage)

## All layer creation options

| Option | Default | Values | Purpose |
|--------|---------|--------|---------|
| `USE_PARQUET_GEO_TYPES` | NO | YES, NO, ONLY | Parquet 2.x native geometry types. YES = native + GeoParquet metadata. ONLY = native only (no GeoParquet metadata). |
| `WRITE_COVERING_BBOX` | AUTO | AUTO, YES, NO | Write xmin/ymin/xmax/ymax bbox columns for spatial filtering |
| `COVERING_BBOX_NAME` | (auto) | string | Custom name for bbox column (default: geometry column + `_bbox`) |
| `SORT_BY_BBOX` | NO | YES, NO | Hilbert-style spatial sorting before writing. Needs temp disk space. |
| `COMPRESSION` | SNAPPY | NONE, SNAPPY, GZIP, BROTLI, ZSTD, LZ4_RAW | File compression. ZSTD recommended. |
| `COMPRESSION_LEVEL` | (codec default) | integer | Compression level (range depends on codec) |
| `GEOMETRY_ENCODING` | WKB | WKB, WKT, GEOARROW, GEOARROW_INTERLEAVED | WKB is standard GeoParquet. GEOARROW is struct-based (GeoParquet 1.1). |
| `ROW_GROUP_SIZE` | 65536 | integer | Max rows per row group. Smaller = better spatial filtering, larger file. |
| `GEOMETRY_NAME` | geometry | string | Geometry column name |
| `FID` | (none) | string | Feature ID column name. None = no FID column. |
| `POLYGON_ORIENTATION` | COUNTERCLOCKWISE | COUNTERCLOCKWISE, UNMODIFIED | Ring orientation (CCW is GeoParquet spec) |
| `EDGES` | PLANAR | PLANAR, SPHERICAL | Edge interpretation. SPHERICAL = geodesic. With `USE_PARQUET_GEO_TYPES=YES`, SPHERICAL creates Geography type instead of Geometry. |

## Gotchas

- **SORT_BY_BBOX creates a temporary GeoPackage** in the same directory as the output. Needs disk space (can be several times the output file size).
- **SORT_BY_BBOX + advanced Arrow types** (lists, maps): fallback to generic writer, may lose nested types.
- **USE_PARQUET_GEO_TYPES=ONLY** files need GDAL >= 3.12 and libarrow >= 21 to read. Use `YES` for backward compatibility.
- **EDGES=SPHERICAL + USE_PARQUET_GEO_TYPES=YES** creates Parquet Geography type (not Geometry).
- **Single layer only** per Parquet file.
- **Multithreading**: GDAL uses up to 4 threads for reading (configurable via `GDAL_NUM_THREADS`).
- **Update support** (GDAL 3.12+): add/update/remove features and fields, but rewrites entire file on flush.

## Pipeline example

```bash
pixi run gdal vector pipeline \
  read input.gpkg \
  ! reproject --dst-crs EPSG:4326 \
  ! write output.parquet \
    --lco USE_PARQUET_GEO_TYPES=YES \
    --lco WRITE_COVERING_BBOX=YES \
    --lco COMPRESSION=ZSTD \
    --lco SORT_BY_BBOX=YES
```

## Common conversions

```bash
# GeoJSON -> GeoParquet
pixi run gdal vector convert input.geojson output.parquet --lco USE_PARQUET_GEO_TYPES=YES --lco COMPRESSION=ZSTD

# Shapefile -> GeoParquet
pixi run gdal vector convert input.shp output.parquet --lco USE_PARQUET_GEO_TYPES=YES --lco COMPRESSION=ZSTD

# GeoPackage -> GeoParquet
pixi run gdal vector convert input.gpkg output.parquet --lco USE_PARQUET_GEO_TYPES=YES --lco COMPRESSION=ZSTD

# GeoParquet -> GeoJSON
pixi run gdal vector convert input.parquet output.geojson

# GeoParquet -> GeoPackage
pixi run gdal vector convert input.parquet output.gpkg

# Filter then write GeoParquet
pixi run gdal vector filter input.parquet output.parquet --where "pop > 1000000"

# SQL on GeoParquet
pixi run gdal vector sql input.parquet output.parquet --sql "SELECT name, pop FROM layer WHERE pop > 1000"

# Reproject GeoParquet
pixi run gdal vector reproject input.parquet output.parquet -d EPSG:3857

# Select fields
pixi run gdal vector select input.parquet output.parquet --fields "name,pop"
```

## Validate with other tools

```bash
# DuckDB: read and check metadata
pixi run duckdb -c "SELECT key, value FROM parquet_kv_metadata('file.parquet');"
pixi run duckdb -c "LOAD spatial; SELECT * FROM read_parquet('file.parquet') LIMIT 5;"

# gpio: full GeoParquet validation
pixi run gpio check all file.parquet
pixi run gpio inspect summary file.parquet
```

## Cross-references

- **geoparquet** skill - gpio for Hilbert sorting, bbox covering, validation, STAC metadata
- **duckdb** skill - DuckDB `read_parquet` / `COPY TO` for SQL-driven Parquet workflows
- [arrow.md](arrow.md) - Arrow IPC/Feather and ADBC drivers
