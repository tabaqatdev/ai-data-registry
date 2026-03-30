# Arrow IPC Driver for GDAL

Arrow IPC (Feather) format for columnar data exchange (GDAL 3.5+). Two variants:
- **File** (`.arrow`, `.feather`): random access, fixed record batches
- **Stream** (`.arrows`): sequential, arbitrary length. Also supports `/vsistdin/` and `/vsistdout/`.

Requires `libgdal-arrow-parquet` (already in root pixi.toml).

## Read

```bash
pixi run gdal info file.arrow

# Force stream format recognition
pixi run gdal info "ARROW_IPC_STREAM:file.arrows"
```

## Write

```bash
# File format (default)
pixi run gdal vector convert input.gpkg output.arrow

# Stream format
pixi run gdal vector convert input.gpkg output.arrows --lco FORMAT=STREAM

# With ZSTD compression
pixi run gdal vector convert input.gpkg output.arrow --lco COMPRESSION=ZSTD

# With WKB geometry (for max interop)
pixi run gdal vector convert input.gpkg output.arrow --lco GEOMETRY_ENCODING=WKB
```

## Layer creation options

| Option | Default | Values | Purpose |
|--------|---------|--------|---------|
| `FORMAT` | FILE | FILE, STREAM | FILE = random access (.arrow), STREAM = sequential (.arrows) |
| `COMPRESSION` | LZ4 | NONE, ZSTD, LZ4 | Compression method |
| `GEOMETRY_ENCODING` | GEOARROW | GEOARROW, WKB, WKT, GEOARROW_INTERLEAVED | Default is struct-based GeoArrow (not WKB like Parquet) |
| `BATCH_SIZE` | 65536 | integer | Max rows per record batch |
| `GEOMETRY_NAME` | geometry | string | Geometry column name |
| `FID` | (none) | string | Feature ID column name |

## Common conversions

```bash
# Arrow -> Parquet
pixi run gdal vector convert input.arrow output.parquet --lco USE_PARQUET_GEO_TYPES=YES --lco COMPRESSION=ZSTD

# Parquet -> Arrow
pixi run gdal vector convert input.parquet output.arrow

# GeoJSON -> Arrow
pixi run gdal vector convert input.geojson output.arrow

# Arrow -> GeoPackage
pixi run gdal vector convert input.arrow output.gpkg
```

## Gotchas

- **Default geometry encoding is GEOARROW** (struct-based), not WKB. Use `--lco GEOMETRY_ENCODING=WKB` for max interoperability.
- **Stream format** (`.arrows`) cannot be randomly accessed. Use FILE for most cases.
- **Single layer only** per Arrow file.

## Cross-references

- [parquet.md](parquet.md) - GeoParquet/Parquet driver (Parquet 2.x native geometry, bbox, compression)
- [adbc.md](adbc.md) - ADBC database connectivity (DuckDB, PostgreSQL, BigQuery)
- **duckdb** skill - DuckDB for direct SQL on Arrow files
