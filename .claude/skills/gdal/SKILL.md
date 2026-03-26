---
name: gdal
description: >
  Geospatial data processing with the unified GDAL CLI (3.12+). Use when converting,
  inspecting, reprojecting, or processing raster/vector data — even if the user just says
  "convert this shapefile" or "what CRS is this file". Replaces legacy ogr2ogr/gdalinfo/ogrinfo.
allowed-tools: Bash, Read, Glob, Grep
---

Run via `pixi run gdal`. Always use the unified CLI, NOT legacy tools.

## Shortcuts
- `gdal <FILE>` → `gdal info <FILE>`
- `gdal read <FILE> ! ...` → `gdal pipeline <FILE> ! ...`

## General
- `gdal info <dataset>` — metadata for any dataset (raster or vector)
- `gdal convert <in> <out>` — auto-detect raster/vector conversion
- `gdal pipeline <steps>` — chain steps with `!` separator

## Vector (`gdal vector ...`)

### Info
- `vector info <dataset>` — schema, layers, features, CRS, extent
- `vector check-geometry <dataset>` — find invalid geometries
- `vector check-coverage <dataset>` — validate polygon coverage

### Convert & Reproject
- `vector convert <in> <out>` — format conversion (no CRS change)
- `vector reproject <in> <out> -d EPSG:xxxx` — reproject (`-d`/`--dst-crs`, required)
  - Optional: `-s`/`--src-crs` to override source CRS

### Spatial ops
- `vector clip <in> <out> --bbox xmin ymin xmax ymax` or `--geometry clip.gpkg`
- `vector buffer <in> <out> --distance <m>`
- `vector simplify <in> <out> --tolerance <val>`
- `vector filter <in> <out> --where "<SQL>"`
- `vector sql <in> <out> --statement "<SQL>"`

### Geometry
- `vector make-valid <in> <out>`
- `vector make-point <in> <out> --x-col lon --y-col lat`
- `vector explode-collections <in> <out>`
- `vector set-geom-type <in> <out> --type POLYGON`
- `vector segmentize <in> <out> --max-length <val>`
- `vector swap-xy <in> <out>`

### Multi-dataset
- `vector concat <in1> <in2> ... <out>`
- `vector layer-algebra <in1> <in2> <out> --operation union|intersection|difference|symdifference`
- `vector simplify-coverage <in> <out>`
- `vector clean-coverage <in> <out>`

### Data management
- `vector select <in> <out> --fields "f1,f2"`
- `vector set-field-type <in> <out> --field name --type Integer`
- `vector edit <in> --metadata KEY=VALUE`
- `vector partition <in> <out_dir> --field <col>`
- `vector index <inputs...> <out>`

### Rasterization
- `vector rasterize <in> <out> --resolution <res>`
- `vector grid <in> <out> --algorithm invdist`

### Pipeline
```bash
pixi run gdal vector pipeline \
  read input.gpkg \
  ! reproject --dst-crs EPSG:4326 \
  ! filter --where "pop > 10000" \
  ! write output.parquet
```

## Raster (`gdal raster ...`)

### Info
- `raster info <dataset>` — bands, resolution, CRS, stats
- `raster pixel-info <dataset> --coord <x> <y>`
- `raster compare <r1> <r2>`

### Convert & Reproject
- `raster convert <in> <out>` — format conversion
- `raster reproject <in> <out> --dst-crs EPSG:xxxx`

### Spatial ops
- `raster clip <in> <out> --bbox xmin ymin xmax ymax` or `--geometry mask.gpkg`
- `raster resize <in> <out> --size <w> <h>`
- `raster mosaic <inputs...> <out>`

### Terrain
- `raster hillshade|slope|aspect|roughness|tpi|tri <in> <out>`
- `raster contour <in> <out> --interval <val>` — vector output
- `raster viewshed <in> <out> --coord <x> <y>`

### Bands
- `raster select <in> <out> --bands 1,3`
- `raster stack <in1> <in2> <out>`
- `raster calc <inputs...> <out> --expression "<formula>"`
- `raster scale <in> <out> --src-min 0 --src-max 255`
- `raster set-type <in> <out> --type Float32`
- `raster unscale <in> <out>`

### Processing
- `raster fill-nodata|nodata-to-alpha|sieve|clean-collar|pansharpen|blend|proximity <in> <out>`
- `raster neighbors <in> <out> --mode average --size 3`
- `raster reclassify <in> <out> --mapping "0-10:1,10-20:2"`
- `raster zonal-stats <raster> <zones> <out>`

### Vectorization
- `raster polygonize|as-features|footprint <in> <out>`

### Management
- `raster overview add|delete|refresh <in>`
- `raster create <out> --size <w> <h> --bands <n>`
- `raster edit <in> --metadata KEY=VALUE`
- `raster index|tile <inputs...> <out>`

### Pipeline
```bash
pixi run gdal raster pipeline \
  read input.tif \
  ! reproject --dst-crs EPSG:3857 \
  ! write output.cog.tif --creation-option COMPRESS=DEFLATE
```

## Other commands
- `gdal mdim info|convert|mosaic` — NetCDF/HDF5/Zarr
- `gdal dataset identify|copy|rename|delete` — dataset management
- `gdal vsi list|copy|delete|move|sync|sozip` — virtual filesystem (S3, Azure, GCS, HTTP, ZIP)
- `gdal driver gpkg repack|gti create|openfilegdb repack` — driver-specific

## Common options (all subcommands)
- `--config KEY=VALUE`, `-f FORMAT`, `--co KEY=VALUE`, `--lco KEY=VALUE`
- `--overwrite`, `--append`, `--quiet`

## Cross-references
- **geoparquet** skill — gpio adds Hilbert sorting, bbox covering, validation that GDAL doesn't
- **spatial-analysis** skill — combined DuckDB spatial + GDAL workflows
