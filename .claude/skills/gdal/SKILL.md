---
name: gdal
description: >
  Geospatial data processing using the new unified GDAL CLI (3.11+).
  Use when converting, inspecting, reprojecting, or processing raster and vector
  geospatial data. Covers the modern `gdal` command that replaces legacy tools
  like ogr2ogr, gdalinfo, and ogrinfo.
allowed-tools: Bash, Read, Glob, Grep
---

You are a geospatial data specialist using the **new unified GDAL CLI** (v3.11+).
Always use the modern `gdal` command, NOT the legacy `ogr2ogr`, `gdalinfo`, `ogrinfo` tools.

Run GDAL commands inside the pixi environment: `pixi run gdal ...`

---

## Quick Reference

### Shortcuts
- `gdal <FILENAME>` → same as `gdal info <FILENAME>`
- `gdal read <FILENAME> ! ...` → same as `gdal pipeline <FILENAME> ! ...`

### Common Options (all subcommands)
- `--config <KEY>=<VALUE>` — set GDAL config option (repeatable)
- `--json-usage` — output usage as JSON
- `--version` — show GDAL version
- `--drivers` — list available drivers (JSON)

---

## General Commands

### `gdal info <dataset>`
Get metadata for any dataset (raster or vector). Replaces `gdalinfo` and `ogrinfo`.
```bash
pixi run gdal info input.gpkg
pixi run gdal info input.tif
```

### `gdal convert <input> <output>`
Convert between formats (auto-detects raster/vector).
```bash
pixi run gdal convert input.shp output.parquet
pixi run gdal convert input.tif output.cog.tif
```

### `gdal pipeline <steps>`
Chain processing steps with `!` separator.
```bash
pixi run gdal pipeline read input.gpkg ! reproject --dst-crs EPSG:4326 ! write output.parquet
```

---

## Vector Commands (`gdal vector ...`)

### Info & Inspection
- `gdal vector info <dataset>` — schema, layer list, feature count, CRS, extent
- `gdal vector check-geometry <dataset>` — find invalid geometries
- `gdal vector check-coverage <dataset>` — validate polygon coverage

### Format Conversion
- `gdal vector convert <input> <output>` — convert between vector formats
  ```bash
  pixi run gdal vector convert input.shp output.parquet
  pixi run gdal vector convert input.geojson output.gpkg
  pixi run gdal vector convert input.gpkg output.fgb  # FlatGeobuf
  ```

### Spatial Operations
- `gdal vector reproject <input> <output> --dst-crs EPSG:xxxx` — coordinate transform
- `gdal vector clip <input> <output> --bbox <xmin> <ymin> <xmax> <ymax>` — clip by extent
- `gdal vector clip <input> <output> --geometry <clip.gpkg>` — clip by geometry
- `gdal vector buffer <input> <output> --distance <meters>` — buffer geometries
- `gdal vector simplify <input> <output> --tolerance <value>` — simplify geometries
- `gdal vector filter <input> <output> --where "<SQL expression>"` — attribute filter
- `gdal vector sql <input> <output> --statement "<SQL>"` — run SQL on layers

### Geometry Manipulation
- `gdal vector make-valid <input> <output>` — repair invalid geometries
- `gdal vector make-point <input> <output> --x-col lon --y-col lat` — create points from coords
- `gdal vector explode-collections <input> <output>` — multi → single geometries
- `gdal vector set-geom-type <input> <output> --type POLYGON` — force geometry type
- `gdal vector segmentize <input> <output> --max-length <value>` — densify vertices
- `gdal vector swap-xy <input> <output>` — swap X/Y coordinates

### Multi-dataset Operations
- `gdal vector concat <input1> <input2> ... <output>` — merge datasets
- `gdal vector layer-algebra <input1> <input2> <output> --operation union|intersection|difference|symdifference`
- `gdal vector simplify-coverage <input> <output>` — simplify shared boundaries
- `gdal vector clean-coverage <input> <output>` — remove gaps/overlaps

### Data Management
- `gdal vector select <input> <output> --fields "field1,field2"` — select columns
- `gdal vector set-field-type <input> <output> --field name --type Integer` — change field type
- `gdal vector edit <input> --metadata KEY=VALUE` — edit metadata
- `gdal vector partition <input> <output_dir> --field <column>` — split by attribute
- `gdal vector index <inputs...> <output>` — build spatial tile index

### Rasterization
- `gdal vector rasterize <input> <output> --resolution <res>` — burn vectors to raster
- `gdal vector grid <input> <output> --algorithm invdist` — interpolate points to grid

### Pipeline
```bash
pixi run gdal vector pipeline \
  read input.gpkg \
  ! reproject --dst-crs EPSG:4326 \
  ! filter --where "population > 10000" \
  ! select --fields "name,population,geometry" \
  ! write output.parquet
```

---

## Raster Commands (`gdal raster ...`)

### Info & Inspection
- `gdal raster info <dataset>` — bands, resolution, CRS, extent, statistics
- `gdal raster pixel-info <dataset> --coord <x> <y>` — query pixel values
- `gdal raster compare <raster1> <raster2>` — diff two rasters

### Format Conversion
- `gdal raster convert <input> <output>` — convert between raster formats
  ```bash
  pixi run gdal raster convert input.tif output.cog.tif  # Cloud Optimized GeoTIFF
  pixi run gdal raster convert input.nc output.tif
  ```

### Spatial Operations
- `gdal raster reproject <input> <output> --dst-crs EPSG:xxxx` — reproject
- `gdal raster clip <input> <output> --bbox <xmin> <ymin> <xmax> <ymax>` — clip by extent
- `gdal raster clip <input> <output> --geometry <mask.gpkg>` — clip by geometry
- `gdal raster resize <input> <output> --size <width> <height>` — resize
- `gdal raster mosaic <inputs...> <output>` — merge multiple rasters

### Terrain Analysis
- `gdal raster hillshade <input> <output>` — shaded relief
- `gdal raster slope <input> <output>` — slope map
- `gdal raster aspect <input> <output>` — aspect map
- `gdal raster roughness <input> <output>` — roughness map
- `gdal raster tpi <input> <output>` — Topographic Position Index
- `gdal raster tri <input> <output>` — Terrain Ruggedness Index
- `gdal raster contour <input> <output> --interval <value>` — contour lines (vector output)
- `gdal raster viewshed <input> <output> --coord <x> <y>` — viewshed analysis

### Band Operations
- `gdal raster select <input> <output> --bands 1,3` — select bands
- `gdal raster stack <input1> <input2> <output>` — combine into multi-band
- `gdal raster calc <inputs...> <output> --expression "<formula>"` — raster algebra
- `gdal raster scale <input> <output> --src-min 0 --src-max 255` — rescale values
- `gdal raster set-type <input> <output> --type Float32` — change data type
- `gdal raster unscale <input> <output>` — apply scale/offset

### Processing
- `gdal raster fill-nodata <input> <output>` — interpolate nodata areas
- `gdal raster nodata-to-alpha <input> <output>` — nodata → alpha band
- `gdal raster sieve <input> <output> --threshold <pixels>` — remove small regions
- `gdal raster clean-collar <input> <output>` — remove edge artifacts
- `gdal raster neighbors <input> <output> --mode average --size 3` — focal statistics
- `gdal raster reclassify <input> <output> --mapping "0-10:1,10-20:2"` — reclassify
- `gdal raster pansharpen <pan> <ms> <output>` — pansharpening
- `gdal raster blend <raster1> <raster2> <output>` — blend two rasters
- `gdal raster proximity <input> <output>` — distance to target pixels
- `gdal raster zonal-stats <raster> <zones> <output>` — stats by zone

### Vectorization
- `gdal raster polygonize <input> <output>` — raster to polygons
- `gdal raster as-features <input> <output>` — pixels to point features
- `gdal raster footprint <input> <output>` — compute data extent polygon

### Management
- `gdal raster overview add <input>` — build pyramids
- `gdal raster overview delete <input>` — remove pyramids
- `gdal raster overview refresh <input>` — update pyramids
- `gdal raster create <output> --size <w> <h> --bands <n>` — create empty raster
- `gdal raster edit <input> --metadata KEY=VALUE` — edit metadata
- `gdal raster index <inputs...> <output>` — build tile index
- `gdal raster tile <input> <output_dir>` — split into tiles
- `gdal raster update <source> <dest>` — overwrite region

### Pipeline
```bash
pixi run gdal raster pipeline \
  read input.tif \
  ! reproject --dst-crs EPSG:3857 \
  ! clip --bbox -180 -85 180 85 \
  ! write output.cog.tif --creation-option COMPRESS=DEFLATE
```

---

## Multidimensional Commands (`gdal mdim ...`)
- `gdal mdim info <dataset>` — inspect NetCDF/HDF5/Zarr structure
- `gdal mdim convert <input> <output>` — convert multidimensional formats
- `gdal mdim mosaic <inputs...> <output>` — combine multidimensional datasets

## Dataset Management (`gdal dataset ...`)
- `gdal dataset identify <file>` — detect the driver
- `gdal dataset copy <src> <dst>` — copy all dataset files
- `gdal dataset rename <old> <new>` — rename dataset
- `gdal dataset delete <dataset>` — remove dataset and sidecars

## Virtual Filesystem (`gdal vsi ...`)
- `gdal vsi list <path>` — list remote/virtual directory (S3, Azure, GCS, HTTP, ZIP)
- `gdal vsi copy <src> <dst>` — copy files across virtual filesystems
- `gdal vsi delete <path>` — delete virtual file
- `gdal vsi move <src> <dst>` — move virtual file
- `gdal vsi sync <src> <dst>` — sync directories
- `gdal vsi sozip <input> <output>` — create seek-optimized ZIP

## Driver-Specific Commands (`gdal driver ...`)
- `gdal driver gpkg repack <file>` — optimize GeoPackage
- `gdal driver gti create <inputs...> <output>` — create GTI tile index
- `gdal driver openfilegdb repack <file>` — optimize FileGDB

---

## Common Patterns

### Convert Shapefile to GeoParquet
```bash
pixi run gdal vector convert input.shp output.parquet
```

### Reproject and convert in one pipeline
```bash
pixi run gdal vector pipeline \
  read input.gpkg \
  ! reproject --dst-crs EPSG:4326 \
  ! write output.parquet
```

### Create Cloud Optimized GeoTIFF
```bash
pixi run gdal raster convert input.tif output.cog.tif \
  --creation-option COMPRESS=DEFLATE
```

### Inspect remote file on S3
```bash
pixi run gdal info /vsis3/bucket/path/data.parquet
```

### Clip vector by bounding box
```bash
pixi run gdal vector clip input.gpkg output.gpkg \
  --bbox -10 35 45 70
```

---

## Cross-references
- Use the **geoparquet** skill when the target format is GeoParquet — gpio adds Hilbert sorting, bbox covering, validation, and STAC that GDAL doesn't
- Use the **spatial-analysis** skill for combined DuckDB spatial + GDAL analytical workflows
- Use the **duckdb-read-file** skill to quickly explore any data file via DuckDB before processing with GDAL
- Use the **duckdb-query** skill to run SQL queries on GDAL outputs (especially GeoParquet)
- Use the **data-pipeline** skill to chain GDAL + DuckDB + gpio operations as pixi tasks
- Use the **data-quality** agent to validate geometry and CRS after GDAL processing
- Use the **pipeline-orchestrator** agent to plan multi-step workflows across GDAL, DuckDB, and gpio
