---
name: geoparquet
description: >
  GeoParquet optimization and management using the gpio CLI (geoparquet-io).
  Use when creating, inspecting, validating, sorting, partitioning, enriching,
  or publishing GeoParquet files. Covers spatial indexing, compression tuning,
  STAC metadata, cloud distribution, and service extraction.
allowed-tools: Bash, Read, Write, Edit, Glob, Grep
---

You are a GeoParquet specialist using the **gpio CLI** (geoparquet-io).
Always follow GeoParquet best practices for compression, spatial ordering, and metadata.

Run gpio commands inside the pixi environment: `pixi run gpio ...`
Alternative: `pixi run python -m geoparquet_io ...`

### Installation

```bash
pixi add --pypi geoparquet-io --pre
```

The `--pre` flag is required because geoparquet-io is currently in beta.

---

## Quick Reference

### Command Groups

| Command | Purpose |
|---------|---------|
| `gpio inspect` | Examine GeoParquet metadata, schema, row groups, bbox, geo metadata |
| `gpio convert` | Convert between GeoParquet and other formats (GeoJSON, FlatGeobuf, Shapefile, CSV) |
| `gpio check` | Validate spec compliance, compression, bbox covering, spatial ordering |
| `gpio add` | Enrich with bbox covering columns, spatial indexes (H3, S2, A5, quadkey, KD-tree) |
| `gpio sort` | Sort spatially (Hilbert curve, S2, H3, geohash) for optimal range reads |
| `gpio partition` | Partition into multiple files by spatial or attribute strategy |
| `gpio extract` | Extract data from services (BigQuery, ArcGIS Feature Services, WFS) |
| `gpio publish` | Generate STAC metadata and publish to cloud storage (S3, GCS, Azure) |
| `gpio benchmark` | Measure spatial query performance, compare files, profile I/O |

---

## Inspection

```bash
# Full metadata inspection
pixi run gpio inspect input.parquet

# Schema and geo metadata only
pixi run gpio inspect input.parquet --schema

# Row group statistics
pixi run gpio inspect input.parquet --row-groups

# Bbox covering metadata
pixi run gpio inspect input.parquet --bbox

# Summary statistics
pixi run gpio inspect input.parquet --stats
```

---

## Conversion

```bash
# GeoJSON to GeoParquet
pixi run gpio convert input.geojson output.parquet

# GeoParquet to FlatGeobuf
pixi run gpio convert input.parquet output.fgb

# GeoParquet to GeoJSON
pixi run gpio convert input.parquet output.geojson

# CSV with coordinates to GeoParquet
pixi run gpio convert input.csv output.parquet --x-col lon --y-col lat --crs EPSG:4326

# With compression options
pixi run gpio convert input.geojson output.parquet --compression zstd --compression-level 15
```

---

## Validation

```bash
# Full validation suite — run this before publishing
pixi run gpio check all input.parquet

# Individual checks
pixi run gpio check spec input.parquet          # GeoParquet spec compliance
pixi run gpio check compression input.parquet    # Compression analysis
pixi run gpio check bbox input.parquet           # Bbox covering metadata validity
pixi run gpio check spatial-order input.parquet  # Spatial ordering quality
pixi run gpio check row-groups input.parquet     # Row group size analysis
```

Always run `gpio check all` before distributing GeoParquet files. It validates spec compliance, compression efficiency, bbox covering correctness, and spatial ordering quality.

---

## Adding Metadata and Indexes

### Bbox Covering

```bash
# Add bbox covering columns (required for efficient spatial queries)
pixi run gpio add bbox input.parquet output.parquet
```

### Spatial Indexes

```bash
# H3 index
pixi run gpio add index input.parquet output.parquet --type h3 --resolution 7

# S2 index
pixi run gpio add index input.parquet output.parquet --type s2 --level 13

# A5 index
pixi run gpio add index input.parquet output.parquet --type a5 --resolution 9

# Quadkey index
pixi run gpio add index input.parquet output.parquet --type quadkey --zoom 12

# KD-tree enrichment
pixi run gpio add index input.parquet output.parquet --type kdtree
```

---

## Spatial Sorting

```bash
# Hilbert sort (recommended — best spatial locality)
pixi run gpio sort hilbert input.parquet output.parquet

# S2 sort
pixi run gpio sort s2 input.parquet output.parquet

# H3 sort
pixi run gpio sort h3 input.parquet output.parquet

# Geohash sort
pixi run gpio sort geohash input.parquet output.parquet
```

Hilbert sorting is the **default recommendation** — it provides the best spatial locality for range queries and minimizes I/O for spatial filters.

---

## Partitioning

```bash
# KD-tree spatial partitioning (recommended for balanced spatial splits)
pixi run gpio partition input.parquet output_dir/ --strategy kdtree --max-rows 100000

# Administrative boundary partitioning
pixi run gpio partition input.parquet output_dir/ --strategy admin --column admin_level_2

# H3 partitioning
pixi run gpio partition input.parquet output_dir/ --strategy h3 --resolution 4

# S2 partitioning
pixi run gpio partition input.parquet output_dir/ --strategy s2 --level 8

# A5 partitioning
pixi run gpio partition input.parquet output_dir/ --strategy a5 --resolution 5

# Quadkey partitioning
pixi run gpio partition input.parquet output_dir/ --strategy quadkey --zoom 8

# String attribute partitioning
pixi run gpio partition input.parquet output_dir/ --strategy string --column country
```

---

## Service Extraction

```bash
# Extract from ArcGIS Feature Service
pixi run gpio extract arcgis "https://services.arcgis.com/.../FeatureServer/0" output.parquet

# Extract from WFS
pixi run gpio extract wfs "https://example.com/wfs" output.parquet --layer layer_name

# Extract from BigQuery
pixi run gpio extract bigquery "project.dataset.table" output.parquet
```

---

## Publishing

```bash
# Generate STAC metadata
pixi run gpio publish stac input.parquet --output stac_catalog/

# Publish to S3
pixi run gpio publish s3 input.parquet --bucket my-bucket --prefix data/

# Publish to GCS
pixi run gpio publish gcs input.parquet --bucket my-bucket --prefix data/
```

---

## Benchmarking

```bash
# Benchmark spatial query performance
pixi run gpio benchmark input.parquet --bbox 30 20 40 30

# Compare two files
pixi run gpio benchmark input_original.parquet input_optimized.parquet --bbox 30 20 40 30

# Profile I/O patterns
pixi run gpio benchmark input.parquet --profile
```

---

## GeoParquet Best Practices

### Compression
- Use **zstd compression at level 15** for the best size/speed tradeoff
- zstd level 15 achieves near-optimal compression without excessive CPU cost

### Spatial Ordering
- Always apply **Hilbert sorting** before distribution — it dramatically improves spatial query performance
- Hilbert curves preserve 2D locality better than Z-order, S2, or geohash

### Row Groups
- Target **50,000 to 150,000 rows per row group**
- Too small: excessive metadata overhead and many I/O requests
- Too large: poor predicate pushdown granularity and wasted reads

### Bbox Covering
- Always add **bbox covering metadata** — it enables efficient spatial filtering without reading geometry bytes
- bbox covering columns store per-row-group min/max bounds for x and y

### Distribution Checklist
1. Hilbert sort the data
2. Set zstd compression level 15
3. Add bbox covering metadata
4. Target 50k-150k rows per row group
5. Run `gpio check all` to validate
6. Generate STAC metadata for discoverability

---

## Python API

The geoparquet-io Python API provides a fluent, chainable interface:

```python
import geoparquet_io as gpio

# Full optimization pipeline
gpio.read('input.parquet').add_bbox().sort_hilbert().write('output.parquet')

# Read and inspect
gdf = gpio.read('input.parquet').to_geopandas()

# Convert with compression
gpio.read('input.geojson').write('output.parquet', compression='zstd', compression_level=15)

# Add spatial index and sort
gpio.read('input.parquet') \
    .add_bbox() \
    .add_index(type='h3', resolution=7) \
    .sort_hilbert() \
    .write('output.parquet', row_group_size=100000)

# Partition output
gpio.read('input.parquet') \
    .sort_hilbert() \
    .partition('output_dir/', strategy='kdtree', max_rows=100000)

# Validate
result = gpio.check('input.parquet')
print(result.summary())
```

---

## Integration with DuckDB

When using DuckDB directly for GeoParquet operations (via the duckdb-query skill), follow these manual best practices:

### Reading GeoParquet in DuckDB
```sql
-- Install and load spatial extension
INSTALL spatial; LOAD spatial;

-- Read GeoParquet
SELECT * FROM read_parquet('input.parquet') LIMIT 10;

-- Spatial filter using bbox covering (if present)
SELECT * FROM read_parquet('input.parquet')
WHERE bbox.xmin >= 30 AND bbox.xmax <= 40
  AND bbox.ymin >= 20 AND bbox.ymax <= 30;
```

### Writing GeoParquet from DuckDB
```sql
-- Write with optimal settings
COPY (
  SELECT * FROM read_parquet('input.parquet')
  WHERE country = 'SA'
) TO 'output.parquet' (
  FORMAT PARQUET,
  COMPRESSION ZSTD,
  COMPRESSION_LEVEL 15,
  ROW_GROUP_SIZE 100000
);
```

**Important**: DuckDB does not write GeoParquet geo metadata by default. After writing from DuckDB, always post-process with gpio to add proper GeoParquet metadata:

```bash
# Add geo metadata and bbox covering to DuckDB output
pixi run gpio add bbox duckdb_output.parquet final_output.parquet
pixi run gpio check all final_output.parquet
```

---

## Common Patterns

### Full Optimization Pipeline
```bash
# Sort, compress, add bbox, validate
pixi run gpio sort hilbert input.parquet sorted.parquet
pixi run gpio add bbox sorted.parquet optimized.parquet
pixi run gpio check all optimized.parquet
```

### Extract, Optimize, and Publish
```bash
# Extract from ArcGIS, optimize, publish
pixi run gpio extract arcgis "https://services.arcgis.com/.../FeatureServer/0" raw.parquet
pixi run gpio sort hilbert raw.parquet sorted.parquet
pixi run gpio add bbox sorted.parquet optimized.parquet
pixi run gpio check all optimized.parquet
pixi run gpio publish stac optimized.parquet --output stac_catalog/
pixi run gpio publish s3 optimized.parquet --bucket my-bucket --prefix data/
```

### Convert and Optimize from GDAL Output
```bash
# GDAL produces Parquet but not GeoParquet-optimized
pixi run gdal vector convert input.shp raw.parquet
pixi run gpio sort hilbert raw.parquet sorted.parquet
pixi run gpio add bbox sorted.parquet final.parquet
pixi run gpio check all final.parquet
```

---

## Cross-references
- Use the **gdal** skill for vector/raster format conversions before GeoParquet optimization
- Use the **duckdb-query** skill for SQL-based exploration and transformations of GeoParquet files
- Use the **duckdb-read-file** skill to quickly inspect GeoParquet files via DuckDB
- Use the **spatial-analysis** skill for spatial analytical workflows on GeoParquet data
- Use the **data-pipeline** skill to chain gpio + DuckDB + GDAL operations as pixi tasks
- Use the **data-explorer** agent to profile GeoParquet datasets and assess data quality
