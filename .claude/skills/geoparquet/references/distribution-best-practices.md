# GeoParquet Distribution Best Practices

A checklist and optimization guide for preparing GeoParquet files for distribution.

---

## Pre-Distribution Checklist

### Required Steps

- [ ] **Hilbert sort** — Apply Hilbert curve spatial sorting for optimal query locality
  ```bash
  pixi run gpio sort hilbert input.parquet sorted.parquet
  ```

- [ ] **Bbox covering metadata** — Add per-row-group bounding box columns for spatial predicate pushdown
  ```bash
  pixi run gpio add bbox sorted.parquet output.parquet
  ```

- [ ] **zstd compression at level 15** — Best size/speed tradeoff for distribution
  ```bash
  pixi run gpio convert input.parquet output.parquet --compression zstd --compression-level 15
  ```

- [ ] **Row group sizing (50k-150k rows)** — Balanced granularity for predicate pushdown
  ```bash
  pixi run gpio convert input.parquet output.parquet --row-group-size 100000
  ```

- [ ] **Validation** — Full spec compliance and optimization check
  ```bash
  pixi run gpio check all output.parquet
  ```

### Recommended Steps

- [ ] **Spatial index enrichment** — Add H3, S2, or quadkey columns for client-side spatial filtering
  ```bash
  pixi run gpio add index output.parquet enriched.parquet --type h3 --resolution 7
  ```

- [ ] **STAC metadata** — Generate catalog metadata for discoverability
  ```bash
  pixi run gpio publish stac output.parquet --output stac_catalog/
  ```

- [ ] **Benchmarking** — Verify spatial query performance meets expectations
  ```bash
  pixi run gpio benchmark output.parquet --bbox 30 20 40 30
  ```

---

## Compression Guide

### Codec Selection

| Codec | Compression Ratio | Read Speed | Write Speed | Recommendation |
|---|---|---|---|---|
| zstd (level 15) | Excellent | Fast | Moderate | **Default for distribution** |
| zstd (level 1-3) | Good | Very fast | Very fast | Interactive/development use |
| snappy | Moderate | Very fast | Very fast | Legacy compatibility |
| gzip | Good | Moderate | Slow | Broad compatibility |
| lz4 | Low | Very fast | Very fast | Speed-critical pipelines |
| brotli | Excellent | Moderate | Slow | Maximum compression needed |
| none | None | Fastest | Fastest | Temporary/intermediate files |

### Why zstd Level 15

- Compression ratio is within 5% of level 22 (maximum) for typical geospatial data
- Decompression speed is identical regardless of compression level
- Compression speed is 3-5x faster than levels 19-22
- Broadly supported by all Parquet readers (Arrow, DuckDB, Spark, etc.)

---

## Row Group Sizing

### Guidelines

| Dataset Size | Recommended Row Group Size | Reasoning |
|---|---|---|
| < 50,000 rows | Single row group | Avoid metadata overhead |
| 50k - 1M rows | 50,000 - 100,000 | Good pushdown granularity |
| 1M - 10M rows | 100,000 - 150,000 | Balanced I/O and granularity |
| > 10M rows | 100,000 - 150,000 | Consistent performance |

### Impact of Row Group Size

**Too small (< 10k rows)**:
- Excessive Parquet footer metadata
- Many small I/O operations for sequential reads
- Higher per-row overhead

**Too large (> 500k rows)**:
- Poor spatial predicate pushdown — must read large chunks even for small spatial queries
- Higher memory requirements for readers
- Wasted bandwidth when filtering returns few rows

**Sweet spot (50k-150k)**:
- Row group statistics (min/max bbox) enable effective spatial skipping
- Reasonable I/O unit size for cloud storage (typically 5-50 MB per row group)
- Good balance between metadata overhead and filtering granularity

---

## Spatial Ordering

### Why Spatial Sorting Matters

Without spatial sorting, a bbox query over a small area may need to read **every row group** because matching rows are scattered throughout the file. With Hilbert sorting, nearby geometries are stored in adjacent row groups, so the same query reads only 1-3 row groups.

### Sorting Algorithm Comparison

| Algorithm | Spatial Locality | Clustering Quality | Best For |
|---|---|---|---|
| **Hilbert** | Excellent | Best | General-purpose distribution |
| S2 | Very good | Very good | Global datasets with S2 ecosystem |
| H3 | Good | Good | Datasets already using H3 |
| Geohash | Moderate | Moderate | Simple implementations |

### Performance Impact

Typical spatial query speedup from Hilbert sorting (measured via `gpio benchmark`):

| Scenario | Unsorted | Hilbert Sorted | Speedup |
|---|---|---|---|
| Point-in-bbox (small area) | Full scan | 1-3 row groups | 10-100x |
| Regional filter (medium area) | Full scan | 5-20 row groups | 3-20x |
| Country-level filter | Full scan | 20-50% of row groups | 1.5-5x |

---

## Bbox Covering Metadata

### What It Does

Bbox covering adds per-row bounding box columns (`bbox.xmin`, `bbox.ymin`, `bbox.xmax`, `bbox.ymax`) and registers them in the GeoParquet `geo` metadata. This enables:

1. **Row-group-level skipping** — Parquet readers use column statistics (min/max of bbox columns) to skip entire row groups
2. **Row-level filtering** — Readers can filter rows by comparing bbox columns before deserializing geometry

### Without Bbox Covering

Readers must deserialize the full WKB geometry column to determine spatial bounds — expensive for complex geometries (polygons with thousands of vertices).

### With Bbox Covering

Readers check simple float columns first, skipping most rows/row groups without touching geometry bytes.

---

## Partitioning Strategy Guide

### When to Partition

| Scenario | Partition? | Strategy |
|---|---|---|
| < 1 GB, general use | No | Single file is fine |
| 1-10 GB, spatial queries | Maybe | Single sorted file often sufficient |
| > 10 GB | Yes | KD-tree or admin partitioning |
| Multi-country dataset | Yes | Admin (country/region column) |
| Tile-based consumers (web maps) | Yes | H3 or quadkey |
| Regular grid needed | Yes | S2 or A5 |

### Strategy Comparison

| Strategy | Balance | Predictability | Semantic Meaning | Best For |
|---|---|---|---|---|
| **kdtree** | Excellent | Low (data-dependent) | None | Balanced spatial splits |
| **admin** | Variable | High | Yes (admin names) | Country/region datasets |
| **h3** | Good | High | Yes (H3 cell IDs) | H3-based analysis pipelines |
| **s2** | Good | High | Yes (S2 cell IDs) | S2-based analysis pipelines |
| **a5** | Good | High | Yes (A5 cell IDs) | A5-based analysis pipelines |
| **quadkey** | Good | High | Yes (tile coordinates) | Web map tile consumers |
| **string** | Variable | High | Yes (attribute values) | Attribute-based access patterns |

### Partition File Naming

gpio generates partition files with predictable names:
```
output_dir/
  partition_0000.parquet
  partition_0001.parquet
  ...
```

For Hive-style partitioning (admin, string strategies):
```
output_dir/
  country=SA/data.parquet
  country=AE/data.parquet
  ...
```

---

## Cloud Storage Optimization

### File Layout for Cloud

- **Single file** (< 5 GB): Simplest to manage, good for most use cases
- **Partitioned directory** (> 5 GB): Better for partial reads of large datasets
- **Hive partitioned** (attribute access): When consumers filter by known attribute values

### HTTP Range Request Optimization

GeoParquet on cloud storage is read via HTTP range requests. Optimization targets:

1. **Minimize number of requests** — Fewer, larger row groups reduce request count
2. **Minimize bytes read** — Spatial sorting + bbox covering skip irrelevant data
3. **Footer caching** — Small, well-structured footers are cached by CDNs

### Cloud Publishing Checklist

- [ ] Hilbert sorted (minimizes range requests for spatial queries)
- [ ] zstd compressed (best decompression speed for range-read workloads)
- [ ] Bbox covering present (enables server-side or client-side row group skipping)
- [ ] STAC metadata generated (enables catalog discovery)
- [ ] Content-Type set to `application/vnd.apache.parquet` on storage objects
- [ ] CORS configured if serving directly to browser-based clients
- [ ] CDN caching enabled for static datasets

---

## Validation Workflow

### Before Distribution

```bash
# 1. Run full validation
pixi run gpio check all output.parquet

# 2. Review the report — all checks should pass:
#    - spec: GeoParquet metadata is valid
#    - compression: zstd is used with appropriate level
#    - bbox: bbox covering columns are present and correct
#    - spatial-order: spatial ordering quality is acceptable
#    - row-groups: row group sizes are within recommended range
```

### Interpreting Check Results

| Check | Pass Criteria | Fix If Failing |
|---|---|---|
| `spec` | Valid geo metadata, correct CRS, geometry encoding | Re-create with gpio or fix metadata |
| `compression` | zstd compression detected | Re-write with `--compression zstd --compression-level 15` |
| `bbox` | Bbox covering columns present and accurate | Run `gpio add bbox` |
| `spatial-order` | Hilbert or similar ordering detected | Run `gpio sort hilbert` |
| `row-groups` | 50k-150k rows per group | Re-write with `--row-group-size 100000` |

---

## Complete Distribution Pipeline

```bash
#!/bin/bash
# Full optimization pipeline for distribution-ready GeoParquet

INPUT="raw.parquet"
SORTED="tmp_sorted.parquet"
OPTIMIZED="final.parquet"

# Step 1: Hilbert sort for spatial locality
pixi run gpio sort hilbert "$INPUT" "$SORTED"

# Step 2: Add bbox covering and set optimal compression
pixi run gpio add bbox "$SORTED" "$OPTIMIZED"

# Step 3: Validate everything
pixi run gpio check all "$OPTIMIZED"

# Step 4: Benchmark spatial query performance
pixi run gpio benchmark "$OPTIMIZED" --bbox 30 20 40 30

# Step 5: Generate STAC metadata
pixi run gpio publish stac "$OPTIMIZED" --output stac_catalog/

# Step 6: Publish to cloud
pixi run gpio publish s3 "$OPTIMIZED" --bucket my-bucket --prefix data/

# Cleanup
rm -f "$SORTED"
```
