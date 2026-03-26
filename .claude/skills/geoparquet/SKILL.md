---
name: geoparquet
description: >
  GeoParquet optimization via gpio CLI (geoparquet-io). Use when creating, inspecting,
  validating, sorting, partitioning, or enriching GeoParquet files — even if the user
  just says "optimize this parquet" or "add spatial index". Covers Hilbert sorting,
  compression, bbox covering, STAC metadata, and cloud upload.
allowed-tools: Bash, Read, Write, Edit, Glob, Grep
---

Run via `pixi run gpio`. Install: `pixi add --pypi geoparquet-io --pre`

## Commands

### inspect — Examine metadata
```bash
pixi run gpio inspect input.parquet            # Full metadata
pixi run gpio inspect input.parquet --schema   # Schema + geo metadata
pixi run gpio inspect input.parquet --row-groups
pixi run gpio inspect input.parquet --bbox
pixi run gpio inspect input.parquet --stats
```

### check — Validate (run before distributing)
```bash
pixi run gpio check all input.parquet           # Full validation suite
pixi run gpio check spec input.parquet          # GeoParquet spec compliance
pixi run gpio check compression input.parquet
pixi run gpio check bbox input.parquet
pixi run gpio check spatial-order input.parquet
pixi run gpio check row-groups input.parquet
```

### add — Enrich with metadata/indexes
```bash
pixi run gpio add bbox input.parquet output.parquet                              # Bbox covering
pixi run gpio add index input.parquet output.parquet --type h3 --resolution 7    # H3
pixi run gpio add index input.parquet output.parquet --type s2 --level 13        # S2
pixi run gpio add index input.parquet output.parquet --type a5 --resolution 9    # A5
pixi run gpio add index input.parquet output.parquet --type quadkey --zoom 12    # Quadkey
pixi run gpio add index input.parquet output.parquet --type kdtree               # KD-tree
```

### sort — Spatial ordering
```bash
pixi run gpio sort hilbert input.parquet output.parquet    # Recommended
pixi run gpio sort s2 input.parquet output.parquet
pixi run gpio sort h3 input.parquet output.parquet
pixi run gpio sort geohash input.parquet output.parquet
```

### partition — Split into multiple files
```bash
pixi run gpio partition input.parquet out_dir/ --strategy kdtree --max-rows 100000
pixi run gpio partition input.parquet out_dir/ --strategy admin --column admin_level_2
pixi run gpio partition input.parquet out_dir/ --strategy h3 --resolution 4
pixi run gpio partition input.parquet out_dir/ --strategy s2 --level 8
pixi run gpio partition input.parquet out_dir/ --strategy string --column country
```

### extract — Pull from services
```bash
pixi run gpio extract arcgis "https://services.arcgis.com/.../FeatureServer/0" output.parquet
pixi run gpio extract wfs "https://example.com/wfs" output.parquet --layer layer_name
pixi run gpio extract bigquery "project.dataset.table" output.parquet
```

### upload — Push to cloud storage
```bash
pixi run gpio upload input.parquet --bucket my-bucket --prefix data/     # S3
pixi run gpio upload input.parquet --provider gcs --bucket my-bucket     # GCS
```

## Best Practices

- **Compression**: zstd level 15 (best size/speed tradeoff)
- **Sorting**: Hilbert (best spatial locality for range queries)
- **Row groups**: 50k-150k rows (balance metadata overhead vs predicate pushdown)
- **Bbox covering**: always add (enables spatial filtering without reading geometry)

### Distribution checklist
1. `gpio sort hilbert` → `gpio add bbox` → `gpio check all`
2. Target zstd compression level 15, row groups 50k-150k
3. Generate STAC metadata for discoverability

### Full optimization pipeline
```bash
pixi run gpio sort hilbert input.parquet sorted.parquet
pixi run gpio add bbox sorted.parquet optimized.parquet
pixi run gpio check all optimized.parquet
```

### Post-GDAL/DuckDB output
DuckDB and GDAL write Parquet but not GeoParquet-optimized. Always post-process:
```bash
pixi run gpio add bbox duckdb_output.parquet final.parquet
pixi run gpio check all final.parquet
```

## Cross-references
- **gdal** skill — vector/raster format conversions before GeoParquet optimization
- **data-pipeline** skill — chain gpio + DuckDB + GDAL operations as pixi tasks
