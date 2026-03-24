---
name: data-pipeline
description: Build data processing pipelines using DuckDB and GDAL. Use when creating ETL workflows, data transformations, format conversions, or batch processing of geospatial data files.
allowed-tools: Read, Write, Edit, Glob, Grep, Bash
---
Build a data processing pipeline for this project.

All CLI tools run via pixi: `pixi run duckdb`, `pixi run gdal`, `pixi run python`.

When building pipelines:
1. Use **DuckDB** (`pixi run duckdb`) for tabular/analytical transformations (SQL-based)
2. Use **GDAL** (`pixi run gdal`) for geospatial format conversions — use the new unified CLI, not legacy tools
3. Prefer **GeoParquet** as the intermediate format between pipeline steps
4. Define pipeline steps as **pixi tasks** in pixi.toml with `depends-on` for ordering
5. Use Python scripts in a `scripts/` or `pipelines/` directory
6. Include error handling and logging
7. Document input/output formats and CRS expectations

Pipeline task pattern in pixi.toml:
```toml
[tasks.extract]
cmd = "python scripts/extract.py"

[tasks.transform]
cmd = "python scripts/transform.py"
depends-on = ["extract"]

[tasks.load]
cmd = "python scripts/load.py"
depends-on = ["transform"]

[tasks.pipeline]
depends-on = ["extract", "transform", "load"]
```

For format conversions inside pipeline scripts, prefer:
- `pixi run gdal vector convert input.shp output.parquet` — vector conversion
- `pixi run gdal raster convert input.tif output.cog.tif` — raster conversion
- `pixi run gdal vector pipeline read input.gpkg ! reproject --dst-crs EPSG:4326 ! write output.parquet` — chained ops

For SQL transformations, use DuckDB CLI:
```bash
pixi run duckdb -csv -c "COPY (SELECT * FROM read_parquet('input.parquet') WHERE ...) TO 'output.parquet' (FORMAT PARQUET)"
```

For GeoParquet optimization as a pipeline step:
```bash
pixi run gpio convert geoparquet input.parquet output.parquet  # Hilbert sort, zstd, bbox covering
pixi run gpio check all output.parquet                         # Validate spec compliance
pixi run gpio partition kdtree output.parquet partitions/       # Partition if >2GB
```

## Cross-references
- Use the **geoparquet** skill for GeoParquet conversion, validation, optimization, and STAC metadata
- Use the **gdal** skill for the full unified GDAL CLI reference
- Use the **duckdb-query** skill for interactive SQL exploration during development
- Use the **duckdb-read-file** skill to inspect data files before building pipeline steps
- Use the **duckdb-state** skill to initialize DuckDB session state before pipeline runs
- Use the **spatial-analysis** skill for spatial-specific ETL patterns
- Use the **data-quality** agent to validate pipeline outputs (schema, geometry, CRS)
- Use the **data-explorer** agent to profile datasets and assess data quality
- Use the **pipeline-orchestrator** agent to plan multi-step workflows across tools
- Use the **env-check** skill to validate environment before running pipelines
