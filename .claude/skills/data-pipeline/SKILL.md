---
name: data-pipeline
description: >
  Build data processing pipelines using DuckDB, GDAL, and gpio as pixi tasks.
  Use when creating ETL workflows, multi-step data transformations, format conversions,
  or batch processing — even if the user just says "process this data" or "build a pipeline".
allowed-tools: Read, Write, Edit, Glob, Grep, Bash
---

All tools via pixi: `pixi run duckdb`, `pixi run gdal`, `pixi run gpio`, `pixi run python`.

## Tool routing
1. **DuckDB** — tabular/analytical transforms (SQL-based)
2. **GDAL** — format conversions, reprojection (unified CLI, not legacy)
3. **gpio** — GeoParquet optimization (sort, bbox, check, partition)
4. **GeoParquet** — preferred intermediate format between steps

## Pipeline as pixi tasks
```toml
[tasks.extract]
cmd = "pixi run gdal vector convert data/raw/input.shp data/interim/input.parquet"

[tasks.transform]
cmd = "pixi run duckdb -c \"COPY (SELECT * FROM read_parquet('data/interim/input.parquet') WHERE area > 10) TO 'data/interim/filtered.parquet' (FORMAT PARQUET);\""
depends-on = ["extract"]

[tasks.optimize]
cmd = "pixi run gpio sort hilbert data/interim/filtered.parquet data/processed/output.parquet"
depends-on = ["transform"]

[tasks.validate]
cmd = "pixi run gpio check all data/processed/output.parquet"
depends-on = ["optimize"]

[tasks.pipeline]
depends-on = ["extract", "transform", "optimize", "validate"]
```

## Guidelines
- Source files in `data/raw/`, intermediates in `data/interim/`, outputs in `data/processed/`
- Name tasks as `<pipeline>-<step>`
- Always add validation step at end
- Use `depends-on` for DAG ordering
- Use `"""` multi-line TOML for long commands

## Cross-references
- **geoparquet** skill — gpio optimization, validation, and STAC metadata
- **gdal** skill — full unified GDAL CLI reference
