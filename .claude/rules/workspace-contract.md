---
paths:
  - "**/pixi.toml"
---
# Workspace Contract Rules

When creating or editing a workspace `pixi.toml` (not the root), enforce the data registry contract.
Full architecture details: `research/architecture.md`
Working example: `workspaces/test-minimal/` (minimal reference implementation)

## Naming: Workspace vs Schema

- **Workspace directory** (`workspaces/test-minimal/`): the isolated pixi environment with its own deps, tasks, and scripts
- **Schema** (`schema = "test-minimal"` in `[tool.registry]`): the DuckLake schema name and S3 prefix where output data lives
- These often match but they are separate concepts. A workspace named `weather-ingest` could write to schema `weather`

## Required `[tool.registry]` Section

Every workspace pixi.toml MUST have:

```toml
[tool.registry]
description = "What this workspace extracts"
schedule = "0 6 * * *"        # cron expression
timeout = 30                  # minutes
tags = ["topic1", "topic2"]
schema = "unique_name"        # S3 prefix + DuckLake schema (the data namespace)
mode = "append"               # append | replace | upsert

# Single table:
table = "table_name"
# Multiple tables:
tables = ["states", "flights"]

[tool.registry.runner]
backend = "github"            # github | hetzner | huggingface
flavor = "ubuntu-latest"      # must match backend (see below)
# image = "ghcr.io/..."       # required for huggingface only

[tool.registry.license]
code = "Apache-2.0"           # OSI-approved SPDX
data = "CC-BY-4.0"            # recognized SPDX
data_source = "Source Name"
mixed = false

# Global checks (apply to all tables unless overridden)
[tool.registry.checks]
min_rows = 1000
max_null_pct = 5
geometry = true
unique_cols = ["id_col"]
schema_match = true
```

## Multi-Table Workspaces

When a workspace produces multiple output files, use `tables` (list) instead of `table` (string). Each table gets its own Parquet file named `<table>.parquet` in `$OUTPUT_DIR/`.

Per-table quality checks override the global `[tool.registry.checks]` defaults:

```toml
[tool.registry]
tables = ["states", "flights"]

[tool.registry.checks]
schema_match = true

[tool.registry.checks.states]
min_rows = 1000
max_null_pct = 70
geometry = true
unique_cols = ["icao24", "snapshot_time"]

[tool.registry.checks.flights]
min_rows = 0
max_null_pct = 5
geometry = false
unique_cols = ["icao24", "first_seen"]
optional = true        # don't fail if file is missing
```

## File Naming Convention

- Extract script writes `<table_name>.parquet` to `$OUTPUT_DIR/` (one file per table)
- The CI workflow timestamps and uploads to S3 as: `s3://bucket/<schema>/<table>/<timestamp>.parquet`
- Each table gets its own S3 subdirectory. Workspaces never choose S3 paths directly.

## Allowed Backend + Flavor Combinations

| Backend | Allowed flavors | GPU | When to use |
|---------|----------------|-----|-------------|
| `github` | `ubuntu-latest` | No | Lightweight: CSV/JSON downloads, API calls |
| `hetzner` | `cax11`, `cax21`, `cax31`, `cax41` | No | Medium: spatial processing, large downloads |
| `huggingface` | `cpu-basic`, `cpu-upgrade`, `t4-small`, `t4-medium`, `l4x1`, `a10g-small`, `a10g-large`, `a10g-largex2`, `a100-large` | Yes (except cpu-*) | GPU: ML inference, embeddings |

## Required Tasks

Every workspace MUST define these tasks:

```toml
[tasks]
extract = "python extract.py"                                    # writes Parquet to $OUTPUT_DIR
validate = { cmd = "python validate_local.py", depends-on = ["extract"] }
pipeline = { depends-on = ["extract", "validate"] }              # entry point
dry-run = { cmd = "python extract.py", env = { DRY_RUN = "1" } } # sample output for PR validation
```

- `extract` MUST write one `<table_name>.parquet` per declared table to `$OUTPUT_DIR/`
- Do NOT hardcode `OUTPUT_DIR` in task `env`. CI passes its own value.
- `pipeline` is what the runner calls: `pixi run -w {name} pipeline`
- `dry-run` is what PR validation calls (sample output only)
- Chain stops on any non-zero exit

## MUST NOT

1. Write to S3 directly (workflow uploads via s5cmd on your behalf)
2. Declare a `schema.table` that conflicts with another workspace
3. Bundle credentials in code (use `$WORKSPACE_SECRET_*` env vars)
4. Declare unsupported backends or flavors
5. Include infrastructure configs (Terraform, provisioning scripts)
6. Hardcode `OUTPUT_DIR` in pixi task `env` (breaks CI override)
7. Use fixed output filenames that don't match a declared table name
