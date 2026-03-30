---
paths:
  - "**/pixi.toml"
---
# Workspace Contract Rules

When creating or editing a workspace `pixi.toml` (not the root), enforce the data registry contract.
Full architecture details: `research/architecture.md`

## Required `[tool.registry]` Section

Every workspace pixi.toml MUST have:

```toml
[tool.registry]
description = "What this workspace extracts"
schedule = "0 6 * * *"        # cron expression
timeout = 30                  # minutes
tags = ["topic1", "topic2"]
schema = "unique_name"        # = S3 prefix = DuckLake schema
table = "table_name"          # DuckLake table name
mode = "append"               # append | replace | upsert
# partition_by = "year(date)" # optional

[tool.registry.runner]
backend = "github"            # github | hetzner | huggingface
flavor = "ubuntu-latest"      # must match backend (see below)
# image = "ghcr.io/..."       # required for huggingface only

[tool.registry.license]
code = "Apache-2.0"           # OSI-approved SPDX
data = "CC-BY-4.0"            # recognized SPDX
data_source = "Source Name"
mixed = false

[tool.registry.checks]
min_rows = 1000
max_null_pct = 5
geometry = true
unique_cols = ["id_col"]
schema_match = true
```

## Allowed Backend + Flavor Combinations

| Backend | Allowed flavors | GPU | When to use |
|---------|----------------|-----|-------------|
| `github` | `ubuntu-latest` | No | Lightweight: CSV/JSON downloads, API calls |
| `hetzner` | `cax11`, `cax21`, `cax31`, `cax41` | No | Medium: spatial processing, large downloads |
| `huggingface` | `t4-medium`, `a10g-large`, `a100-large` | Yes | GPU: ML inference, embeddings |

## Required Tasks

Every workspace MUST define these tasks:

```toml
[tasks]
setup = "..."                                                    # optional but standardized
extract = { cmd = "...", depends-on = ["setup"], env = { OUTPUT_DIR = "output" } }
validate = { cmd = "...", depends-on = ["extract"] }
pipeline = { depends-on = ["setup", "extract", "validate"] }     # entry point
dry-run = { cmd = "...", env = { DRY_RUN = "1", OUTPUT_DIR = "output" } }
```

- `extract` MUST write Parquet to `$OUTPUT_DIR/`
- `pipeline` is what the runner calls: `pixi run -w {name} pipeline`
- `dry-run` is what PR validation calls (sample output only)
- Chain stops on any non-zero exit

## MUST NOT

1. Write to S3 directly (workflow uploads via s5cmd on behalf)
2. Declare a `schema` that conflicts with another workspace
3. Bundle credentials in code (use `$WORKSPACE_SECRET_*` env vars)
4. Declare unsupported backends or flavors
5. Include infrastructure configs (Terraform, provisioning scripts)
