---
paths:
  - "workspaces/**"
  - "pixi.toml"
---
# Multi-Workspace Isolation Rules

This is a multi-workspace mono-repo. Each sub-workspace under `workspaces/` is a standalone pixi project with its own `pixi.toml`, committed `pixi.lock`, deps, and tasks. Root `pixi.toml` and `pixi.lock` cover shared tools only (gdal, duckdb, s5cmd, gpio).

**Naming distinction:**
- **Workspace** = the directory (`workspaces/weather/`) providing an isolated pixi environment
- **Schema** = the data namespace in `[tool.registry].schema`, used as S3 prefix and DuckLake schema

Reference implementation: `workspaces/test-minimal/`

## Isolation Rules

- **Never** add workspace-specific deps to root `pixi.toml`. Each workspace owns its own.
- Each workspace may use a different language. Check its `pixi.toml` first.
- Never share state between workspaces.
- GeoParquet is the interchange format when workspaces share data.

## Running Commands

- Shared tools: `pixi run <tool>` from root (gdal, duckdb, s5cmd, gpio)
- Workspace tasks: `pixi run --manifest-path workspaces/<workspace>/pixi.toml <task>` from root
- Or: `cd workspaces/<workspace> && pixi run <task>`
- Add deps: `cd workspaces/<workspace> && pixi add <pkg>` (conda-forge) or `--pypi <pkg>` (fallback)

For workspace creation steps, see @CONTRIBUTING.md.
