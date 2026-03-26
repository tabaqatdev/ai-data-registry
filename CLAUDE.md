# Project: ai-data-registry

## Overview
Geospatial data processing mono-repo using pixi for reproducible environment management.
Multi-workspace project — each workspace has its own `pixi.toml`, language runtime, dependencies, and tasks.

## Package Manager: Pixi
- **Config**: Each workspace has its own `pixi.toml`
- **Root**: `pixi.toml` at project root defines shared tools
- **Lock file**: Single `pixi.lock` at root for all workspaces (auto-generated, never edit manually)
- **Environments**: `.pixi/envs/` (gitignored)
- **Channels**: conda-forge

---

## Multi-Workspace Architecture

```
ai-data-registry/
├── pixi.toml              # Root — shared tools (GDAL, DuckDB, gpio, pnpm, Python)
├── pixi.lock              # Single lock file for ALL workspaces
├── .claude/               # AI rules, skills, agents, commands (project-wide)
├── workspace-a/
│   └── pixi.toml          # Own runtime, deps, tasks (NO separate pixi.lock)
├── workspace-b/
│   └── pixi.toml
```

### Creating a Sub-Workspace
```bash
# From project root:
mkdir my-workspace
cd my-workspace
pixi init . --channel conda-forge --platform osx-arm64 --platform linux-64 --platform win-64
cd ..
pixi workspace register --name my-workspace --path my-workspace

# Add deps targeting the workspace (from root):
pixi add -w my-workspace python
pixi add -w my-workspace <other-deps>
```
Or use `/project:new-workspace <name> <language>` for guided setup.

### Workspace Isolation Principles

**What lives in root `pixi.toml` (shared):**
- GDAL, DuckDB, gpio, pnpm, Python, Node.js — tools used by ALL workspaces
- Cross-workspace orchestration tasks only

**What lives in each workspace `pixi.toml` (isolated):**
- The workspace's own language runtime (may differ from root Python)
- All workspace-specific dependencies
- All workspace-specific tasks
- Platform-specific deps via `[target.<platform>.dependencies]`

**Boundaries — NEVER cross these:**
- Never add workspace-specific deps to root `pixi.toml`
- Never assume a workspace uses Python — always check its `pixi.toml` first
- Never share state between workspaces (each has its own `.pixi/envs/`)
- GeoParquet is the interchange format when workspaces need to share data

**Running commands:**
```bash
# Shared tools (from root — uses root pixi.toml)
pixi run duckdb -csv -c "SELECT 42"
pixi run gdal info input.gpkg
pixi run gpio inspect summary file.parquet

# Workspace tasks (from root, using -w flag)
pixi run -w workspace-a <task>

# Adding deps to a workspace (from root)
pixi add -w workspace-a <pkg>
pixi add -w workspace-a --pypi <pkg>
```

---

## Conventions
- **All tools run through pixi** — never run `duckdb`, `gdal`, `gpio`, `python`, `node`, `pnpm` directly
- `pixi run pnpm` — NEVER npm or yarn (npm is denied in settings.json)
- **GeoParquet is the standard interchange format** — validate with `pixi run gpio check all`
- New unified `gdal` CLI (v3.11+) — NOT legacy `ogr2ogr`/`gdalinfo`/`ogrinfo`
- Tasks in `[tasks]` of each workspace's `pixi.toml`, not Makefiles
- Never commit `.pixi/` environments (only `.pixi/config.toml` is tracked)
- `pixi.lock` is committed but treated as binary (see `.gitattributes`)

### Adding Dependencies: conda vs PyPI

**IMPORTANT:** Pixi supports two package sources. Always prefer conda-forge; fall back to PyPI only when the package is not available on conda-forge.

| Source | Command (root) | Command (workspace) | When to use |
|--------|---------------|---------------------|-------------|
| **conda-forge** | `pixi add <pkg>` | `pixi add -w <workspace> <pkg>` | Default — native compiled packages, C/C++ libraries, runtimes (Python, Node, GDAL, DuckDB) |
| **PyPI** | `pixi add --pypi <pkg>` | `pixi add -w <workspace> --pypi <pkg>` | Only when the package does not exist on conda-forge (pure Python packages, niche tools) |

**Decision flow:**
1. Search conda-forge first: `pixi search <pkg>` — if found, use `pixi add <pkg>`
2. If not on conda-forge, use `pixi add --pypi <pkg>`
3. Never mix — do not add the same package from both sources
4. Conda packages go in `[dependencies]`, PyPI packages go in `[pypi-dependencies]` in `pixi.toml`

---

## Reference: Rules (`.claude/rules/`)

Rules load automatically when working with matching files. Path-scoped rules only activate for files matching their `paths:` glob.

| Rule | Scope | When it activates | What it enforces |
|------|-------|-------------------|-----------------|
| `tool-execution.md` | Global | Always | All tools via `pixi run`, workspace targeting patterns |
| `pixi.md` | `**/pixi.toml`, `**/pixi.lock` | Editing pixi config | Deps format, tasks, workspace registration, platform-specific patterns |
| `workspaces.md` | Global | Always | Isolation principles, workspace creation, shared vs isolated deps |
| `duckdb.md` | `**/*.sql`, `**/*.py` | SQL or Python files | DuckDB dialect, Friendly SQL, spatial extension, GeoParquet best practices |
| `geospatial.md` | `**/*.parquet`, `**/*.gpkg`, `**/*.shp`, `**/*.tif`, etc. | Spatial files | GeoParquet as standard, tool selection (gpio vs gdal vs duckdb), CRS rules |
| `nodejs.md` | `**/*.js`, `**/*.ts`, `**/package.json` | Node/JS files | pnpm only, workspace Node.js patterns, playwright setup |

## Reference: Commands (`.claude/commands/`)

Slash commands for common operations. Invoked as `/project:<name>`. All use `pixi run` and work cross-platform.

| Command | Usage | What it does |
|---------|-------|------|
| `/project:new-workspace` | `<name> <language>` | Scaffold a sub-workspace: init, add runtime, register, generate tasks |
| `/project:env-info` | (no args) | Show pixi env, installed packages, tool versions, registered workspaces |
| `/project:add-dep` | `<package> [--pypi] [-w workspace]` | Add dependency (conda-forge preferred, PyPI fallback) |
| `/project:query` | `<SQL or description>` | Run DuckDB query via `pixi run duckdb` |
| `/project:run-in` | `<workspace> <task>` | Run a pixi task in a specific workspace |
| `/project:inspect-file` | `<file-path>` | Inspect any data file — schema, row count, samples, spatial info |
| `/project:convert` | `<input> <output>` | Convert between geospatial formats (GeoParquet, GeoJSON, GeoPackage, etc.) |

## Reference: Skills (`.claude/skills/`)

Skills are invoked automatically when the task matches, or explicitly. All use `pixi run`.

| Skill | When to use | Tool |
|-------|-------------|------|
| **geoparquet** | Creating, validating, optimizing, partitioning GeoParquet; STAC metadata; spatial indexing (H3/S2/A5) | `pixi run gpio` |
| **gdal** | Vector/raster format conversion, reprojection, pipeline, terrain analysis, VSI remote files | `pixi run gdal` |
| **spatial-analysis** | Spatial SQL queries, geometry operations, CRS transforms, spatial joins, DuckDB + GDAL combined | `pixi run duckdb` + `pixi run gdal` |
| **data-pipeline** | Building ETL pipelines as pixi tasks with `depends-on`, multi-tool chaining | all tools |
| **duckdb-query** | SQL queries (Friendly SQL), natural language → SQL, ad-hoc or session mode | `pixi run duckdb` |
| **duckdb-read-file** | Explore any file: CSV, Parquet, Excel, JSON, spatial, Avro, SQLite, Jupyter, remote (S3/GCS/Azure) | `pixi run duckdb` |
| **duckdb-attach-db** | Attach a .duckdb file for persistent querying across sessions | `pixi run duckdb` |
| **duckdb-docs** | Search DuckDB documentation via full-text search (cached locally) | `pixi run duckdb` |
| **duckdb-install** | Install or update DuckDB extensions (spatial, httpfs, fts, community exts) | `pixi run duckdb` |
| **duckdb-read-memories** | Recover context from past Claude Code sessions via DuckDB JSONL queries | `pixi run duckdb` |
| **duckdb-state** | Initialize/manage shared `state.sql` (extensions, credentials, macros, locking) | `pixi run duckdb` |
| **env-check** | Validate environment health: pixi, DuckDB, GDAL, gpio versions, extension status, compatibility | `pixi run` |
| **playwright-skill** | Browser automation, testing, screenshots, responsive design, form testing, link checking | `pixi run node` |

## Reference: Agents (`.claude/agents/`)

Agents are spawned as subprocesses for complex tasks. They run autonomously and report back.

| Agent | When it's used | What it does |
|-------|---------------|------|
| **data-explorer** | Proactively when investigating any data file | Profiles datasets: row count, schema, nulls, types, CRS, geometry, Parquet metadata |
| **data-quality** | When validating data integrity | Deep checks: null rates, cardinality, duplicates, outliers, geometry validity, CRS consistency, GeoParquet spec |
| **pipeline-orchestrator** | When planning multi-step workflows | Routes operations to right tool (GDAL/DuckDB/gpio), generates pixi task definitions, plans step order |

---

## Root-Level Shared Tools

| Tool | Version | Command | Purpose |
|------|---------|---------|---------|
| GDAL | >=3.12.3 | `pixi run gdal ...` | Unified vector/raster CLI (v3.11+) |
| DuckDB | >=1.5.1 | `pixi run duckdb ...` | Analytical SQL engine |
| gpio | beta | `pixi run gpio ...` | GeoParquet optimization/validation |
| libgdal-arrow-parquet | >=3.12.3 | (GDAL driver) | Parquet I/O via Arrow |
| pnpm | >=10.32.1 | `pixi run pnpm ...` | Node package manager (NEVER npm) |
| Python | >=3.14.3 | `pixi run python ...` | Default runtime |
| Node.js | via pixi | `pixi run node ...` | Node.js runtime |

## Platforms
osx-arm64, linux-64, win-64 — all dependencies must be cross-platform compatible.

---

## Watch Out For
- Run **env-check** skill after setup or when things break — it validates everything
- Always run `pixi install` after pulling to sync the environment
- GDAL version must match libgdal-arrow-parquet version
- DuckDB spatial extension: `INSTALL spatial; LOAD spatial;` (or use **duckdb-state** skill)
- Always validate GeoParquet: `pixi run gpio check all <file>`
- Use `pixi run pnpm` not `npm` — npm is denied in settings.json
- Python 3.14 is bleeding edge — some PyPI packages may not have wheels
- Each workspace may use a different language — check its `pixi.toml` first
- Never mix workspace dependencies — isolation is enforced
