# Multi-Workspace Rules

This is a multi-workspace mono-repo. Each sub-workspace is a directory with its own `pixi.toml`, registered in the root workspace via `pixi workspace register`.

## Architecture

```
ai-data-registry/
├── pixi.toml              # Root workspace — shared tools (GDAL, DuckDB, pnpm, Python)
├── pixi.lock              # Root lock file
├── workspace-a/
│   ├── pixi.toml          # Sub-workspace — own runtime, deps, tasks
│   └── pixi.lock
├── workspace-b/
│   ├── pixi.toml
│   └── pixi.lock
```

## Creating a New Sub-Workspace

Use `/project:new-workspace <name> <language>` or manually:

```bash
# 1. Create and initialize
mkdir <name> && cd <name>
pixi init . --channel conda-forge --platform osx-arm64 --platform linux-64 --platform win-64

# 2. Add language runtime and dependencies
pixi add python                    # or go, nodejs, rust, etc.
pixi add <workspace-specific-deps>

# 3. Register in root workspace (from root directory)
cd ..
pixi workspace register --name <name> --path <name>

# 4. Verify registration
pixi workspace register list
```

## Separation of Concerns
- **Never** add workspace-specific deps to the root `pixi.toml` — each workspace owns its own
- **Never** run a workspace task from the root unless it's a root-level orchestration task
- Each workspace may use a different language (Python, Go, Node, Rust, etc.)
- Always check the workspace's `pixi.toml` to understand its runtime before making changes

## Shared Root Tools
The root `pixi.toml` provides tools available to all workspaces:
- `pixi run duckdb` — DuckDB CLI
- `pixi run gdal` — unified GDAL CLI
- `pixi run gpio` — GeoParquet CLI
- `pixi run python` — Python runtime
- `pixi run pnpm` — Node package manager (NEVER npm)
- `pixi run node` — Node.js runtime

## Running Commands

### In a specific workspace
```bash
cd <workspace>/ && pixi run <task>
# or from root:
pixi run --manifest-path <workspace>/pixi.toml <task>
```

### Root shared tools
```bash
pixi run duckdb -csv -c "SELECT 42"
pixi run gdal info input.gpkg
```

### Root tools from inside a workspace
If workspace doesn't declare its own copy of a tool:
```bash
pixi run --manifest-path ../pixi.toml duckdb -csv -c "SELECT 42"
```

## Adding Dependencies
```bash
cd <workspace>/ && pixi add <pkg>       # Workspace-specific
cd <workspace>/ && pixi add --pypi <pkg> # PyPI package to workspace
# Root-level shared:
pixi add <pkg>                           # From root directory
```

## Advanced Task Patterns

### Environment variables in tasks
```toml
[tasks]
serve = { cmd = "python app.py", env = { DB_PATH = "$CONDA_PREFIX/data/db.duckdb" } }
```

### Task dependencies (chaining)
```toml
[tasks]
extract = "python scripts/extract.py"
transform = { cmd = "python scripts/transform.py", depends-on = ["extract"] }
load = { cmd = "python scripts/load.py", depends-on = ["transform"] }
pipeline = { depends-on = ["extract", "transform", "load"] }
```

### Platform-specific dependencies
```toml
[dependencies]
gdal = ">=3.12.3,<4"

[target.unix.dependencies]
libduckdb = ">=1.5.1,<2"

[target.osx-arm64.dependencies]
mac-specific-tool = ">=1.0"

[target.win-64.dependencies]
win-specific-tool = ">=1.0"
```

### Working directory for tasks
```toml
[tasks]
test = { cmd = "pytest", cwd = "tests/" }
```

### Task arguments
```toml
[tasks.greet]
cmd = "echo Hello"
args = [{ arg = "name", default = "World" }]
```

## Workspace Registration
```bash
pixi workspace register --name <name> --path <name>   # Register
pixi workspace register list                            # List all
pixi workspace register remove <name>                   # Unregister
pixi workspace register prune                           # Clean stale entries
```
