# Multi-Workspace Rules

This is a multi-workspace mono-repo. Each sub-workspace is a directory with its own `pixi.toml`, registered in the root workspace via `pixi workspace register`. All workspaces share a single `pixi.lock` at the root.

## Architecture

```
ai-data-registry/
├── pixi.toml              # Root workspace — shared tools (GDAL, DuckDB, pnpm, Python)
├── pixi.lock              # Single lock file for ALL workspaces
├── workspace-a/
│   └── pixi.toml          # Sub-workspace — own runtime, deps, tasks
├── workspace-b/
│   └── pixi.toml
```

## Creating a New Sub-Workspace

Use `/project:new-workspace <name> <language>` or manually:

```bash
# 1. Create and initialize (from project root)
mkdir <name>
cd <name>
pixi init . --channel conda-forge --platform osx-arm64 --platform linux-64 --platform win-64
cd ..

# 2. Register in root workspace
pixi workspace register --name <name> --path <name>

# 3. Add language runtime and dependencies (using -w flag from root)
pixi add -w <name> python                    # or go, nodejs, rust, etc.
pixi add -w <name> <workspace-specific-deps>

# 4. Verify registration
pixi workspace register list
```

## Separation of Concerns
- **Never** add workspace-specific deps to the root `pixi.toml` — each workspace owns its own
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

### Shared tools (from root)
```bash
pixi run duckdb -csv -c "SELECT 42"
pixi run gdal info input.gpkg
```

### Workspace tasks (from root, using -w flag)
```bash
pixi run -w <workspace> <task>
```

### Adding dependencies
```bash
# To a specific workspace (from root):
pixi add -w <workspace> <pkg>
pixi add -w <workspace> --pypi <pkg>

# To root (shared tools):
pixi add <pkg>
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
