# Tool Execution Rules

All CLI tools MUST be run through pixi to ensure the correct versions and environment.

## Shared Tools (from root pixi.toml)

| Tool | Command | What it does |
|------|---------|------|
| DuckDB | `pixi run duckdb` | SQL engine, spatial, Parquet |
| GDAL | `pixi run gdal` | Vector/raster I/O (new unified CLI) |
| gpio | `pixi run gpio` | GeoParquet optimization/validation |
| Python | `pixi run python` | Python runtime |
| Node.js | `pixi run node` | Node.js runtime |
| pnpm | `pixi run pnpm` | Node package manager (NEVER npm/yarn) |

## Running from Root vs Workspace

### From project root (shared tools)
```bash
pixi run duckdb -csv -c "SELECT 42"
pixi run gdal info input.gpkg
pixi run gpio inspect summary input.parquet
```

### From inside a workspace
```bash
cd <workspace>/
pixi run <workspace-task>          # Uses workspace pixi.toml
pixi run python script.py          # Uses workspace Python (if declared)
```

### From root targeting a specific workspace
```bash
pixi run --manifest-path <workspace>/pixi.toml <task>
```

### Using root tools from inside a workspace
If a workspace doesn't declare its own DuckDB/GDAL/gpio, use the root:
```bash
pixi run --manifest-path ./pixi.toml duckdb -csv -c "SELECT 42"
pixi run --manifest-path ./pixi.toml gdal info input.gpkg
```

## Never Run Directly
- Do NOT run `duckdb` directly — always `pixi run duckdb`
- Do NOT run `gdal` directly — always `pixi run gdal`
- Do NOT run `gpio` directly — always `pixi run gpio`
- Do NOT run `npm` — always `pixi run pnpm`
- Do NOT run `python` directly — always `pixi run python`
- This ensures version consistency across platforms and workspaces
