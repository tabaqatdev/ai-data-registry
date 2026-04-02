---
description: Show current pixi environment info, installed packages, and shared tool versions
allowed-tools: Bash(pixi:*)
---
## Environment Info

!`pixi info`

## Installed Packages

!`pixi list`

## Shared Tool Versions

!`pixi run duckdb --version 2>/dev/null || echo "DuckDB not available"`
!`pixi run gdal --version 2>/dev/null || echo "GDAL not available"`
!`pixi run gpio --version 2>/dev/null || echo "gpio (geoparquet-io) not available — install with: pixi add --pypi geoparquet-io"`
!`pixi run python --version 2>/dev/null || echo "Python not available"`
!`pixi run node --version 2>/dev/null || echo "Node.js not available"`
!`pixi run pnpm --version 2>/dev/null || echo "pnpm not available"`

## Workspaces

!`ls workspaces/*/pixi.toml 2>/dev/null | sed 's|workspaces/||;s|/pixi.toml||' || echo "No workspaces found"`

## Dependency Tree

!`pixi tree`

Summarize the environment state: Python version, key package versions (GDAL, DuckDB, gpio), platform, discovered workspaces, and any version conflicts.
For deeper diagnostics, use the **env-check** skill.
