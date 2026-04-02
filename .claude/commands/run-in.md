---
description: Run a pixi task in a specific workspace
argument-hint: <workspace> <task>
allowed-tools: Bash(pixi:*)
---
Parse the workspace name and task name from: `$ARGUMENTS`

Run the task in the specified workspace:

```bash
pixi run --manifest-path workspaces/$0/pixi.toml $1
```

If the task fails, troubleshoot:
1. Are dependencies installed? `pixi install --manifest-path workspaces/$0/pixi.toml`
2. Does the task exist? Check the workspace's `pixi.toml` `[tasks]` section
3. Is this a shared tool instead? Root-level tools run without `--manifest-path`: `pixi run duckdb`, `pixi run gdal`, etc.
