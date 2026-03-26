---
description: Run a pixi task in a specific workspace
argument-hint: <workspace> <task>
allowed-tools: Bash(pixi:*)
---
Parse the workspace name and task name from: `$ARGUMENTS`

Run the task in the specified workspace:

```bash
pixi run -w $0 $1
```

If the task fails, troubleshoot:
1. Is the workspace registered? `pixi workspace register list`
2. Are dependencies installed? `pixi install`
3. Does the task exist? Check the workspace's `pixi.toml` `[tasks]` section
4. Is this a shared tool instead? Root-level tools run without `-w`: `pixi run duckdb`, `pixi run gdal`, etc.
