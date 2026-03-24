---
description: Run a pixi task in a specific workspace
argument-hint: [workspace-name] [task-name]
---
Parse the workspace name and task name from: `$ARGUMENTS`

Run the task in the specified workspace using:
```
pixi run --manifest-path <workspace>/pixi.toml <task>
```

If the task fails, check:
1. Does the workspace exist? (`ls <workspace>/pixi.toml`)
2. Are dependencies installed? (`cd <workspace> && pixi install`)
3. Does the task exist? (`cd <workspace> && pixi task list`)

For root-level shared tools, run from the project root instead:
- `pixi run duckdb ...`
- `pixi run gdal ...`
