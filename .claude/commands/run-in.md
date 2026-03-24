---
description: Run a pixi task in a specific workspace
argument-hint: [workspace-name] [task-name]
---
Parse the workspace name and task name from: `$ARGUMENTS`

Run the task in the specified workspace using the `-w` flag:
```
pixi run -w <workspace> <task>
```

If the task fails, check:
1. Is the workspace registered? (`pixi workspace register list`)
2. Are dependencies installed? (`pixi install`)
3. Does the task exist? (`pixi run -w <workspace> pixi task list`)

For root-level shared tools, run from the project root instead:
- `pixi run duckdb ...`
- `pixi run gdal ...`
