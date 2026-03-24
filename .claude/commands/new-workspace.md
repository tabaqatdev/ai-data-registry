---
description: Create a new pixi sub-workspace with its own isolated environment
argument-hint: [workspace-name] [language: python|go|node|rust]
---
Create a new sub-workspace named `$ARGUMENTS` in this mono-repo.

Follow the workspace rules in `.claude/rules/workspaces.md`.

## Steps

1. **Parse arguments** — extract workspace name and language from `$ARGUMENTS`

2. **Create and initialize the workspace**
```bash
mkdir -p <name>
cd <name>
pixi init . --channel conda-forge --platform osx-arm64 --platform linux-64 --platform win-64
```

3. **Add the language runtime**
   - python: `pixi add python`
   - go: `pixi add go`
   - node: `pixi add nodejs`
   - rust: `pixi add rust`

4. **Add workspace-specific dependencies** (ask the user what they need)

5. **Set up basic tasks in pixi.toml**

For Python workspaces:
```toml
[tasks]
dev = "python main.py"
test = { cmd = "pytest", cwd = "tests/" }
lint = "ruff check ."
```

For Go workspaces:
```toml
[tasks]
build = "go build -o bin/app ."
run = { cmd = "./bin/app", depends-on = ["build"] }
test = "go test ./..."
```

For Node workspaces:
```toml
[tasks]
dev = "pnpm run dev"
build = "pnpm run build"
test = "pnpm run test"
```

6. **Add platform-specific deps if needed**
```toml
[target.unix.dependencies]
# Unix-only deps here

[target.win-64.dependencies]
# Windows-only deps here
```

7. **Register the workspace in the root** (from project root)
```bash
cd ..
pixi workspace register --name <name> --path <name>
pixi workspace register list  # Verify
```

8. **Add .gitignore for pixi environments** (if not inherited from root)
```bash
pixi run python -c "
import pathlib
pathlib.Path('<name>/.gitignore').write_text('# pixi environments\n.pixi/*\n!.pixi/config.toml\n')
"
```

9. **Show the final pixi.toml for review**

## Notes
- Shared tools (DuckDB, GDAL, gpio, pnpm) are available from the root — no need to add them per workspace unless a specific version is needed
- Run workspace tasks: `cd <name> && pixi run <task>` or `pixi run --manifest-path <name>/pixi.toml <task>`
- Use `env = { KEY = "$CONDA_PREFIX/..." }` in tasks to reference the environment prefix
- Use `depends-on` to chain tasks within the workspace
- Use `[target.<platform>.dependencies]` for platform-specific deps
