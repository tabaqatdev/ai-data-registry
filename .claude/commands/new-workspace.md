---
description: Create a new pixi sub-workspace with its own isolated environment
argument-hint: [workspace-name] [language: python|go|node|rust]
---
Create a new sub-workspace named `$ARGUMENTS` in this mono-repo.

Follow the workspace rules in `.claude/rules/workspaces.md`.

## Steps

1. **Parse arguments** — extract workspace name and language from `$ARGUMENTS`

2. **Create and initialize the workspace** (from project root)
```bash
mkdir <name>
cd <name>
pixi init . --channel conda-forge --platform osx-arm64 --platform linux-64 --platform win-64
cd ..
```

3. **Register in root workspace**
```bash
pixi workspace register --name <name> --path <name>
```

4. **Add the language runtime** (using -w flag from root)
   - python: `pixi add -w <name> python`
   - go: `pixi add -w <name> go`
   - node: `pixi add -w <name> nodejs`
   - rust: `pixi add -w <name> rust`

5. **Add workspace-specific dependencies** (ask the user what they need)
```bash
pixi add -w <name> <dep1> <dep2>
pixi add -w <name> --pypi <pypi-dep>
```

6. **Set up basic tasks in the workspace pixi.toml**

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

7. **Add platform-specific deps if needed**
```toml
[target.unix.dependencies]
# Unix-only deps here

[target.win-64.dependencies]
# Windows-only deps here
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
- Run workspace tasks from root: `pixi run -w <name> <task>`
- Use `env = { KEY = "$CONDA_PREFIX/..." }` in tasks to reference the environment prefix
- Use `depends-on` to chain tasks within the workspace
- Use `[target.<platform>.dependencies]` for platform-specific deps
- Single `pixi.lock` at root covers all workspaces — do NOT expect per-workspace lock files
