---
description: Add a dependency to a workspace or root pixi.toml (conda-forge preferred, PyPI fallback)
argument-hint: <package> [--pypi] [-w workspace]
allowed-tools: Bash(pixi:*), Read
---
Add the package `$ARGUMENTS` using pixi.

## Determine target

- If `-w <name>` is specified in arguments, use that workspace
- If at the project root and no `-w` flag, ask whether this is a shared tool (root) or workspace-specific
- If inside a workspace directory, target that workspace automatically

## Decide package source: conda vs PyPI

**Always prefer conda-forge. Fall back to PyPI only when not available on conda-forge.**

1. Search conda-forge first: `pixi search <pkg>`
2. If found → `pixi add [-w <workspace>] <pkg>` (goes in `[dependencies]`)
3. If NOT found on conda-forge, or `--pypi` flag was passed → `pixi add [-w <workspace>] --pypi <pkg>` (goes in `[pypi-dependencies]`)
4. For root shared tools: `pixi add <pkg>` (no `-w` flag)

Never add the same package from both conda and PyPI sources.

## After adding

Show the updated dependencies section from the target `pixi.toml`.
