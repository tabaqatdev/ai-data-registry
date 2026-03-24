---
description: Add a dependency to the current workspace's pixi.toml
argument-hint: [package-name] [--pypi]
---
Add the package `$ARGUMENTS` to this workspace using pixi.

Before adding, confirm which workspace we're in by checking for `pixi.toml` in the current directory.
If at the project root, ask whether this is a shared tool (root) or workspace-specific dependency.

Steps:
1. Check current directory for `pixi.toml`
2. Search for the package: `pixi search $ARGUMENTS`
3. If found on conda-forge, add with: `pixi add $ARGUMENTS`
4. If only on PyPI, add with: `pixi add --pypi $ARGUMENTS`
5. Verify it installed correctly: `pixi list | grep $ARGUMENTS`
6. Show the updated pixi.toml dependencies section
