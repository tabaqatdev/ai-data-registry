---
description: Add a dependency to a workspace's pixi.toml
argument-hint: [package-name] [--pypi] [-w workspace]
---
Add the package `$ARGUMENTS` using pixi.

Before adding, determine the target workspace:
- If `-w <name>` is specified, use that workspace
- If at the project root, ask whether this is a shared tool (root) or workspace-specific dependency
- If inside a workspace directory, target that workspace

Steps:
1. Determine target workspace
2. Search for the package: `pixi search <pkg>`
3. If found on conda-forge: `pixi add -w <workspace> <pkg>`
4. If only on PyPI: `pixi add -w <workspace> --pypi <pkg>`
5. For root shared tools: `pixi add <pkg>` (no -w flag)
6. Show the updated pixi.toml dependencies section
