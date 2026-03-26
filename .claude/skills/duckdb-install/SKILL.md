---
name: install-duckdb
description: >
  Install or update DuckDB extensions. Use when DuckDB reports a missing extension,
  when the user asks to install/update extensions, or when other skills delegate here.
  Each argument is a plain extension name or name@repo. Pass --update to update.
argument-hint: "[--update] [ext1 ext2@repo ...]"
allowed-tools: Bash
---

Arguments: `$@`

Parse each extension as `name` → `INSTALL name;` or `name@repo` → `INSTALL name FROM repo;`.

## Step 1 — Locate DuckDB

```bash
pixi run duckdb --version
```

If pixi fails, fall back to system `duckdb --version`. If neither works, tell the user to install via `pixi add duckdb` or see https://duckdb.org/docs/installation.

## Step 2 — Install or update

**Install mode** (no `--update` flag):

```bash
pixi run duckdb :memory: -c "INSTALL ext1; INSTALL ext2 FROM repo2; ..."
```

**Update mode** (`--update` in `$@`):

```bash
# Check CLI version
pixi run duckdb --version
# Update extensions
pixi run duckdb :memory: -c "UPDATE EXTENSIONS;"
# Or specific: UPDATE EXTENSIONS (ext1, ext2);
```

Report success or failure. Common extensions for this project: `spatial`, `httpfs`, `fts`, `parquet`.
