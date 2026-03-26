---
name: attach-db
description: >
  Attach a DuckDB database for persistent querying. Use when the user has a .duckdb
  file they want to explore, or when they want to persist query results across sessions.
  Explores schema and wires up state.sql so all DuckDB skills can access it.
argument-hint: <path-to-database.duckdb>
allowed-tools: Bash
---

Database path: `$0`

## Step 1 — Resolve path

```bash
RESOLVED_PATH="$(cd "$(dirname "$0")" 2>/dev/null && pwd)/$(basename "$0")"
test -f "$RESOLVED_PATH" || echo "File not found — create new empty database?"
```

## Step 2 — Validate

```bash
pixi run duckdb "$RESOLVED_PATH" -c "PRAGMA version;"
```

## Step 3 — Explore schema

```bash
pixi run duckdb "$RESOLVED_PATH" -csv -c "
SELECT table_name, estimated_size FROM duckdb_tables() ORDER BY table_name;
"
# For each table (up to 20):
pixi run duckdb "$RESOLVED_PATH" -csv -c "DESCRIBE <table>; SELECT count() FROM <table>;"
```

## Step 4 — Resolve state directory

```bash
STATE_DIR=""
test -f .duckdb-skills/state.sql && STATE_DIR=".duckdb-skills"
PROJECT_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || echo "$PWD")"
PROJECT_ID="$(echo "$PROJECT_ROOT" | tr '/' '-')"
test -f "$HOME/.duckdb-skills/$PROJECT_ID/state.sql" && STATE_DIR="$HOME/.duckdb-skills/$PROJECT_ID"
```

If neither exists, ask user: project-local (`.duckdb-skills/`) or home (`~/.duckdb-skills/<project-id>/`).

## Step 5 — Append ATTACH

Derive alias from filename (e.g., `my_data.duckdb` -> `my_data`).
```bash
grep -q "ATTACH.*RESOLVED_PATH" "$STATE_DIR/state.sql" 2>/dev/null || \
  cat >> "$STATE_DIR/state.sql" <<'STATESQL'
ATTACH IF NOT EXISTS 'RESOLVED_PATH' AS my_data;
USE my_data;
STATESQL
```

## Step 6 — Verify and report

```bash
pixi run duckdb -init "$STATE_DIR/state.sql" -c "SHOW TABLES;"
```

Report: database path, alias, state file location, tables with column counts and row counts.

## Cross-references
- **duckdb-query** — run SQL against the attached database
- **duckdb-read-file** — explore data files to import
- **duckdb-docs** — look up DuckDB syntax
