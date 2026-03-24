---
name: duckdb-state
description: >
  Initializes and manages shared DuckDB session state — maintains a single
  state.sql file with pre-loaded extensions, credentials, and macros so all
  DuckDB skills share a consistent environment.
allowed-tools:
  - Bash
---

# DuckDB State Management Skill

This skill is the **single source of truth** for DuckDB session initialization
in the ai-data-registry project. All other `duckdb-*` skills delegate state
setup here.

---

## State Directory

The state directory is `.duckdb-skills/` at the project root. All state files
live here:

```
.duckdb-skills/
  state.sql      # Initialization script sourced by every DuckDB session
  .lock          # flock lock file for atomic updates
```

### Resolution

```bash
STATE_DIR="${PROJECT_ROOT:-.}/.duckdb-skills"
STATE_FILE="$STATE_DIR/state.sql"
```

---

## Initialization

Create the state directory and seed `state.sql` with core extensions:

```bash
STATE_DIR=".duckdb-skills"
STATE_FILE="$STATE_DIR/state.sql"

mkdir -p "$STATE_DIR"

cat > "$STATE_FILE" << 'SQL'
-- =============================================================
-- DuckDB session state — managed by duckdb-state skill
-- Do not edit manually; use the skill to add entries.
-- =============================================================

-- Core extensions
INSTALL spatial;  LOAD spatial;
INSTALL httpfs;   LOAD httpfs;
INSTALL fts;      LOAD fts;
-- parquet is auto-loaded in modern DuckDB, but be explicit
INSTALL parquet;  LOAD parquet;

-- read_any macro (from duckdb-read-file)
-- Provides a universal reader that auto-detects file format.
CREATE OR REPLACE MACRO read_any(path) AS TABLE
  SELECT * FROM query_table(
    CASE
      WHEN path LIKE '%.parquet' OR path LIKE '%.geoparquet'
        THEN format('read_parquet(''{}'')', path)
      WHEN path LIKE '%.csv' OR path LIKE '%.tsv'
        THEN format('read_csv(''{}'')', path)
      WHEN path LIKE '%.json' OR path LIKE '%.geojson' OR path LIKE '%.ndjson'
        THEN format('read_json_auto(''{}'')', path)
      ELSE format('read_parquet(''{}'')', path)
    END
  );
SQL

echo "state.sql initialized at $STATE_FILE"
```

---

## Using state.sql in a DuckDB Session

Every DuckDB invocation should source state.sql first:

```bash
pixi run duckdb -init "$STATE_DIR/state.sql" -c "<your query>"
```

Or in a script:

```bash
pixi run duckdb << SQL
.read $STATE_DIR/state.sql
SELECT * FROM read_parquet('data.parquet') LIMIT 10;
SQL
```

---

## Atomic State Updates (File Locking)

When appending configuration to state.sql (credentials, extra extensions,
ATTACHes), use `flock` to prevent concurrent writes from corrupting the file:

```bash
STATE_DIR=".duckdb-skills"

(
  flock 9
  if ! grep -q "LOAD json;" "$STATE_DIR/state.sql"; then
    cat >> "$STATE_DIR/state.sql" << 'SQL'

-- JSON extension (added by duckdb-state)
INSTALL json; LOAD json;
SQL
  fi
) 9>"$STATE_DIR/.lock"
```

### Pattern: Add a credential block

```bash
STATE_DIR=".duckdb-skills"

(
  flock 9
  if ! grep -q "SET s3_region" "$STATE_DIR/state.sql"; then
    cat >> "$STATE_DIR/state.sql" << 'SQL'

-- S3 credentials (added by duckdb-state)
SET s3_region = 'us-east-1';
SET s3_access_key_id = getenv('AWS_ACCESS_KEY_ID');
SET s3_secret_access_key = getenv('AWS_SECRET_ACCESS_KEY');
SQL
  fi
) 9>"$STATE_DIR/.lock"
```

### Pattern: Add GCS credentials

```bash
STATE_DIR=".duckdb-skills"

(
  flock 9
  if ! grep -q "SET gcs_" "$STATE_DIR/state.sql"; then
    cat >> "$STATE_DIR/state.sql" << 'SQL'

-- GCS credentials (added by duckdb-state)
SET gcs_access_key_id = getenv('GCS_ACCESS_KEY_ID');
SET gcs_secret = getenv('GCS_SECRET');
SQL
  fi
) 9>"$STATE_DIR/.lock"
```

### Pattern: Add Azure credentials

```bash
STATE_DIR=".duckdb-skills"

(
  flock 9
  if ! grep -q "LOAD azure;" "$STATE_DIR/state.sql"; then
    cat >> "$STATE_DIR/state.sql" << 'SQL'

-- Azure credentials (added by duckdb-state)
INSTALL azure; LOAD azure;
CREATE SECRET azure_secret (
  TYPE AZURE,
  CONNECTION_STRING getenv('AZURE_STORAGE_CONNECTION_STRING')
);
SQL
  fi
) 9>"$STATE_DIR/.lock"
```

---

## State Validation

After initialization or updates, validate that state.sql is healthy:

```bash
STATE_DIR=".duckdb-skills"

echo "Validating state.sql..."
if pixi run duckdb -init "$STATE_DIR/state.sql" -c "SELECT 'state_ok';" 2>&1 | grep -q "state_ok"; then
  echo "state.sql is valid."
else
  echo "ERROR: state.sql failed validation. Review errors above."
  exit 1
fi
```

This catches:
- Extensions that fail to INSTALL or LOAD
- Syntax errors in appended blocks
- ATTACH statements pointing to missing databases
- Credential setups that error out (missing env vars produce warnings, not
  errors, so these are generally safe)

---

## State Reset

If state.sql becomes corrupt or needs a clean slate:

```bash
STATE_DIR=".duckdb-skills"
rm -f "$STATE_DIR/state.sql" "$STATE_DIR/.lock"
# Then re-run initialization (see above)
```

---

## Cross-references

This skill is the foundation for all DuckDB skills:

- **duckdb-query** — sources state.sql before running queries
- **duckdb-install** — may call duckdb-state to add extensions
- **duckdb-read-file** — the `read_any` macro is initialized here
- **duckdb-cloud** — cloud credentials are appended via this skill
- **duckdb-export** — sources state.sql for COPY operations
