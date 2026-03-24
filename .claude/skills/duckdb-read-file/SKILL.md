---
name: read-file
description: >
  Read and explore any data file (CSV, JSON, Parquet, Avro, Excel, spatial, …)
  locally or remotely (S3, HTTPS). Resolves the path automatically. Uses DuckDB
  with extension-based format detection — no magic extension needed.
argument-hint: <filename or URL> [question about the data]
allowed-tools: Bash
---

You are helping the user read and analyze a data file using DuckDB.

Filename given: `$0`
Question: `${1:-describe the data}`

Follow these steps in order, stopping and reporting clearly if any step fails.

## Step 1 — Classify and resolve the path

Determine whether the input is **local** or **remote**:

- **S3 URI** (`s3://...`, `s3a://...`, `s3n://...`) → remote, needs S3 secret + httpfs
- **HTTPS/HTTP URL** (`https://...`, `http://...`) → remote, needs httpfs
- **GCS URI** (`gs://...`, `gcs://...`) → remote, needs GCS secret + httpfs
- **Azure URI** (`azure://...`, `az://...`, `abfss://...`) → remote, needs Azure secret + httpfs
- **Otherwise** → local file

### Local files

```bash
# Cross-platform file search using pixi run python
pixi run python -c "
import pathlib, sys
matches = [p for p in pathlib.Path('.').rglob('$0') if '.git' not in p.parts]
for m in matches:
    print(m.resolve())
"
```

- **Zero results** → tell the user the file was not found and stop.
- **More than one result** → list all matches, ask the user to re-run with a fuller path, and stop.
- **Exactly one result** → use that full path (`RESOLVED_PATH`).

### Remote files

Use the URI/URL as-is for `RESOLVED_PATH`. Skip the find step.

## Step 2 — Resolve the state directory and set up remote access (if needed)

Look for an existing state file:

```bash
STATE_DIR=""
test -f .duckdb-skills/state.sql && STATE_DIR=".duckdb-skills"
PROJECT_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || echo "$PWD")"
PROJECT_ID="$(echo "$PROJECT_ROOT" | tr '/' '-')"
test -f "$HOME/.duckdb-skills/$PROJECT_ID/state.sql" && STATE_DIR="$HOME/.duckdb-skills/$PROJECT_ID"
```

If the file is **local** and `STATE_DIR` is set, skip to Step 3. If the file is **local** and no state exists, skip to Step 3 (no state needed for local reads).

If the file is **remote**, state is needed for secrets/extensions. If `STATE_DIR` is empty, ask the user where to store state (same options as attach-db):

> 1. **In the project directory** (`.duckdb-skills/`) — optionally gitignored
> 2. **In your home directory** (`~/.duckdb-skills/<project-id>/`)

Create the chosen directory, then set up access and **persist it to state.sql**:

### S3

```bash
duckdb :memory: -c "INSTALL httpfs;"
grep -q "credential_chain" "$STATE_DIR/state.sql" 2>/dev/null || cat >> "$STATE_DIR/state.sql" <<'SQL'
LOAD httpfs;
CREATE SECRET IF NOT EXISTS __default_s3 (TYPE S3, PROVIDER credential_chain);
SQL
```

### GCS

DuckDB's built-in GCS support uses the S3 API, which requires HMAC keys. Explain this to the user and offer two options:

**Option A — HMAC keys** (built-in, no extra extension):

```bash
duckdb :memory: -c "INSTALL httpfs;"
grep -q "__default_gcs" "$STATE_DIR/state.sql" 2>/dev/null || cat >> "$STATE_DIR/state.sql" <<SQL
LOAD httpfs;
CREATE SECRET IF NOT EXISTS __default_gcs (TYPE GCS, PROVIDER credential_chain);
SQL
```

**Option B — Native GCP credentials** via duckdb-gcs community extension:

```bash
duckdb :memory: -c "INSTALL gcs FROM community;"
grep -q "LOAD gcs;" "$STATE_DIR/state.sql" 2>/dev/null || cat >> "$STATE_DIR/state.sql" <<'SQL'
LOAD gcs;
CREATE SECRET IF NOT EXISTS __default_gcp (TYPE GCP, PROVIDER credential_chain);
SQL
```

### Azure

```bash
duckdb :memory: -c "INSTALL httpfs; INSTALL azure;"
grep -q "__default_azure" "$STATE_DIR/state.sql" 2>/dev/null || cat >> "$STATE_DIR/state.sql" <<SQL
LOAD httpfs;
LOAD azure;
CREATE SECRET IF NOT EXISTS __default_azure (TYPE AZURE, PROVIDER credential_chain, ACCOUNT_NAME '${ACCOUNT_NAME}');
SQL
```

### HTTPS

```bash
duckdb :memory: -c "INSTALL httpfs;"
grep -q "LOAD httpfs;" "$STATE_DIR/state.sql" 2>/dev/null || echo "LOAD httpfs;" >> "$STATE_DIR/state.sql"
```

## Step 3 — Ensure the `read_any` macro is in state.sql

If `STATE_DIR` is empty (local file, no state created yet), create one now. Ask the user the same location question as above.

Check if `state.sql` already defines the macro:

```bash
grep -q "read_any" "$STATE_DIR/state.sql" 2>/dev/null
```

If not, append it:

```bash
cat >> "$STATE_DIR/state.sql" <<'SQL'
-- read_any: auto-detect file format by extension and dispatch to the right reader
CREATE OR REPLACE MACRO read_any(file_name) AS TABLE
  WITH json_case AS (FROM read_json_auto(file_name))
     , csv_case AS (FROM read_csv(file_name))
     , parquet_case AS (FROM read_parquet(file_name))
     , avro_case AS (FROM read_avro(file_name))
     , blob_case AS (FROM read_blob(file_name))
     , spatial_case AS (FROM st_read(file_name))
     , excel_case AS (FROM read_xlsx(file_name))
     , sqlite_case AS (FROM sqlite_scan(file_name, (SELECT name FROM sqlite_master(file_name) LIMIT 1)))
     , ipynb_case AS (
         WITH nb AS (FROM read_json_auto(file_name))
         SELECT cell_idx, cell.cell_type,
                array_to_string(cell.source, '') AS source,
                cell.execution_count
         FROM nb, UNNEST(cells) WITH ORDINALITY AS t(cell, cell_idx)
         ORDER BY cell_idx
     )
  FROM query_table(
    CASE
      WHEN file_name ILIKE '%.json' OR file_name ILIKE '%.jsonl' OR file_name ILIKE '%.ndjson' OR file_name ILIKE '%.geojson' OR file_name ILIKE '%.geojsonl' OR file_name ILIKE '%.har' THEN 'json_case'
      WHEN file_name ILIKE '%.csv' OR file_name ILIKE '%.tsv' OR file_name ILIKE '%.tab' OR file_name ILIKE '%.txt' THEN 'csv_case'
      WHEN file_name ILIKE '%.parquet' OR file_name ILIKE '%.pq' THEN 'parquet_case'
      WHEN file_name ILIKE '%.avro' THEN 'avro_case'
      WHEN file_name ILIKE '%.xlsx' OR file_name ILIKE '%.xls' THEN 'excel_case'
      WHEN file_name ILIKE '%.shp' OR file_name ILIKE '%.gpkg' OR file_name ILIKE '%.fgb' OR file_name ILIKE '%.kml' THEN 'spatial_case'
      WHEN file_name ILIKE '%.ipynb' THEN 'ipynb_case'
      WHEN file_name ILIKE '%.db' OR file_name ILIKE '%.sqlite' OR file_name ILIKE '%.sqlite3' THEN 'sqlite_case'
      ELSE 'blob_case'
    END
  );
SQL
```

Some readers require core extensions. If the read fails with a missing extension error, install it and add the LOAD to `state.sql`:

| Reader | Core extension needed |
|---|---|
| `st_read`, `read_xlsx` | `spatial` |
| `sqlite_scan` | `sqlite_scanner` |
| `read_avro` | built-in (v1.3+) |
| All others | built-in |

```bash
pixi run duckdb :memory: -c "INSTALL <extension>;"
# Cross-platform: prepend LOAD to state.sql via Python
pixi run python -c "
import pathlib
state = pathlib.Path('$STATE_DIR/state.sql')
content = state.read_text()
if 'LOAD <extension>;' not in content:
    state.write_text('LOAD <extension>;\n' + content)
"
```

## Step 4 — Read the file

**Remote files** (use state.sql for secrets/extensions + macro):

```bash
duckdb -init "$STATE_DIR/state.sql" -csv -c "
SELECT column_name FROM (DESCRIBE FROM read_any('RESOLVED_PATH'));
SELECT count(*) AS row_count FROM read_any('RESOLVED_PATH');
FROM read_any('RESOLVED_PATH') LIMIT 10;
"
```

**Local files** (sandboxed):

```bash
duckdb -init "$STATE_DIR/state.sql" -csv -c "
SET allowed_paths=['RESOLVED_PATH'];
SET enable_external_access=false;
SET allow_persistent_secrets=false;
SET lock_configuration=true;
SELECT column_name FROM (DESCRIBE FROM read_any('RESOLVED_PATH'));
SELECT count(*) AS row_count FROM read_any('RESOLVED_PATH');
FROM read_any('RESOLVED_PATH') LIMIT 10;
"
```

**If this succeeds** → skip to Step 5 (Answer).

**If this fails** → diagnose the cause:
- **`duckdb: command not found`** → invoke install-duckdb and retry.
- **Access denied / credentials error** → ask the user to verify credentials.
- **Missing extension** → install it, add `LOAD` to `state.sql`, and retry.
- **Wrong reader / parse error** → run manually with the correct `read_*` function.
- **Persistent or unclear DuckDB error** → use duckdb-docs to search documentation.

Notes:
- **Spatial files**: `st_read` globs for sidecar files. Add a stem-wildcard to `allowed_paths`:
  `SET allowed_paths=['RESOLVED_PATH', 'RESOLVED_PATH_WITHOUT_EXTENSION.*']`

## Step 5 — Answer the question

Using the schema, row count, and sample rows gathered above, answer:

`${1:-describe the data: summarize column types, row count, and any notable patterns.}`

## Step 6 — Suggest next steps

After answering, if the data looks like something the user might want to explore further:

> *If you want to keep querying this data, you can use the query skill. It supports SQL and natural language questions.*

If the file is large:

> *To attach this as a database for repeated queries, run the attach-db skill.*

Keep these suggestions brief and only show them once.

## Cross-references
- Use the **duckdb-state** skill to initialize session state before reading (extensions, credentials)
- Use the **duckdb-query** skill for follow-up SQL queries on the data
- Use the **duckdb-attach-db** skill to persist large files as a database for repeated queries
- Use the **duckdb-docs** skill when encountering DuckDB errors or unknown functions
- Use the **duckdb-install** skill to add missing extensions (spatial, httpfs, etc.)
- Use the **geoparquet** skill for GeoParquet-specific inspection (`gpio inspect`) and validation (`gpio check all`)
- Use the **gdal** skill to convert spatial formats (Shapefile, GeoPackage, etc.) before reading
- Use the **spatial-analysis** skill for geospatial analysis workflows on the data
- Use the **data-quality** agent for deep validation (nulls, duplicates, geometry, CRS)
- Use the **data-explorer** agent for comprehensive dataset profiling
