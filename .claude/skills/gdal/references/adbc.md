# ADBC Driver for GDAL

Arrow Database Connectivity (GDAL 3.11+) for Arrow-native access to databases. Supports DuckDB, PostgreSQL, SQLite, BigQuery (read+write since 3.12), Snowflake, Flight SQL.

## Connection

```bash
# DuckDB database (if libduckdb is available on the system)
pixi run gdal info my.duckdb

# Parquet via DuckDB ADBC
pixi run gdal info "ADBC:file.parquet" --oo ADBC_DRIVER=libduckdb

# PostgreSQL
pixi run gdal info "ADBC:postgresql://user:pass@host/db" --oo ADBC_DRIVER=adbc_driver_postgresql

# BigQuery
pixi run gdal info "ADBC:" --oo ADBC_DRIVER=adbc_driver_bigquery --oo ADBC_OPTION_uri="bigquery://project_id"

# With SQL query
pixi run gdal info "ADBC:my.duckdb" --oo SQL="SELECT * FROM my_table WHERE pop > 1000"

# DuckDB with spatial extension
pixi run gdal info "ADBC:my.duckdb" \
  --oo PRELUDE_STATEMENTS="INSTALL spatial" \
  --oo PRELUDE_STATEMENTS="LOAD spatial"
```

## Open options

| Option | Purpose |
|--------|---------|
| `ADBC_DRIVER` | Driver name: `adbc_driver_sqlite`, `adbc_driver_postgresql`, `adbc_driver_bigquery`, `adbc_driver_snowflake`, or path to DuckDB shared lib |
| `SQL` | SQL query to create a result layer |
| `ADBC_OPTION_xxx` | Custom ADBC option passed to `AdbcDatabaseSetOption()` (driver-specific) |
| `PRELUDE_STATEMENTS` | SQL statements to run before discovering layers (repeatable) |

## Special layer: table_list

For PostgreSQL, SQLite, DuckDB, and Parquet, layers are auto-discovered. For other databases, query the `table_list` layer to find available tables:

```bash
pixi run gdal info "ADBC:my.duckdb" --layer table_list
```

Returns: `catalog_name`, `schema_name`, `table_name`, `table_type`.

## BigQuery (GDAL 3.12+)

BigQuery support is read+write via `adbc_driver_bigquery`. Requires the BigQuery ADBC driver shared library and Google credentials.

### Authentication

Use `gcloud auth application-default login` to generate credentials, then reference the file:

```bash
CRED="$HOME/.config/gcloud/application_default_credentials.json"
```

### Read

```bash
# List tables in a dataset
pixi run gdal vector info ADBC: \
  --oo BIGQUERY_PROJECT_ID=my_project \
  --oo BIGQUERY_DATASET_ID=my_dataset \
  --oo BIGQUERY_JSON_CREDENTIAL_FILE=$CRED

# SQL query
pixi run gdal vector info ADBC: --features \
  --oo BIGQUERY_PROJECT_ID=my_project \
  --oo BIGQUERY_JSON_CREDENTIAL_FILE=$CRED \
  --oo "SQL=SELECT * FROM my_dataset.my_table ORDER BY area DESC LIMIT 10"

# Export to GeoPackage
pixi run gdal vector sql --input=ADBC: --output=out.gpkg \
  "--sql=SELECT * FROM my_dataset.my_table WHERE country = 'France'" \
  --oo BIGQUERY_PROJECT_ID=my_project \
  --oo BIGQUERY_JSON_CREDENTIAL_FILE=$CRED
```

### Write

```bash
# GeoPackage -> BigQuery (input must use geographic coordinates)
pixi run gdal vector convert --update --input=my.gpkg --output=ADBC: \
  --output-oo BIGQUERY_PROJECT_ID=my_project \
  --output-oo BIGQUERY_DATASET_ID=my_dataset \
  --output-oo BIGQUERY_JSON_CREDENTIAL_FILE=$CRED

# Reproject + rename layer + write to BigQuery
pixi run gdal vector pipeline read my.gpkg ! \
  reproject --dst-crs=EPSG:4326 ! \
  write ADBC: --update \
    --output-oo BIGQUERY_PROJECT_ID=my_project \
    --output-oo BIGQUERY_DATASET_ID=my_dataset \
    --output-oo BIGQUERY_JSON_CREDENTIAL_FILE=$CRED \
    --output-layer newlayer
```

### BigQuery open options

| Option | Required | Purpose |
|--------|----------|---------|
| `BIGQUERY_PROJECT_ID` | Yes | Google project ID |
| `BIGQUERY_DATASET_ID` | Yes (unless SQL) | BigQuery dataset ID |
| `BIGQUERY_JSON_CREDENTIAL_FILE` | One of these | Path to credentials JSON file |
| `BIGQUERY_JSON_CREDENTIAL_STRING` | One of these | Inline credentials JSON string |

### BigQuery gotchas

- **Feature insertion is one-at-a-time**. Use BigQuery bulk import for large datasets.
- **Write requires geographic coordinates** (EPSG:4326). Reproject before writing if needed.
- **FID column** defaults to `ogc_fid`. Set `--lco FID=` (empty) to skip FID creation.
- **No explicit ADBC_DRIVER needed** if any `BIGQUERY_*` option is set.

## Gotchas

- **Read-only** for most drivers (BigQuery is the exception with write support).
- **Spatial support** only when ADBC driver is DuckDB, for native spatial databases and GeoParquet datasets with spatial extension.
- **Priority**: Parquet, SQLite, GPKG drivers are registered before ADBC. Use `ADBC:` prefix or `-if ADBC` to force.
- **DuckDB ADBC** requires `libduckdb` shared library on the system path (not guaranteed in pixi env). For most workflows, use DuckDB directly via `pixi run duckdb` instead.
- **Spatial filtering** is pushed to DuckDB engine when available (uses GeoParquet bbox columns and RTree indices).

## Cross-references

- [parquet.md](parquet.md) - GeoParquet/Parquet driver
- [arrow.md](arrow.md) - Arrow IPC/Feather driver
- **duckdb** skill - Direct DuckDB access via `pixi run duckdb` (preferred over ADBC for most workflows)
