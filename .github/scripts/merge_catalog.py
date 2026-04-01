# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "duckdb>=1.5.1",
# ]
# ///
"""Merge workspace catalogs into the global DuckLake catalog.

Usage:
    uv run merge_catalog.py --workspace <name> [--catalog-dir <path>] [--storage <name>]

For each table declared in the workspace pixi.toml:
  1. Scans S3 for Parquet files under s3://bucket/{owner}/{repo}/{branch}/{schema}/{table}/
  2. Registers any files not yet in the workspace catalog
  3. Diffs the workspace catalog against the global catalog
  4. Registers only NEW files in the global catalog (zero-copy)

Supports multi-storage: runs merge independently for each target storage.
Pass --storage to merge a specific storage, or omit to merge all workspace storages.

Runs with concurrency: 1 to prevent concurrent writes to the global catalog.

CRITICAL: Catalog files use the DuckDB backend (.duckdb), NOT SQLite (.ducklake).
DuckDB catalogs support remote S3/HTTPS read-only access via httpfs, enabling
direct querying without downloading. SQLite catalogs do NOT support remote access
(blocked by duckdb/ducklake#912).
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.registry_config import (
    WORKSPACE_NAME_RE,
    WORKSPACES_DIR,
    build_catalog_path,
    build_global_catalog_path,
    build_s3_root,
    get_tables,
    get_workspace_storages,
    load_storage_configs,
    parse_workspace_registry,
    quote_ident,
    quote_literal,
    resolve_storage_env,
    s5cmd_for_storage,
)


def download_catalog(storage_name: str, s3_path: str, local_path: str) -> bool:
    """Download a catalog file from S3. Returns True on success."""
    result = s5cmd_for_storage(storage_name, "cp", s3_path, local_path)
    return result.returncode == 0


def upload_catalog(storage_name: str, local_path: str, s3_path: str) -> bool:
    """Upload a catalog file to S3. Returns True on success."""
    result = s5cmd_for_storage(storage_name, "cp", local_path, s3_path)
    return result.returncode == 0


def create_s3_secret(con, storage_name: str):
    """Configure S3 credentials via CREATE SECRET for DuckLake operations."""
    creds = resolve_storage_env(storage_name)
    endpoint = creds["endpoint_url"]
    if not endpoint:
        return
    parsed = urlparse(endpoint)
    s3_host = parsed.hostname or endpoint.replace("https://", "").replace("http://", "")
    access_key = creds["access_key"] or ""
    secret_key = creds["secret_key"] or ""
    region = creds["region"] or "auto"
    con.execute(f"""
        CREATE OR REPLACE SECRET registry_s3 (
            TYPE S3,
            KEY_ID {quote_literal(access_key)},
            SECRET {quote_literal(secret_key)},
            ENDPOINT {quote_literal(s3_host)},
            URL_STYLE 'path',
            USE_SSL {str(parsed.scheme == 'https').lower()},
            REGION {quote_literal(region)}
        )
    """)


def list_registered_files(con, catalog: str, schema: str, table: str) -> set[str]:
    """Get set of file paths already registered in a DuckLake catalog table."""
    try:
        rows = con.execute(f"""
            SELECT data_file
            FROM ducklake_list_files({quote_literal(catalog)}, {quote_literal(table)}, schema => {quote_literal(schema)})
        """).fetchall()
        return {r[0] for r in rows}
    except Exception:
        return set()


def scan_s3_files(con, data_path: str, schema: str, table: str) -> list[str]:
    """Discover Parquet files on S3 for a given table.

    Looks in the new layout: s3://bucket/{prefix}/{schema}/{table}/*.parquet
    Also checks legacy flat layout: s3://bucket/{prefix}/{schema}/{table}.parquet
    """
    found = []

    # New layout: table subdirectory with timestamped files
    table_glob = f"{data_path}{schema}/{table}/*.parquet"
    try:
        rows = con.execute(f"SELECT file FROM glob({quote_literal(table_glob)})").fetchall()
        found.extend(r[0] for r in rows)
    except Exception:
        pass

    # Legacy layout: flat file named after the table
    legacy_path = f"{data_path}{schema}/{table}.parquet"
    try:
        rows = con.execute(f"SELECT file FROM glob({quote_literal(legacy_path)})").fetchall()
        found.extend(r[0] for r in rows)
    except Exception:
        pass

    return found


def sync_workspace_table(con, data_path: str, schema: str, table: str) -> int:
    """Register any S3 files not yet tracked by the workspace catalog.

    Returns the number of newly registered files.
    """
    s3_files = scan_s3_files(con, data_path, schema, table)
    if not s3_files:
        print(f"  No S3 files found for {schema}.{table}")
        return 0

    # Check if table exists in workspace catalog
    table_exists = True
    try:
        con.execute(f'SELECT 1 FROM ws.{quote_ident(schema)}.{quote_ident(table)} LIMIT 0')
    except Exception:
        table_exists = False

    if not table_exists:
        # Create table from the first file's schema
        con.execute(f'CREATE SCHEMA IF NOT EXISTS ws.{quote_ident(schema)}')
        con.execute(f"""
            CREATE TABLE ws.{quote_ident(schema)}.{quote_ident(table)} AS
            SELECT * FROM read_parquet({quote_literal(s3_files[0])}) LIMIT 0
        """)
        print(f"  Created table {schema}.{table} in workspace catalog")

    # Find unregistered files
    registered = list_registered_files(con, "ws", schema, table)
    # Normalize S3 paths to relative (strip data_path prefix) for comparison
    new_files = []
    for full_path in s3_files:
        rel_path = full_path.removeprefix(data_path) if full_path.startswith(data_path) else full_path
        if rel_path not in registered and full_path not in registered:
            new_files.append(full_path)

    if not new_files:
        print(f"  {schema}.{table}: {len(s3_files)} file(s) on S3, all registered")
        return 0

    count = 0
    for file_path in new_files:
        try:
            con.execute(f"""
                CALL ducklake_add_data_files('ws', {quote_literal(table)}, {quote_literal(file_path)},
                    schema => {quote_literal(schema)},
                    allow_missing => true,
                    ignore_extra_columns => true
                )
            """)
            count += 1
        except Exception as e:
            print(f"  WARNING: Failed to register {file_path} in workspace catalog: {e}")

    total = con.execute(f'SELECT COUNT(*) FROM ws.{quote_ident(schema)}.{quote_ident(table)}').fetchone()[0]
    print(f"  {schema}.{table}: registered {count} new file(s), {total} total rows")
    return count


def merge_table_to_global(con, schema: str, table: str) -> int:
    """Copy newly registered files from workspace catalog to global catalog.

    Returns the number of files registered in the global catalog.
    """
    # Ensure table exists in global catalog
    try:
        con.execute(f'SELECT 1 FROM global_cat.{quote_ident(schema)}.{quote_ident(table)} LIMIT 0')
    except Exception:
        print(f"  Creating {schema}.{table} in global catalog...")
        con.execute(f'CREATE SCHEMA IF NOT EXISTS global_cat.{quote_ident(schema)}')
        cols = con.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_catalog = 'ws' AND table_schema = ? AND table_name = ?",
            [schema, table],
        ).fetchall()
        if not cols:
            print(f"  WARNING: Table {schema}.{table} has no columns in workspace catalog, skipping")
            return 0
        col_defs = ", ".join(f'{quote_ident(name)} {dtype}' for name, dtype in cols)
        con.execute(f'CREATE TABLE global_cat.{quote_ident(schema)}.{quote_ident(table)} ({col_defs})')

    # Diff file lists
    ws_files = list_registered_files(con, "ws", schema, table)
    global_files = list_registered_files(con, "global_cat", schema, table)
    new_files = ws_files - global_files

    if not new_files:
        print(f"  {schema}.{table}: global catalog up to date")
        return 0

    print(f"  {schema}.{table}: registering {len(new_files)} new file(s) in global catalog...")
    count = 0
    for file_path in sorted(new_files):
        try:
            con.execute(f"""
                CALL ducklake_add_data_files('global_cat', {quote_literal(table)}, {quote_literal(file_path)},
                    schema => {quote_literal(schema)},
                    allow_missing => true,
                    ignore_extra_columns => true
                )
            """)
            count += 1
        except Exception as e:
            print(f"  WARNING: Failed to register {file_path} in global catalog: {e}")

    return count


def merge_workspace_storage(
    workspace: str,
    storage_name: str,
    schema: str,
    tables: list[str],
    catalog_dir: str,
) -> bool:
    """Merge a workspace's catalog for a single storage target."""
    print(f"\n=== Storage: {storage_name} ===")

    # S3 paths using path builders
    ws_catalog_s3 = build_catalog_path(storage_name, workspace)
    global_catalog_s3 = build_global_catalog_path(storage_name)
    data_path = build_s3_root(storage_name)

    # Use storage-specific subdirectory to avoid conflicts between storages
    storage_catalog_dir = os.path.join(catalog_dir, storage_name)
    os.makedirs(storage_catalog_dir, exist_ok=True)
    ws_catalog_local = os.path.join(storage_catalog_dir, f"{workspace}.duckdb")
    global_catalog_local = os.path.join(storage_catalog_dir, "catalog.duckdb")

    # Setup DuckDB
    try:
        import duckdb
    except ImportError:
        print("  ERROR: duckdb Python package not available.")
        return False

    con = duckdb.connect()
    con.execute("INSTALL ducklake; LOAD ducklake;")
    con.execute("INSTALL httpfs; LOAD httpfs;")

    create_s3_secret(con, storage_name)

    # Download catalogs
    print(f"Downloading workspace catalog: {ws_catalog_s3}")
    if not download_catalog(storage_name, ws_catalog_s3, ws_catalog_local):
        print(f"  INFO: No workspace catalog found. Will create a new one.")

    print(f"Downloading global catalog: {global_catalog_s3}")
    if not download_catalog(storage_name, global_catalog_s3, global_catalog_local):
        print(f"  INFO: No global catalog found. Will create a new one.")

    # -- Phase 1: Sync workspace catalog --
    # Attach workspace catalog READ_WRITE so we can register new S3 files.
    try:
        con.execute(f"""
            ATTACH {quote_literal('ducklake:' + ws_catalog_local)} AS ws (
                DATA_PATH {quote_literal(data_path)}
            )
        """)
    except duckdb.Error as e:
        print(f"  ERROR: Failed to attach workspace catalog: {e}")
        con.close()
        return False

    ws_changed = False
    for table in tables:
        newly_registered = sync_workspace_table(con, data_path, schema, table)
        if newly_registered > 0:
            ws_changed = True

    # Upload workspace catalog if it changed
    if ws_changed:
        con.execute("DETACH ws")
        print(f"Uploading workspace catalog: {ws_catalog_s3}")
        if not upload_catalog(storage_name, ws_catalog_local, ws_catalog_s3):
            print(f"  WARNING: Failed to upload workspace catalog.")
        # Re-attach as read-only for the merge step
        con.execute(f"ATTACH {quote_literal('ducklake:' + ws_catalog_local)} AS ws (READ_ONLY)")
    else:
        # Switch to read-only for the merge step
        con.execute("DETACH ws")
        con.execute(f"ATTACH {quote_literal('ducklake:' + ws_catalog_local)} AS ws (READ_ONLY)")

    # -- Phase 2: Merge to global catalog --
    try:
        con.execute(f"""
            ATTACH {quote_literal('ducklake:' + global_catalog_local)} AS global_cat (
                DATA_PATH {quote_literal(data_path)}
            )
        """)
    except duckdb.Error as e:
        print(f"  ERROR: Failed to attach global catalog: {e}")
        con.close()
        return False

    # Disable auto_compact on global catalog to prevent file deletion
    try:
        con.execute("CALL global_cat.set_option('auto_compact', false)")
    except duckdb.Error:
        pass

    global_changed = False
    for table in tables:
        newly_merged = merge_table_to_global(con, schema, table)
        if newly_merged > 0:
            global_changed = True

    con.close()

    # Upload global catalog if it changed
    if global_changed:
        print(f"Uploading global catalog: {global_catalog_s3}")
        if not upload_catalog(storage_name, global_catalog_local, global_catalog_s3):
            print(f"  ERROR: Failed to upload global catalog.")
            return False
        print(f"  Merge complete for workspace '{workspace}' on storage '{storage_name}'.")
    else:
        print(f"  No changes to global catalog for workspace '{workspace}' on storage '{storage_name}'.")

    return True


def merge_workspace(workspace: str, catalog_dir: str, storage_name: str | None = None) -> bool:
    """Merge a workspace's catalog into the global catalog for one or all storages."""
    # Parse workspace config
    ws_pixi = WORKSPACES_DIR / workspace / "pixi.toml"
    registry = parse_workspace_registry(ws_pixi)
    if not registry:
        print(f"  ERROR: No [tool.registry] found for workspace '{workspace}'.")
        return False

    schema = registry.get("schema", workspace)
    tables = get_tables(registry)
    if not tables:
        print(f"  ERROR: No tables defined for workspace '{workspace}'.")
        return False

    # Determine which storages to merge
    if storage_name:
        storage_names = [storage_name]
    else:
        storage_names = get_workspace_storages(registry)

    print(f"Merging workspace '{workspace}' (schema={schema}) for {len(storage_names)} storage(s): {', '.join(storage_names)}")

    all_ok = True
    for sn in storage_names:
        ok = merge_workspace_storage(workspace, sn, schema, tables, catalog_dir)
        if not ok:
            all_ok = False

    return all_ok


def main():
    parser = argparse.ArgumentParser(description="Merge workspace catalog into global catalog")
    parser.add_argument("--workspace", required=True, help="Workspace name to merge")
    parser.add_argument("--catalog-dir", help="Directory for catalog files (default: temp dir)")
    parser.add_argument("--storage", default=None, help="Specific storage target (default: all workspace storages)")
    args = parser.parse_args()

    if not WORKSPACE_NAME_RE.match(args.workspace):
        print(f"ERROR: Invalid workspace name: {args.workspace}")
        sys.exit(1)

    if args.catalog_dir:
        catalog_dir = args.catalog_dir
        os.makedirs(catalog_dir, exist_ok=True)
        success = merge_workspace(args.workspace, catalog_dir, args.storage)
    else:
        with tempfile.TemporaryDirectory() as tmpdir:
            success = merge_workspace(args.workspace, tmpdir, args.storage)

    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
