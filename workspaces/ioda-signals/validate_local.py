"""Validate extracted IODA signals Parquet output locally."""

import logging
import os
import time

import duckdb

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    level=logging.DEBUG if os.environ.get("DRY_RUN") else logging.INFO,
)
log = logging.getLogger(__name__)

OUT = os.environ.get("OUTPUT_DIR", "output")
PATH = f"{OUT}/signals.parquet"
DRY_RUN = os.environ.get("DRY_RUN", "0") == "1"

EXPECTED_COLS = {
    "country_code", "country_name", "datasource", "subtype",
    "timestamp", "timestamp_utc", "step_seconds", "value", "snapshot_time",
}

MIN_ROWS = 20 if os.environ.get("DRY_RUN") else 500


def main():
    t0 = time.monotonic()
    db = duckdb.connect()

    log.info("Validating %s...", PATH)
    if not os.path.exists(PATH):
        log.error("Output not found at %s", PATH)
        raise SystemExit(1)

    count = db.execute(f"SELECT COUNT(*) FROM read_parquet('{PATH}')").fetchone()[0]
    log.info("Row count: %d", count)
    assert count >= MIN_ROWS, f"Too few rows: {count} (expected >= {MIN_ROWS})"

    cols = db.execute(f"DESCRIBE SELECT * FROM read_parquet('{PATH}')").fetchall()
    col_names = {c[0] for c in cols}
    missing = EXPECTED_COLS - col_names
    assert not missing, f"Missing columns: {missing}"
    log.info("Schema OK (%d columns)", len(col_names))

    dupes = db.execute(f"""
        SELECT country_code, datasource, subtype, timestamp, snapshot_time, COUNT(*) AS n
        FROM read_parquet('{PATH}')
        GROUP BY ALL
        HAVING n > 1
        LIMIT 1
    """).fetchone()
    assert dupes is None, f"Duplicate composite key: {dupes}"

    country_counts = db.execute(f"""
        SELECT country_code, COUNT(*) AS n
        FROM read_parquet('{PATH}')
        GROUP BY country_code
        ORDER BY n DESC
    """).fetchall()
    log.info("Countries: %s", ", ".join(f"{c}={n}" for c, n in country_counts[:10]))

    ds_counts = db.execute(f"""
        SELECT datasource, COUNT(*) AS n
        FROM read_parquet('{PATH}')
        GROUP BY datasource
        ORDER BY n DESC
    """).fetchall()
    log.info("Datasources: %s", ", ".join(f"{d}={n}" for d, n in ds_counts))

    null_vals = db.execute(
        f"SELECT COUNT(*) FROM read_parquet('{PATH}') WHERE value IS NULL"
    ).fetchone()[0]
    log.info("Null values: %d", null_vals)

    elapsed = time.monotonic() - t0
    log.info("Validation passed (%.1fs)", elapsed)


if __name__ == "__main__":
    main()
