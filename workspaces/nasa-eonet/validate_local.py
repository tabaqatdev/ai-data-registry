"""Validate extracted NASA EONET events GeoParquet output locally."""

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
PATH = f"{OUT}/events.parquet"

EXPECTED_COLS = {
    "event_id", "title", "description",
    "category_id", "category_title",
    "is_closed", "closed_date",
    "longitude", "latitude",
    "magnitude_value", "magnitude_unit",
    "event_date", "geometry_count", "source_count",
    "source_ids", "source_urls",
    "h3_r5", "h3_r8", "h3_r10",
    "snapshot_time", "geometry",
}


def main():
    t0 = time.monotonic()
    db = duckdb.connect()
    db.execute("INSTALL spatial; LOAD spatial;")

    log.info("Validating %s...", PATH)
    if not os.path.exists(PATH):
        log.error("Output not found at %s", PATH)
        raise SystemExit(1)

    count = db.execute(f"SELECT COUNT(*) FROM read_parquet('{PATH}')").fetchone()[0]
    log.info("Row count: %d", count)

    cols = db.execute(f"DESCRIBE SELECT * FROM read_parquet('{PATH}')").fetchall()
    col_names = {c[0] for c in cols}
    missing = EXPECTED_COLS - col_names
    assert not missing, f"Missing expected columns: {missing}"
    log.info("Schema OK (%d columns)", len(col_names))

    if count == 0:
        log.warning("No events in current window. Valid for quiet periods.")
        elapsed = time.monotonic() - t0
        log.info("Validation passed (empty window) (%.1fs)", elapsed)
        return

    dupes = db.execute(f"""
        SELECT event_id, snapshot_time, COUNT(*) AS n
        FROM read_parquet('{PATH}')
        GROUP BY ALL
        HAVING n > 1
        LIMIT 1
    """).fetchone()
    assert dupes is None, f"Duplicate (event_id, snapshot_time): {dupes}"

    cat_counts = db.execute(f"""
        SELECT category_id, COUNT(*) AS n
        FROM read_parquet('{PATH}')
        GROUP BY category_id
        ORDER BY n DESC
    """).fetchall()
    log.info("Categories: %s",
             ", ".join(f"{c}={n}" for c, n in cat_counts))

    bbox_check = db.execute(f"""
        SELECT MIN(latitude), MAX(latitude), MIN(longitude), MAX(longitude)
        FROM read_parquet('{PATH}')
    """).fetchone()
    log.info("BBOX actual: lat [%.2f, %.2f], lon [%.2f, %.2f]", *bbox_check)

    h3_nulls = db.execute(f"""
        SELECT
            COUNT(*) FILTER (WHERE h3_r5 IS NULL),
            COUNT(*) FILTER (WHERE h3_r8 IS NULL),
            COUNT(*) FILTER (WHERE h3_r10 IS NULL)
        FROM read_parquet('{PATH}')
    """).fetchone()
    log.info("H3 nulls - r5: %d, r8: %d, r10: %d", *h3_nulls)
    assert all(n == 0 for n in h3_nulls), "H3 cells must be non-null"

    elapsed = time.monotonic() - t0
    log.info("Validation passed (%.1fs)", elapsed)


if __name__ == "__main__":
    main()
