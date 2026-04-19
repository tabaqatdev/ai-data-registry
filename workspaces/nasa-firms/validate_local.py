"""Validate extracted NASA FIRMS active_fires GeoParquet output locally."""

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
PATH = f"{OUT}/active_fires.parquet"

EXPECTED_COLS = {
    "sensor", "latitude", "longitude",
    "bright_ti4", "bright_ti5", "frp", "scan", "track",
    "acq_date", "acq_time", "satellite", "confidence", "version", "daynight",
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
        log.warning("Zero fire detections in current window. Valid for quiet days.")
        elapsed = time.monotonic() - t0
        log.info("Validation passed (empty window) (%.1fs)", elapsed)
        return

    sensor_counts = db.execute(f"""
        SELECT sensor, COUNT(*) AS n
        FROM read_parquet('{PATH}')
        GROUP BY sensor
        ORDER BY n DESC
    """).fetchall()
    log.info("Sensor counts: %s",
             ", ".join(f"{s}={n}" for s, n in sensor_counts))

    bbox_check = db.execute(f"""
        SELECT
            MIN(latitude), MAX(latitude),
            MIN(longitude), MAX(longitude)
        FROM read_parquet('{PATH}')
    """).fetchone()
    log.info("BBOX actual: lat [%.2f, %.2f], lon [%.2f, %.2f]", *bbox_check)
    assert bbox_check[0] >= 12.0 and bbox_check[1] <= 38.0, "latitude outside bbox"
    assert bbox_check[2] >= 25.0 and bbox_check[3] <= 62.0, "longitude outside bbox"

    dupes = db.execute(f"""
        SELECT sensor, latitude, longitude, acq_date, acq_time, snapshot_time, COUNT(*) AS n
        FROM read_parquet('{PATH}')
        GROUP BY ALL
        HAVING n > 1
        LIMIT 1
    """).fetchone()
    assert dupes is None, f"Duplicate detection: {dupes}"

    snap_check = db.execute(f"""
        SELECT COUNT(DISTINCT snapshot_time) FROM read_parquet('{PATH}')
    """).fetchone()[0]
    assert snap_check == 1, f"Expected 1 snapshot_time, got {snap_check}"

    h3_null = db.execute(f"""
        SELECT
            COUNT(*) FILTER (WHERE h3_r5 IS NULL) AS n5,
            COUNT(*) FILTER (WHERE h3_r8 IS NULL) AS n8,
            COUNT(*) FILTER (WHERE h3_r10 IS NULL) AS n10
        FROM read_parquet('{PATH}')
    """).fetchone()
    log.info("H3 nulls - r5: %d, r8: %d, r10: %d", *h3_null)
    assert all(n == 0 for n in h3_null), "H3 cells must be non-null"

    log.info("Summary of fire intensity:")
    summary = db.execute(f"""
        SUMMARIZE SELECT bright_ti4, bright_ti5, frp
        FROM read_parquet('{PATH}')
    """).fetchall()
    for row in summary:
        col, min_v, max_v, null_pct = row[0], row[2], row[3], row[11]
        log.info("  %s: min=%s max=%s null_pct=%s%%", col, min_v, max_v, null_pct)

    elapsed = time.monotonic() - t0
    log.info("Validation passed (%.1fs)", elapsed)


if __name__ == "__main__":
    main()
