"""Validate extracted CelesTrak satellites GeoParquet output locally."""

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
PATH = f"{OUT}/satellites.parquet"
DRY_RUN = os.environ.get("DRY_RUN", "0") == "1"

EXPECTED_COLS = {
    "norad_cat_id", "object_name", "object_id", "group", "group_label",
    "epoch", "mean_motion", "eccentricity", "inclination",
    "ra_of_asc_node", "arg_of_pericenter", "mean_anomaly",
    "classification", "bstar", "rev_at_epoch",
    "latitude_approx", "longitude_approx", "altitude_km",
    "h3_r5", "over_region", "snapshot_time", "geometry",
}

MIN_ROWS = 50 if os.environ.get("DRY_RUN") else 500


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
    assert count >= MIN_ROWS, f"Too few rows: {count} (expected >= {MIN_ROWS})"

    cols = db.execute(f"DESCRIBE SELECT * FROM read_parquet('{PATH}')").fetchall()
    col_names = {c[0] for c in cols}
    missing = EXPECTED_COLS - col_names
    assert not missing, f"Missing columns: {missing}"
    log.info("Schema OK (%d columns)", len(col_names))

    dupes = db.execute(f"""
        SELECT norad_cat_id, snapshot_time, COUNT(*) AS n
        FROM read_parquet('{PATH}')
        GROUP BY ALL
        HAVING n > 1
        LIMIT 1
    """).fetchone()
    assert dupes is None, f"Duplicate (norad_cat_id, snapshot_time): {dupes}"

    group_counts = db.execute(f"""
        SELECT "group", COUNT(*) AS n
        FROM read_parquet('{PATH}')
        GROUP BY "group"
        ORDER BY n DESC
    """).fetchall()
    log.info("Groups: %s", ", ".join(f"{g}={n}" for g, n in group_counts))

    alt_stats = db.execute(f"""
        SELECT MIN(altitude_km), MAX(altitude_km), AVG(altitude_km),
               COUNT(*) FILTER (WHERE altitude_km IS NULL)
        FROM read_parquet('{PATH}')
    """).fetchone()
    log.info("Altitude km - min: %.1f, max: %.1f, avg: %.1f, null: %d",
             alt_stats[0] or -1, alt_stats[1] or -1, alt_stats[2] or -1, alt_stats[3])

    over_region = db.execute(
        f"SELECT COUNT(*) FROM read_parquet('{PATH}') WHERE over_region"
    ).fetchone()[0]
    log.info("Satellites currently over SA + Gulf region: %d", over_region)

    elapsed = time.monotonic() - t0
    log.info("Validation passed (%.1fs)", elapsed)


if __name__ == "__main__":
    main()
