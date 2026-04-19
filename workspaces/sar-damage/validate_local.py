"""Validate SAR damage detection output locally.

Two validation modes:
  - Full output: damage_cells.parquet must exist with expected schema.
  - Search-only (DRY_RUN): search_summary.json must report pre/post scenes.
"""

import json
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
PARQUET = f"{OUT}/damage_cells.parquet"
SEARCH_SUMMARY = f"{OUT}/search_summary.json"

EXPECTED_COLS = {
    "h3_index", "h3_hex", "h3_r9", "h3_r9_hex",
    "latitude", "longitude",
    "pixel_count", "damaged_pixels",
    "avg_t_score", "max_t_score", "damage_ratio",
    "aoi", "event_date", "snapshot_time", "geometry",
}


def validate_search_summary():
    log.info("Validating search summary %s...", SEARCH_SUMMARY)
    with open(SEARCH_SUMMARY) as f:
        data = json.load(f)
    log.info("AOI: %s event: %s pre=%d post=%d via %s",
             data["aoi"], data["event_date"],
             data["pre_scenes"], data["post_scenes"], data["catalog"])
    assert data["pre_scenes"] >= 1, "No pre-event scenes"
    assert data["post_scenes"] >= 1, "No post-event scenes"
    log.info("Search-only validation passed")


def validate_parquet():
    db = duckdb.connect()
    db.execute("INSTALL spatial; LOAD spatial;")
    log.info("Validating %s...", PARQUET)

    count = db.execute(f"SELECT COUNT(*) FROM read_parquet('{PARQUET}')").fetchone()[0]
    log.info("Row count: %d", count)
    assert count > 0, "Empty damage cells"

    cols = db.execute(f"DESCRIBE SELECT * FROM read_parquet('{PARQUET}')").fetchall()
    col_names = {c[0] for c in cols}
    missing = EXPECTED_COLS - col_names
    assert not missing, f"Missing cols: {missing}"

    dupes = db.execute(f"""
        SELECT h3_index, aoi, event_date, COUNT(*) AS n
        FROM read_parquet('{PARQUET}')
        GROUP BY ALL
        HAVING n > 1
        LIMIT 1
    """).fetchone()
    assert dupes is None, f"Duplicate key: {dupes}"

    stats = db.execute(f"""
        SELECT
            MIN(damage_ratio), MAX(damage_ratio), AVG(damage_ratio),
            SUM(damaged_pixels), SUM(pixel_count)
        FROM read_parquet('{PARQUET}')
    """).fetchone()
    log.info("Damage ratio - min=%.3f max=%.3f avg=%.3f  damaged=%d/%d",
             *stats)


def main():
    t0 = time.monotonic()
    if os.path.exists(PARQUET):
        validate_parquet()
    elif os.path.exists(SEARCH_SUMMARY):
        validate_search_summary()
    else:
        log.error("No output produced (neither %s nor %s exist)", PARQUET, SEARCH_SUMMARY)
        raise SystemExit(1)
    log.info("Validation passed (%.1fs)", time.monotonic() - t0)


if __name__ == "__main__":
    main()
