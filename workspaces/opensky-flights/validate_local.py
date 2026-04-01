"""Validate extracted GeoParquet output locally."""

import logging
import os
import sys
import time

import duckdb

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    level=logging.DEBUG if os.environ.get("DRY_RUN") else logging.INFO,
)
log = logging.getLogger(__name__)

OUT = os.environ.get("OUTPUT_DIR", "output")
STATES_PATH = f"{OUT}/states.parquet"
FLIGHTS_PATH = f"{OUT}/flights.parquet"


def validate_states(db):
    """Validate states GeoParquet output."""
    log.info("Validating %s...", STATES_PATH)

    count = db.execute(f"SELECT COUNT(*) FROM read_parquet('{STATES_PATH}')").fetchone()[0]
    log.info("Row count: %d", count)
    assert count >= 1000, f"Too few rows: {count} (expected >= 1000)"

    nulls = db.execute(f"""
        SELECT
            COUNT(*) FILTER (WHERE icao24 IS NULL) AS null_icao24,
            COUNT(*) FILTER (WHERE longitude IS NULL) AS null_lon,
            COUNT(*) FILTER (WHERE latitude IS NULL) AS null_lat,
            COUNT(*) FILTER (WHERE snapshot_time IS NULL) AS null_snapshot
        FROM read_parquet('{STATES_PATH}')
    """).fetchone()
    log.debug("Null counts - icao24: %d, lon: %d, lat: %d, snapshot: %d", nulls[0], nulls[1], nulls[2], nulls[3])
    assert nulls[0] == 0, "icao24 must not be null"
    assert nulls[1] == 0, "longitude must not be null"
    assert nulls[2] == 0, "latitude must not be null"
    assert nulls[3] == 0, "snapshot_time must not be null"

    dupes = db.execute(f"""
        SELECT icao24, snapshot_time, COUNT(*) AS n
        FROM read_parquet('{STATES_PATH}')
        GROUP BY icao24, snapshot_time
        HAVING n > 1
        LIMIT 1
    """).fetchone()
    assert dupes is None, f"Duplicate (icao24, snapshot_time): {dupes}"

    cols = db.execute(f"DESCRIBE SELECT * FROM read_parquet('{STATES_PATH}')").fetchall()
    col_names = [c[0] for c in cols]
    assert "geometry" in col_names, f"Missing geometry column. Columns: {col_names}"
    assert "snapshot_time" in col_names, f"Missing snapshot_time column. Columns: {col_names}"

    log.info("States validation passed.")


def validate_flights(db):
    """Validate flights Parquet output (optional, may not exist)."""
    try:
        count = db.execute(f"SELECT COUNT(*) FROM read_parquet('{FLIGHTS_PATH}')").fetchone()[0]
    except Exception:
        log.info("No flights file at %s, skipping", FLIGHTS_PATH)
        return

    log.info("Validating %s...", FLIGHTS_PATH)
    log.info("Row count: %d", count)

    if count == 0:
        log.info("Empty flights file, skipping further checks")
        return

    nulls = db.execute(f"""
        SELECT
            COUNT(*) FILTER (WHERE icao24 IS NULL) AS null_icao24,
            COUNT(*) FILTER (WHERE first_seen IS NULL) AS null_first,
            COUNT(*) FILTER (WHERE last_seen IS NULL) AS null_last
        FROM read_parquet('{FLIGHTS_PATH}')
    """).fetchone()
    log.debug("Null counts - icao24: %d, first_seen: %d, last_seen: %d", nulls[0], nulls[1], nulls[2])
    assert nulls[0] == 0, "icao24 must not be null in flights"

    dupes = db.execute(f"""
        SELECT icao24, first_seen, COUNT(*) AS n
        FROM read_parquet('{FLIGHTS_PATH}')
        GROUP BY icao24, first_seen
        HAVING n > 1
        LIMIT 1
    """).fetchone()
    assert dupes is None, f"Duplicate (icao24, first_seen) in flights: {dupes}"

    log.info("Flights validation passed.")


def main():
    t0 = time.time()
    db = duckdb.connect()
    db.execute("INSTALL spatial; LOAD spatial;")

    validate_states(db)
    validate_flights(db)

    db.close()
    elapsed = time.time() - t0
    log.info("All validations passed (%.1fs)", elapsed)


if __name__ == "__main__":
    main()
