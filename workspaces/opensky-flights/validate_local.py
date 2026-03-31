"""Validate extracted GeoParquet output locally."""

import os
import sys

import duckdb

OUT = os.environ.get("OUTPUT_DIR", "output")
STATES_PATH = f"{OUT}/states.parquet"
FLIGHTS_PATH = f"{OUT}/flights.parquet"


def validate_states(db):
    """Validate states GeoParquet output."""
    print(f"Validating {STATES_PATH}...")

    count = db.execute(f"SELECT COUNT(*) FROM read_parquet('{STATES_PATH}')").fetchone()[0]
    print(f"  Row count: {count}")
    assert count >= 1000, f"Too few rows: {count} (expected >= 1000)"

    nulls = db.execute(f"""
        SELECT
            COUNT(*) FILTER (WHERE icao24 IS NULL) AS null_icao24,
            COUNT(*) FILTER (WHERE longitude IS NULL) AS null_lon,
            COUNT(*) FILTER (WHERE latitude IS NULL) AS null_lat,
            COUNT(*) FILTER (WHERE snapshot_time IS NULL) AS null_snapshot
        FROM read_parquet('{STATES_PATH}')
    """).fetchone()
    print(f"  Null counts - icao24: {nulls[0]}, lon: {nulls[1]}, lat: {nulls[2]}, snapshot: {nulls[3]}")
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

    print("  States validation passed.")


def validate_flights(db):
    """Validate flights Parquet output (optional, may not exist)."""
    try:
        count = db.execute(f"SELECT COUNT(*) FROM read_parquet('{FLIGHTS_PATH}')").fetchone()[0]
    except Exception:
        print(f"  No flights file at {FLIGHTS_PATH}, skipping")
        return

    print(f"Validating {FLIGHTS_PATH}...")
    print(f"  Row count: {count}")

    if count == 0:
        print("  Empty flights file, skipping further checks")
        return

    nulls = db.execute(f"""
        SELECT
            COUNT(*) FILTER (WHERE icao24 IS NULL) AS null_icao24,
            COUNT(*) FILTER (WHERE first_seen IS NULL) AS null_first,
            COUNT(*) FILTER (WHERE last_seen IS NULL) AS null_last
        FROM read_parquet('{FLIGHTS_PATH}')
    """).fetchone()
    print(f"  Null counts - icao24: {nulls[0]}, first_seen: {nulls[1]}, last_seen: {nulls[2]}")
    assert nulls[0] == 0, "icao24 must not be null in flights"

    dupes = db.execute(f"""
        SELECT icao24, first_seen, COUNT(*) AS n
        FROM read_parquet('{FLIGHTS_PATH}')
        GROUP BY icao24, first_seen
        HAVING n > 1
        LIMIT 1
    """).fetchone()
    assert dupes is None, f"Duplicate (icao24, first_seen) in flights: {dupes}"

    print("  Flights validation passed.")


def main():
    db = duckdb.connect()
    db.execute("INSTALL spatial; LOAD spatial;")

    validate_states(db)
    validate_flights(db)

    db.close()
    print("All validations passed.")


if __name__ == "__main__":
    main()
