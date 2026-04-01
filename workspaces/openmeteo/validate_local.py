"""Validate extracted weather and air quality Parquet output locally."""

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
WEATHER_HOURLY_PATH = f"{OUT}/weather_hourly.parquet"
WEATHER_DAILY_PATH = f"{OUT}/weather_daily.parquet"
AIR_QUALITY_PATH = f"{OUT}/air_quality.parquet"


def validate_weather_hourly(db):
    """Validate weather_hourly GeoParquet output."""
    log.info("Validating %s...", WEATHER_HOURLY_PATH)

    count = db.execute(
        f"SELECT COUNT(*) FROM read_parquet('{WEATHER_HOURLY_PATH}')"
    ).fetchone()[0]
    log.info("Row count: %d", count)
    assert count >= 500, f"Too few rows: {count} (expected >= 500)"

    nulls = db.execute(f"""
        SELECT
            COUNT(*) FILTER (WHERE city IS NULL) AS null_city,
            COUNT(*) FILTER (WHERE country_code IS NULL) AS null_cc,
            COUNT(*) FILTER (WHERE "time" IS NULL) AS null_time,
            COUNT(*) FILTER (WHERE latitude IS NULL) AS null_lat,
            COUNT(*) FILTER (WHERE longitude IS NULL) AS null_lon
        FROM read_parquet('{WEATHER_HOURLY_PATH}')
    """).fetchone()
    log.info("Null counts - city: %d, country: %d, time: %d, lat: %d, lon: %d", nulls[0], nulls[1], nulls[2], nulls[3], nulls[4])
    assert nulls[0] == 0, "city must not be null"
    assert nulls[1] == 0, "country_code must not be null"
    assert nulls[2] == 0, "time must not be null"

    null_pct = db.execute(f"""
        SELECT
            ROUND(100.0 * COUNT(*) FILTER (WHERE temperature_2m IS NULL) / COUNT(*), 1)
        FROM read_parquet('{WEATHER_HOURLY_PATH}')
    """).fetchone()[0]
    log.info("Temperature null %%: %s%%", null_pct)
    assert null_pct <= 15, f"Too many null temperatures: {null_pct}%"

    dupes = db.execute(f"""
        SELECT city, country_code, latitude, longitude, "time", COUNT(*) AS n
        FROM read_parquet('{WEATHER_HOURLY_PATH}')
        GROUP BY city, country_code, latitude, longitude, "time"
        HAVING n > 1
        LIMIT 1
    """).fetchone()
    assert dupes is None, f"Duplicate (city, country_code, lat, lon, time): {dupes}"

    cols = db.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{WEATHER_HOURLY_PATH}')"
    ).fetchall()
    col_names = [c[0] for c in cols]
    assert "geometry" in col_names, f"Missing geometry column. Columns: {col_names}"
    assert "time" in col_names, f"Missing time column. Columns: {col_names}"

    countries = db.execute(f"""
        SELECT COUNT(DISTINCT country_code)
        FROM read_parquet('{WEATHER_HOURLY_PATH}')
    """).fetchone()[0]
    log.info("Countries covered: %d", countries)

    log.info("Weather hourly validation passed.")


def validate_weather_daily(db):
    """Validate weather_daily GeoParquet output."""
    log.info("Validating %s...", WEATHER_DAILY_PATH)

    count = db.execute(
        f"SELECT COUNT(*) FROM read_parquet('{WEATHER_DAILY_PATH}')"
    ).fetchone()[0]
    log.info("Row count: %d", count)
    assert count >= 100, f"Too few rows: {count} (expected >= 100)"

    nulls = db.execute(f"""
        SELECT
            COUNT(*) FILTER (WHERE city IS NULL) AS null_city,
            COUNT(*) FILTER (WHERE country_code IS NULL) AS null_cc,
            COUNT(*) FILTER (WHERE "date" IS NULL) AS null_date
        FROM read_parquet('{WEATHER_DAILY_PATH}')
    """).fetchone()
    log.info("Null counts - city: %d, country: %d, date: %d", nulls[0], nulls[1], nulls[2])
    assert nulls[0] == 0, "city must not be null"
    assert nulls[1] == 0, "country_code must not be null"
    assert nulls[2] == 0, "date must not be null"

    dupes = db.execute(f"""
        SELECT city, country_code, latitude, longitude, "date", COUNT(*) AS n
        FROM read_parquet('{WEATHER_DAILY_PATH}')
        GROUP BY city, country_code, latitude, longitude, "date"
        HAVING n > 1
        LIMIT 1
    """).fetchone()
    assert dupes is None, f"Duplicate (city, country_code, lat, lon, date): {dupes}"

    cols = db.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{WEATHER_DAILY_PATH}')"
    ).fetchall()
    col_names = [c[0] for c in cols]
    assert "geometry" in col_names, f"Missing geometry column. Columns: {col_names}"

    log.info("Weather daily validation passed.")


def validate_air_quality(db):
    """Validate air_quality GeoParquet output."""
    try:
        count = db.execute(
            f"SELECT COUNT(*) FROM read_parquet('{AIR_QUALITY_PATH}')"
        ).fetchone()[0]
    except Exception:
        log.warning("No air quality file at %s, skipping", AIR_QUALITY_PATH)
        return

    log.info("Validating %s...", AIR_QUALITY_PATH)
    log.info("Row count: %d", count)
    assert count >= 200, f"Too few rows: {count} (expected >= 200)"

    nulls = db.execute(f"""
        SELECT
            COUNT(*) FILTER (WHERE city IS NULL) AS null_city,
            COUNT(*) FILTER (WHERE country_code IS NULL) AS null_cc,
            COUNT(*) FILTER (WHERE "time" IS NULL) AS null_time
        FROM read_parquet('{AIR_QUALITY_PATH}')
    """).fetchone()
    log.info("Null counts - city: %d, country: %d, time: %d", nulls[0], nulls[1], nulls[2])
    assert nulls[0] == 0, "city must not be null"
    assert nulls[1] == 0, "country_code must not be null"
    assert nulls[2] == 0, "time must not be null"

    # AQ data has higher null rates (not all pollutants measured everywhere)
    null_pct = db.execute(f"""
        SELECT
            ROUND(100.0 * COUNT(*) FILTER (WHERE us_aqi IS NULL) / COUNT(*), 1)
        FROM read_parquet('{AIR_QUALITY_PATH}')
    """).fetchone()[0]
    log.info("US AQI null %%: %s%%", null_pct)
    assert null_pct <= 25, f"Too many null AQI values: {null_pct}%"

    dupes = db.execute(f"""
        SELECT city, country_code, latitude, longitude, "time", COUNT(*) AS n
        FROM read_parquet('{AIR_QUALITY_PATH}')
        GROUP BY city, country_code, latitude, longitude, "time"
        HAVING n > 1
        LIMIT 1
    """).fetchone()
    assert dupes is None, f"Duplicate (city, country_code, lat, lon, time): {dupes}"

    cols = db.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{AIR_QUALITY_PATH}')"
    ).fetchall()
    col_names = [c[0] for c in cols]
    assert "geometry" in col_names, f"Missing geometry column. Columns: {col_names}"

    log.info("Air quality validation passed.")


def main():
    t0 = time.monotonic()
    db = duckdb.connect()
    db.execute("INSTALL spatial; LOAD spatial;")

    validate_weather_hourly(db)
    validate_weather_daily(db)
    validate_air_quality(db)

    db.close()
    elapsed = time.monotonic() - t0
    log.info("All validations passed (%.1fs)", elapsed)


if __name__ == "__main__":
    main()
