"""Extract NASA FIRMS active fires for the SA + Gulf region (24h rolling).

Three sensors are queried independently so that one going offline does not
take down the pipeline (MODIS-on-Aqua/Terra is in extended mission and may
retire). Outputs are unioned, bbox-filtered, H3-indexed at three resolutions
(r5/r8/r10), Hilbert-sorted, and written as GeoParquet 2.x.

Append mode with `snapshot_time` as part of the unique key, so every 3-hour
run produces a new file and DuckLake retains the full hourly trail.
"""

import logging
import os
import time
from datetime import datetime, timezone

import duckdb

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    level=logging.DEBUG if os.environ.get("DRY_RUN") else logging.INFO,
)
log = logging.getLogger(__name__)

OUT = os.environ.get("OUTPUT_DIR", "output")
DRY_RUN = os.environ.get("DRY_RUN", "0") == "1"

# SA + Gulf bbox: includes SA, Iran, Iraq, Egypt, Syria, Yemen, Gulf states
BBOX = {"min_lon": 25.0, "max_lon": 62.0, "min_lat": 12.0, "max_lat": 38.0}

SENSORS = [
    (
        "VIIRS_SNPP",
        "https://firms.modaps.eosdis.nasa.gov/data/active_fire/"
        "suomi-npp-viirs-c2/csv/SUOMI_VIIRS_C2_Global_24h.csv",
    ),
    (
        "VIIRS_NOAA20",
        "https://firms.modaps.eosdis.nasa.gov/data/active_fire/"
        "noaa-20-viirs-c2/csv/J1_VIIRS_C2_Global_24h.csv",
    ),
    (
        "MODIS",
        "https://firms.modaps.eosdis.nasa.gov/data/active_fire/"
        "modis-c6.1/csv/MODIS_C6_1_Global_24h.csv",
    ),
]

COMMON_COLS = [
    "latitude", "longitude", "bright_ti4", "bright_ti5", "frp",
    "scan", "track", "acq_date", "acq_time", "satellite",
    "confidence", "version", "daynight",
]


def fetch_sensor(db, sensor, url, limit_clause=""):
    """Fetch one sensor's CSV into a temp table. Returns row count or None on failure.

    MODIS CSV has slightly different columns (no bright_ti4/ti5, has brightness/bright_t31).
    Aligned by coalescing via COLUMNS() pattern would be brittle; instead, keep both
    bright_ti4/ti5 for VIIRS and set them to NULL for MODIS, filled from MODIS's
    `brightness` and `bright_t31`.
    """
    log.info("Fetching %s from %s", sensor, url)
    is_modis = sensor == "MODIS"
    try:
        if is_modis:
            # MODIS columns: latitude, longitude, brightness, scan, track, acq_date,
            # acq_time, satellite, confidence, version, bright_t31, frp, daynight
            db.execute(f"""
                CREATE OR REPLACE TEMP TABLE sensor_{sensor} AS
                SELECT
                    latitude, longitude,
                    brightness AS bright_ti4,
                    bright_t31 AS bright_ti5,
                    frp, scan, track,
                    acq_date, acq_time, satellite,
                    confidence::VARCHAR AS confidence,
                    version, daynight,
                    '{sensor}' AS sensor
                FROM read_csv('{url}', auto_detect=true, sample_size=-1)
                WHERE latitude BETWEEN {BBOX['min_lat']} AND {BBOX['max_lat']}
                  AND longitude BETWEEN {BBOX['min_lon']} AND {BBOX['max_lon']}
                {limit_clause}
            """)
        else:
            # VIIRS columns: latitude, longitude, bright_ti4, bright_ti5, scan, track,
            # acq_date, acq_time, satellite, confidence, version, frp, daynight
            db.execute(f"""
                CREATE OR REPLACE TEMP TABLE sensor_{sensor} AS
                SELECT
                    latitude, longitude,
                    bright_ti4, bright_ti5,
                    frp, scan, track,
                    acq_date, acq_time, satellite,
                    confidence, version, daynight,
                    '{sensor}' AS sensor
                FROM read_csv('{url}', auto_detect=true, sample_size=-1)
                WHERE latitude BETWEEN {BBOX['min_lat']} AND {BBOX['max_lat']}
                  AND longitude BETWEEN {BBOX['min_lon']} AND {BBOX['max_lon']}
                {limit_clause}
            """)
    except duckdb.Error:
        log.exception("Sensor %s failed; continuing without it", sensor)
        return None

    n = db.execute(f"SELECT COUNT(*) FROM sensor_{sensor}").fetchone()[0]
    log.info("%s: %d rows in bbox", sensor, n)
    return n


def main():
    t0 = time.monotonic()
    mode = "dry-run" if DRY_RUN else "extract"
    log.info("Starting %s, output_dir=%s", mode, OUT)

    os.makedirs(OUT, exist_ok=True)
    out_path = f"{OUT}/active_fires.parquet"

    db = duckdb.connect()
    db.execute("INSTALL httpfs; LOAD httpfs;")
    db.execute("INSTALL spatial; LOAD spatial;")
    db.execute("INSTALL h3 FROM community; LOAD h3;")
    db.execute("SET geometry_always_xy = true;")

    snapshot = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    log.info("Snapshot timestamp: %s", snapshot)

    limit_clause = "LIMIT 200" if DRY_RUN else ""

    valid_sensors = []
    for sensor, url in SENSORS:
        n = fetch_sensor(db, sensor, url, limit_clause)
        if n is not None:
            valid_sensors.append(sensor)

    if not valid_sensors:
        log.error("All sensor fetches failed. Aborting.")
        raise SystemExit(1)

    log.info("Union of %d sensors: %s", len(valid_sensors), valid_sensors)
    union_sql = "\nUNION ALL\n".join(
        f"SELECT * FROM sensor_{s}" for s in valid_sensors
    )

    db.execute(f"""
        CREATE TABLE unified AS
        {union_sql}
    """)

    n_all = db.execute("SELECT COUNT(*) FROM unified").fetchone()[0]
    log.info("Total unioned: %d rows", n_all)

    log.info("Writing %s with H3 r5/r8/r10 and Hilbert sort...", out_path)
    db.execute(f"""
        COPY (
            SELECT
                sensor,
                latitude, longitude,
                bright_ti4, bright_ti5, frp,
                scan, track,
                acq_date, acq_time,
                satellite, confidence, version, daynight,
                h3_latlng_to_cell(latitude, longitude, 5)::UBIGINT AS h3_r5,
                h3_latlng_to_cell(latitude, longitude, 8)::UBIGINT AS h3_r8,
                h3_latlng_to_cell(latitude, longitude, 10)::UBIGINT AS h3_r10,
                '{snapshot}'::TIMESTAMP AS snapshot_time,
                ST_SetCRS(ST_Point(longitude, latitude), 'EPSG:4326') AS geometry
            FROM unified
            ORDER BY ST_Hilbert(ST_Point(longitude, latitude)), acq_date, acq_time
        ) TO '{out_path}' (
            FORMAT PARQUET,
            COMPRESSION ZSTD,
            COMPRESSION_LEVEL 15,
            ROW_GROUP_SIZE 100000,
            GEOPARQUET_VERSION 'V2'
        )
    """)

    final = db.execute(
        f"SELECT COUNT(*) FROM read_parquet('{out_path}')"
    ).fetchone()[0]
    size_mb = os.path.getsize(out_path) / 1024 / 1024
    log.info("Wrote %s (%d rows, %.2f MB)", out_path, final, size_mb)

    db.close()
    elapsed = time.monotonic() - t0
    log.info("%s complete: %d rows in %.1fs", mode, final, elapsed)


if __name__ == "__main__":
    main()
