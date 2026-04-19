"""Extract USGS FDSN earthquakes for the Middle East.

Bbox: 12-42 N, 25-65 E (covers Zagros, Red Sea rift, Arabian plate, Gulf).
Rolling 30-day window, run every 6h, append mode with snapshot_time in key.
"""

import logging
import os
import time
import urllib.request
from datetime import datetime, timedelta, timezone

import duckdb

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    level=logging.DEBUG if os.environ.get("DRY_RUN") else logging.INFO,
)
log = logging.getLogger(__name__)

OUT = os.environ.get("OUTPUT_DIR", "output")
DRY_RUN = os.environ.get("DRY_RUN", "0") == "1"

BBOX = {"lat_min": 12.0, "lat_max": 42.0, "lon_min": 25.0, "lon_max": 65.0}
LOOKBACK_DAYS = 30

FDSN = "https://earthquake.usgs.gov/fdsnws/event/1/query"

USER_AGENT = "ai-data-registry/usgs-earthquakes"
HTTP_TIMEOUT_SECS = 60
HTTP_RETRIES = 4
HTTP_RETRY_WAIT_SECS = 5


def fetch_to_file(url, dest_path):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    last_err = None
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_SECS) as resp:
                with open(dest_path, "wb") as f:
                    while True:
                        chunk = resp.read(1024 * 256)
                        if not chunk:
                            break
                        f.write(chunk)
            return
        except Exception as e:
            last_err = e
            code = getattr(e, "code", None)
            if code is not None and 400 <= code < 500 and code != 429:
                log.error("HTTP %d (terminal) on %s", code, url)
                raise
            log.warning("attempt %d/%d failed (%s); retrying in %ds",
                        attempt, HTTP_RETRIES, e, HTTP_RETRY_WAIT_SECS * attempt)
            if attempt < HTTP_RETRIES:
                time.sleep(HTTP_RETRY_WAIT_SECS * attempt)
    raise RuntimeError(f"giving up on {url} after {HTTP_RETRIES} attempts: {last_err}")


def main():
    t0 = time.monotonic()
    mode = "dry-run" if DRY_RUN else "extract"
    log.info("Starting %s, output_dir=%s", mode, OUT)
    os.makedirs(OUT, exist_ok=True)

    now = datetime.now(timezone.utc)
    start = (now - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%dT%H:%M:%S")
    snapshot = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    url = (
        f"{FDSN}?format=geojson&starttime={start}"
        f"&minlatitude={BBOX['lat_min']}&maxlatitude={BBOX['lat_max']}"
        f"&minlongitude={BBOX['lon_min']}&maxlongitude={BBOX['lon_max']}"
    )
    if DRY_RUN:
        url += "&limit=50"
    log.info("Fetching %s", url)
    raw_path = f"{OUT}/_raw.geojson"
    fetch_to_file(url, raw_path)

    db = duckdb.connect()
    db.execute("INSTALL spatial; LOAD spatial;")
    db.execute("INSTALL h3 FROM community; LOAD h3;")
    db.execute("SET geometry_always_xy = true;")

    out_path = f"{OUT}/events.parquet"
    db.execute(f"""
        COPY (
            WITH raw AS (
                SELECT unnest(features) AS f
                FROM read_json_auto('{raw_path}', maximum_object_size=134217728)
            )
            SELECT
                f.id AS event_id,
                f.properties.mag::DOUBLE AS mag,
                f.properties.magType AS mag_type,
                f.properties.place AS place,
                f.properties.type AS event_type,
                f.properties.status AS status,
                f.properties.tsunami::INTEGER AS tsunami,
                f.properties.sig::INTEGER AS sig,
                f.properties.felt::INTEGER AS felt,
                f.properties.cdi::DOUBLE AS cdi,
                f.properties.mmi::DOUBLE AS mmi,
                f.properties.alert AS alert,
                f.properties.net AS net,
                f.properties.code AS code,
                f.properties.ids AS ids,
                f.properties.sources AS sources,
                f.properties.nst::INTEGER AS nst,
                f.properties.dmin::DOUBLE AS dmin,
                f.properties.rms::DOUBLE AS rms,
                f.properties.gap::DOUBLE AS gap,
                f.properties.title AS title,
                f.properties.url AS url,
                epoch_ms(f.properties.time::BIGINT) AS event_time,
                epoch_ms(f.properties.updated::BIGINT) AS updated_time,
                f.geometry.coordinates[1]::DOUBLE AS longitude,
                f.geometry.coordinates[2]::DOUBLE AS latitude,
                f.geometry.coordinates[3]::DOUBLE AS depth_km,
                h3_latlng_to_cell(
                    f.geometry.coordinates[2]::DOUBLE,
                    f.geometry.coordinates[1]::DOUBLE, 5
                )::UBIGINT AS h3_r5,
                h3_latlng_to_cell(
                    f.geometry.coordinates[2]::DOUBLE,
                    f.geometry.coordinates[1]::DOUBLE, 8
                )::UBIGINT AS h3_r8,
                '{snapshot}'::TIMESTAMP AS snapshot_time,
                ST_SetCRS(
                    ST_Point(
                        f.geometry.coordinates[1]::DOUBLE,
                        f.geometry.coordinates[2]::DOUBLE
                    ), 'EPSG:4326'
                ) AS geometry
            FROM raw
            ORDER BY ST_Hilbert(
                ST_Point(
                    f.geometry.coordinates[1]::DOUBLE,
                    f.geometry.coordinates[2]::DOUBLE
                )
            ), event_time
        ) TO '{out_path}' (
            FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 15,
            ROW_GROUP_SIZE 100000, GEOPARQUET_VERSION 'V2'
        )
    """)
    n = db.execute(f"SELECT COUNT(*) FROM read_parquet('{out_path}')").fetchone()[0]
    size_kb = os.path.getsize(out_path) / 1024
    log.info("Wrote %s (%d rows, %.1f KB)", out_path, n, size_kb)

    os.unlink(raw_path)
    db.close()
    log.info("%s complete (%.1fs)", mode, time.monotonic() - t0)


if __name__ == "__main__":
    main()
