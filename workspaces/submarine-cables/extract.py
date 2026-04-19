"""Extract TeleGeography submarine cables + landing points, clipped to ME.

Source: https://www.submarinecablemap.com/api/v3/{cable,landing-point}-geo.json
ME/Red Sea corridor bbox: 20-75 deg E, 5-45 deg N. Wider than Gulf-only so
that cables landing on the Mediterranean/Arabian coasts are both captured.

Cables are MultiLineString; we intersect with the bbox polygon so only the
segments touching the AOI are retained. Landing points are points; we filter
by coordinate.

Mode is "replace": TeleGeography publishes a single current snapshot, weekly
refresh is plenty.
"""

import logging
import os
import time
import urllib.request

import duckdb

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    level=logging.DEBUG if os.environ.get("DRY_RUN") else logging.INFO,
)
log = logging.getLogger(__name__)

OUT = os.environ.get("OUTPUT_DIR", "output")
DRY_RUN = os.environ.get("DRY_RUN", "0") == "1"

BBOX = {"lat_min": 5.0, "lat_max": 45.0, "lon_min": 20.0, "lon_max": 75.0}

CABLES_URL = "https://www.submarinecablemap.com/api/v3/cable/cable-geo.json"
LANDING_URL = "https://www.submarinecablemap.com/api/v3/landing-point/landing-point-geo.json"

USER_AGENT = "ai-data-registry/submarine-cables"
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

    cables_path = f"{OUT}/_cables.geojson"
    lp_path = f"{OUT}/_landing_points.geojson"
    log.info("Fetching cables...")
    fetch_to_file(CABLES_URL, cables_path)
    log.info("Fetching landing points...")
    fetch_to_file(LANDING_URL, lp_path)

    from datetime import datetime, timezone
    snapshot = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    db = duckdb.connect()
    db.execute("INSTALL spatial; LOAD spatial;")
    db.execute("INSTALL h3 FROM community; LOAD h3;")
    db.execute("SET geometry_always_xy = true;")

    bbox_wkt = (
        f"POLYGON(({BBOX['lon_min']} {BBOX['lat_min']}, "
        f"{BBOX['lon_max']} {BBOX['lat_min']}, "
        f"{BBOX['lon_max']} {BBOX['lat_max']}, "
        f"{BBOX['lon_min']} {BBOX['lat_max']}, "
        f"{BBOX['lon_min']} {BBOX['lat_min']}))"
    )

    # Cables: MultiLineString, intersect with bbox to keep only ME segments
    out_cables = f"{OUT}/cables.parquet"
    db.execute(f"""
        COPY (
            WITH raw AS (
                SELECT unnest(features) AS f
                FROM read_json_auto('{cables_path}', maximum_object_size=134217728)
            ),
            geom AS (
                SELECT
                    f.properties.feature_id AS feature_id,
                    f.properties.id AS cable_id,
                    f.properties.name AS cable_name,
                    f.properties.color AS color,
                    ST_SetCRS(ST_GeomFromGeoJSON(to_json(f.geometry)), 'EPSG:4326') AS g
                FROM raw
            )
            SELECT
                feature_id, cable_id, cable_name, color,
                '{snapshot}'::TIMESTAMP AS snapshot_time,
                ST_Intersection(g, ST_GeomFromText('{bbox_wkt}')) AS geometry
            FROM geom
            WHERE ST_Intersects(g, ST_GeomFromText('{bbox_wkt}'))
            ORDER BY ST_Hilbert(ST_Centroid(g))
        ) TO '{out_cables}' (
            FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 15,
            ROW_GROUP_SIZE 100000, GEOPARQUET_VERSION 'V2'
        )
    """)
    n_cables = db.execute(
        f"SELECT COUNT(*) FROM read_parquet('{out_cables}')"
    ).fetchone()[0]
    log.info("cables.parquet: %d rows", n_cables)

    # Landing points: point features, bbox filter
    out_lp = f"{OUT}/landing_points.parquet"
    db.execute(f"""
        COPY (
            WITH raw AS (
                SELECT unnest(features) AS f
                FROM read_json_auto('{lp_path}', maximum_object_size=134217728)
            )
            SELECT
                f.properties.id AS lp_id,
                f.properties.name AS lp_name,
                f.geometry.coordinates[1]::DOUBLE AS lon,
                f.geometry.coordinates[2]::DOUBLE AS lat,
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
            WHERE f.geometry.coordinates[2]::DOUBLE BETWEEN {BBOX['lat_min']} AND {BBOX['lat_max']}
              AND f.geometry.coordinates[1]::DOUBLE BETWEEN {BBOX['lon_min']} AND {BBOX['lon_max']}
            ORDER BY ST_Hilbert(ST_Point(
                f.geometry.coordinates[1]::DOUBLE,
                f.geometry.coordinates[2]::DOUBLE
            ))
        ) TO '{out_lp}' (
            FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 15,
            ROW_GROUP_SIZE 100000, GEOPARQUET_VERSION 'V2'
        )
    """)
    n_lp = db.execute(
        f"SELECT COUNT(*) FROM read_parquet('{out_lp}')"
    ).fetchone()[0]
    log.info("landing_points.parquet: %d rows", n_lp)

    os.unlink(cables_path)
    os.unlink(lp_path)
    db.close()
    log.info("%s complete (%.1fs)", mode, time.monotonic() - t0)


if __name__ == "__main__":
    main()
