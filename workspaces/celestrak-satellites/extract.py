"""Extract CelesTrak satellite catalogs and compute approximate sub-satellite points.

Pulls 7 priority groups (stations, weather, geo, gnss, military, sarsat,
resource). Deduplicates by NORAD_CAT_ID across groups (favors first group seen).
Computes a simplified sub-satellite point (lat, lon, altitude) from mean motion
and orbital elements, suitable for visualization and regional overflight checks
but NOT for precision tracking.

Global catalog (no bbox filter). H3 r5 on the approximate sub-point, which is
the coarsest useful resolution given the ~km-scale propagation error inherent
in the simplified formula. Any finer H3 resolution would be misleading.
Append mode keyed on (norad_cat_id, snapshot_time).
"""

import json
import logging
import math
import os
import time
import urllib.error
import urllib.request
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

CELESTRAK_BASE = "https://celestrak.org/NORAD/elements/gp.php"
GROUPS = [
    ("stations", "Space Stations"),
    ("weather", "Weather Satellites"),
    ("geo", "Geostationary Satellites"),
    ("gnss", "Navigation (GNSS)"),
    ("military", "Military Satellites"),
    ("sarsat", "Search & Rescue (SARSAT)"),
    ("resource", "Earth Resources"),
]

EARTH_RADIUS_KM = 6371.0
EARTH_MU = 398600.4418
SIDEREAL_DAY_S = 86164.0905

# Region overflight check (SA + Gulf bbox, wider for orbital context)
REGION_BBOX = {"min_lon": 25.0, "max_lon": 62.0, "min_lat": 12.0, "max_lat": 38.0}


def fetch_group(group):
    url = f"{CELESTRAK_BASE}?GROUP={group}&FORMAT=json"
    req = urllib.request.Request(
        url, headers={"User-Agent": "ai-data-registry/celestrak-satellites"}
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read())
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
        log.exception("Failed to fetch group %s", group)
        return []


def compute_subsatellite(sat, now_utc):
    """Simplified sub-satellite point from orbital elements.

    Returns (lat, lon, altitude_km) or (None, None, None) if computation fails.
    Uses mean motion + epoch; accurate to roughly tens of km for short time
    windows, which is sufficient for a watch-center "approximate ground track"
    visualization but NOT for precision overflight prediction.
    """
    epoch_str = sat.get("EPOCH")
    if not epoch_str:
        return None, None, None

    try:
        epoch = datetime.fromisoformat(epoch_str.replace("Z", "+00:00"))
        if epoch.tzinfo is None:
            epoch = epoch.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None, None, None

    mean_motion = sat.get("MEAN_MOTION") or 0
    inclination = sat.get("INCLINATION") or 0
    raan = sat.get("RA_OF_ASC_NODE") or 0
    mean_anomaly = sat.get("MEAN_ANOMALY") or 0

    if mean_motion <= 0:
        return None, None, None

    period_s = 86400.0 / mean_motion
    sma = (EARTH_MU * (period_s / (2 * math.pi)) ** 2) ** (1 / 3)
    altitude_km = sma - EARTH_RADIUS_KM

    dt = (now_utc - epoch).total_seconds()
    current_ma = (mean_anomaly + 360.0 * mean_motion * dt / 86400.0) % 360.0
    arg_of_lat = current_ma

    lat = math.degrees(
        math.asin(
            math.sin(math.radians(inclination))
            * math.sin(math.radians(arg_of_lat))
        )
    )
    earth_rot_deg = (dt % SIDEREAL_DAY_S) / SIDEREAL_DAY_S * 360.0
    lon = (raan + arg_of_lat - earth_rot_deg) % 360.0
    if lon > 180:
        lon -= 360

    return round(lat, 4), round(lon, 4), round(altitude_km, 1)


def main():
    t0 = time.monotonic()
    mode = "dry-run" if DRY_RUN else "extract"
    log.info("Starting %s, output_dir=%s", mode, OUT)
    os.makedirs(OUT, exist_ok=True)
    out_path = f"{OUT}/satellites.parquet"

    now = datetime.now(timezone.utc)
    snapshot = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    log.info("Snapshot: %s", snapshot)

    rows = []
    seen_ids = set()
    groups = GROUPS[:2] if DRY_RUN else GROUPS
    for group, label in groups:
        log.info("Fetching group: %s (%s)", group, label)
        data = fetch_group(group)
        n_new = 0
        for sat in data:
            nid = sat.get("NORAD_CAT_ID")
            if nid is None or nid in seen_ids:
                continue
            seen_ids.add(nid)
            lat, lon, alt = compute_subsatellite(sat, now)
            rows.append({
                "norad_cat_id": nid,
                "object_name": sat.get("OBJECT_NAME") or "",
                "object_id": sat.get("OBJECT_ID") or "",
                "group": group,
                "group_label": label,
                "epoch": sat.get("EPOCH") or "",
                "mean_motion": sat.get("MEAN_MOTION"),
                "eccentricity": sat.get("ECCENTRICITY"),
                "inclination": sat.get("INCLINATION"),
                "ra_of_asc_node": sat.get("RA_OF_ASC_NODE"),
                "arg_of_pericenter": sat.get("ARG_OF_PERICENTER"),
                "mean_anomaly": sat.get("MEAN_ANOMALY"),
                "classification": sat.get("CLASSIFICATION_TYPE") or "U",
                "bstar": sat.get("BSTAR"),
                "rev_at_epoch": sat.get("REV_AT_EPOCH"),
                "latitude_approx": lat,
                "longitude_approx": lon,
                "altitude_km": alt,
                "snapshot_time": snapshot,
            })
            n_new += 1
        log.info("  %s: +%d unique satellites (total so far: %d)", group, n_new, len(rows))

    log.info("Total unique satellites: %d", len(rows))
    if len(rows) == 0:
        log.error("No satellites fetched. Aborting.")
        raise SystemExit(1)

    db = duckdb.connect()
    db.execute("INSTALL spatial; LOAD spatial;")
    db.execute("INSTALL h3 FROM community; LOAD h3;")
    db.execute("SET geometry_always_xy = true;")

    db.execute("""
        CREATE TABLE raw (
            norad_cat_id BIGINT, object_name VARCHAR, object_id VARCHAR,
            "group" VARCHAR, group_label VARCHAR, epoch VARCHAR,
            mean_motion DOUBLE, eccentricity DOUBLE, inclination DOUBLE,
            ra_of_asc_node DOUBLE, arg_of_pericenter DOUBLE, mean_anomaly DOUBLE,
            classification VARCHAR, bstar DOUBLE, rev_at_epoch BIGINT,
            latitude_approx DOUBLE, longitude_approx DOUBLE, altitude_km DOUBLE,
            snapshot_time VARCHAR
        )
    """)
    placeholders = ", ".join(["?"] * 19)
    db.executemany(
        f"INSERT INTO raw VALUES ({placeholders})",
        [tuple(r.values()) for r in rows],
    )

    log.info("Writing %s...", out_path)
    bbox = REGION_BBOX
    db.execute(f"""
        COPY (
            SELECT
                norad_cat_id, object_name, object_id,
                "group", group_label,
                epoch::TIMESTAMP AS epoch,
                mean_motion, eccentricity, inclination,
                ra_of_asc_node, arg_of_pericenter, mean_anomaly,
                classification, bstar, rev_at_epoch,
                latitude_approx, longitude_approx, altitude_km,
                CASE
                    WHEN latitude_approx IS NULL OR longitude_approx IS NULL THEN NULL
                    ELSE h3_latlng_to_cell(latitude_approx, longitude_approx, 5)::UBIGINT
                END AS h3_r5,
                (longitude_approx BETWEEN {bbox['min_lon']} AND {bbox['max_lon']}
                 AND latitude_approx BETWEEN {bbox['min_lat']} AND {bbox['max_lat']}
                ) AS over_region,
                snapshot_time::TIMESTAMP AS snapshot_time,
                CASE
                    WHEN latitude_approx IS NULL OR longitude_approx IS NULL THEN NULL
                    ELSE ST_SetCRS(ST_Point(longitude_approx, latitude_approx), 'EPSG:4326')
                END AS geometry
            FROM raw
            ORDER BY
                CASE WHEN latitude_approx IS NULL THEN 1 ELSE 0 END,
                ST_Hilbert(
                    ST_Point(
                        COALESCE(longitude_approx, 0),
                        COALESCE(latitude_approx, 0)
                    )
                ),
                norad_cat_id
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
    over_region = db.execute(
        f"SELECT COUNT(*) FROM read_parquet('{out_path}') WHERE over_region"
    ).fetchone()[0]
    size_kb = os.path.getsize(out_path) / 1024
    log.info("Wrote %s (%d rows, %d over region, %.1f KB)",
             out_path, final, over_region, size_kb)

    db.close()
    elapsed = time.monotonic() - t0
    log.info("%s complete: %d rows in %.1fs", mode, final, elapsed)


if __name__ == "__main__":
    main()
