"""Extract Walkthru.Earth H3 res-5 indices for SA + Gulf.

Seven source Parquets on source.coop, three static and four Overture-based:

  static (one-time v2 drop, no release partition):
    - population_h3r5         WorldPop SSP2 2025-2100 projections
    - buildings_h3r5          Global Building Atlas (legacy footprint index)
    - terrain_h3r5            GEDTM-30m elevation derivatives

  Overture monthly (release=YYYY-MM-DD.N, we pick the latest):
    - base_h3r5               infrastructure + land use + water counts
    - buildings_overture_h3r5 Overture buildings with class breakdown
    - places_h3r5             Overture POIs with category breakdown
    - transportation_h3r5     Overture roads/rail with class breakdown

Addresses-index is intentionally excluded: Overture has no Saudi Arabia
address coverage as of 2026-04 (city_index contains 40 non-SA countries
only), so it is unusable for our region.

Mode is "replace": we always publish the latest snapshot for both the static
tables and the most recent Overture release. Historical Overture releases
remain queryable on source.coop directly if needed.

Bbox clip: 25-62 deg E, 12-38 deg N (SA + Gulf region).
"""

import logging
import os
import time
import urllib.request
import xml.etree.ElementTree as ET

import duckdb

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    level=logging.DEBUG if os.environ.get("DRY_RUN") else logging.INFO,
)
log = logging.getLogger(__name__)

OUT = os.environ.get("OUTPUT_DIR", "output")
DRY_RUN = os.environ.get("DRY_RUN", "0") == "1"

BBOX = {"lat_min": 12.0, "lat_max": 38.0, "lon_min": 25.0, "lon_max": 62.0}

S3_LIST_HOST = "https://data.source.coop"
BUCKET = "walkthru-earth"
S3_NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"

STATIC_SOURCES = {
    "population_h3r5": {
        "url": (
            "https://data.source.coop/walkthru-earth/indices/"
            "population/v2/scenario=SSP2/h3_res=5/data.parquet"
        ),
        "cols": [
            "h3_index",
            "pop_2025", "pop_2030", "pop_2035", "pop_2040", "pop_2045",
            "pop_2050", "pop_2055", "pop_2060", "pop_2065", "pop_2070",
            "pop_2075", "pop_2080", "pop_2085", "pop_2090", "pop_2095",
            "pop_2100",
        ],
    },
    "buildings_h3r5": {
        "url": (
            "https://data.source.coop/walkthru-earth/indices/"
            "building/v2/h3/h3_res=5/data.parquet"
        ),
        "cols": [
            "h3_index",
            "building_count", "building_density",
            "total_footprint_m2", "coverage_ratio",
            "avg_height_m", "max_height_m", "height_std_m",
            "total_volume_m3", "volume_density_m3_per_km2",
            "avg_footprint_m2",
        ],
    },
    "terrain_h3r5": {
        "url": (
            "https://data.source.coop/walkthru-earth/"
            "dem-terrain/v2/h3/h3_res=5/data.parquet"
        ),
        "cols": ["h3_index", "elev", "slope", "aspect", "tri", "tpi"],
    },
}

OVERTURE_INDICES = {
    "base_h3r5": "indices/base-index/v1",
    "buildings_overture_h3r5": "indices/buildings-index/v1",
    "places_h3r5": "indices/places-index/v1",
    "transportation_h3r5": "indices/transportation-index/v1",
}


def latest_overture_release(prefix):
    """List release=... partitions under a prefix and return the newest.

    Uses source.coop's S3-compatible bucket listing with delimiter=/ to get
    only immediate sub-prefixes, then picks max() lexically since walkthru
    uses zero-padded YYYY-MM-DD.N naming.
    """
    url = (
        f"{S3_LIST_HOST}/{BUCKET}/?list-type=2"
        f"&prefix={prefix}/&delimiter=/"
    )
    log.debug("Listing %s", url)
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "ai-data-registry/walkthru-indices"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        xml = resp.read()
    root = ET.fromstring(xml)
    releases = []
    for cp in root.findall(f"{S3_NS}CommonPrefixes"):
        p = cp.findtext(f"{S3_NS}Prefix", "")
        # Expect "indices/<name>/v1/release=YYYY-MM-DD.N/"
        if "release=" in p:
            releases.append(p.rstrip("/").rsplit("release=", 1)[1])
    if not releases:
        raise RuntimeError(f"no release=... partitions found under {prefix}/")
    latest = max(releases)
    log.info("  latest release for %s: %s (out of %d)", prefix, latest, len(releases))
    return latest


def main():
    t0 = time.monotonic()
    mode = "dry-run" if DRY_RUN else "extract"
    log.info("Starting %s, output_dir=%s", mode, OUT)
    os.makedirs(OUT, exist_ok=True)

    db = duckdb.connect()
    db.execute("INSTALL httpfs; LOAD httpfs;")
    db.execute("INSTALL h3 FROM community; LOAD h3;")

    from datetime import datetime, timezone
    snapshot = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    limit_clause = "LIMIT 500" if DRY_RUN else ""

    # Static tables: single fixed URL, pass-through columns
    for table_name, cfg in STATIC_SOURCES.items():
        out_path = f"{OUT}/{table_name}.parquet"
        cols_list = ", ".join(cfg["cols"])
        log.info("Extracting %s from %s...", table_name, cfg["url"])
        t1 = time.monotonic()
        db.execute(f"""
            COPY (
                SELECT
                    {cols_list},
                    h3_cell_to_lat(h3_index) AS lat,
                    h3_cell_to_lng(h3_index) AS lng,
                    '{snapshot}'::TIMESTAMP AS snapshot_time
                FROM read_parquet('{cfg["url"]}')
                WHERE h3_cell_to_lat(h3_index) BETWEEN {BBOX['lat_min']} AND {BBOX['lat_max']}
                  AND h3_cell_to_lng(h3_index) BETWEEN {BBOX['lon_min']} AND {BBOX['lon_max']}
                ORDER BY h3_index
                {limit_clause}
            ) TO '{out_path}' (
                FORMAT PARQUET,
                COMPRESSION ZSTD,
                COMPRESSION_LEVEL 15,
                ROW_GROUP_SIZE 100000
            )
        """)
        n = db.execute(
            f"SELECT COUNT(*) FROM read_parquet('{out_path}')"
        ).fetchone()[0]
        size_mb = os.path.getsize(out_path) / 1024 / 1024
        log.info("  %s: %d rows, %.2f MB (%.1fs)",
                 table_name, n, size_mb, time.monotonic() - t1)

    # Overture tables: resolve latest release per index, then read that file
    for table_name, prefix in OVERTURE_INDICES.items():
        t1 = time.monotonic()
        release = latest_overture_release(prefix)
        url = (
            f"https://data.source.coop/{BUCKET}/{prefix}/"
            f"release={release}/h3/h3_res=5/data.parquet"
        )
        out_path = f"{OUT}/{table_name}.parquet"
        log.info("Extracting %s from %s...", table_name, url)
        # SELECT * EXCLUDE (h3_res) keeps schema lean; h3_res=5 is implicit
        db.execute(f"""
            COPY (
                SELECT
                    * EXCLUDE (h3_res, release),
                    h3_cell_to_lat(h3_index) AS lat,
                    h3_cell_to_lng(h3_index) AS lng,
                    '{release}' AS overture_release,
                    '{snapshot}'::TIMESTAMP AS snapshot_time
                FROM read_parquet('{url}')
                WHERE h3_cell_to_lat(h3_index) BETWEEN {BBOX['lat_min']} AND {BBOX['lat_max']}
                  AND h3_cell_to_lng(h3_index) BETWEEN {BBOX['lon_min']} AND {BBOX['lon_max']}
                ORDER BY h3_index
                {limit_clause}
            ) TO '{out_path}' (
                FORMAT PARQUET,
                COMPRESSION ZSTD,
                COMPRESSION_LEVEL 15,
                ROW_GROUP_SIZE 100000
            )
        """)
        n = db.execute(
            f"SELECT COUNT(*) FROM read_parquet('{out_path}')"
        ).fetchone()[0]
        size_mb = os.path.getsize(out_path) / 1024 / 1024
        log.info("  %s: %d rows, %.2f MB (%.1fs)",
                 table_name, n, size_mb, time.monotonic() - t1)

    db.close()
    log.info("%s complete (%.1fs)", mode, time.monotonic() - t0)


if __name__ == "__main__":
    main()
