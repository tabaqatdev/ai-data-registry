"""Extract OSM infrastructure for the SA + Gulf region using QuackOSM.

Pulls 11 infrastructure categories from Geofabrik PBF extracts, writes one
flat GeoParquet per category. Each feature gets H3 r8 + r10 on a
representative point (centroid for polygons, first vertex for lines).

Heavy workspace: full extract downloads the GCC states PBF (~250-400 MB),
takes 60-75 min on Hetzner cax21. DRY_RUN uses a 0.5° x 0.5° bbox around
Riyadh for a fast local smoke test.

Mode `replace` with timestamped filenames, S3 retains the history via the
extract workflow, DuckLake registers only the latest file. If point-in-time
history is needed later, switch to `append` + `snapshot_date`.
"""

import logging
import os
import time
from pathlib import Path

import duckdb
import quackosm as qosm
from shapely.geometry import box

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    level=logging.DEBUG if os.environ.get("DRY_RUN") else logging.INFO,
)
log = logging.getLogger(__name__)

OUT = Path(os.environ.get("OUTPUT_DIR", "output"))
DRY_RUN = os.environ.get("DRY_RUN", "0") == "1"

# SA + Gulf extended bbox (lng_min, lat_min, lng_max, lat_max)
FULL_BBOX = box(25.0, 12.0, 62.0, 38.0)
# Tiny Riyadh bbox for dry-run smoke testing (0.5° square)
DRY_BBOX = box(46.5, 24.5, 47.0, 25.0)

CATEGORIES = {
    "power_lines": {"power": ["line", "minor_line", "cable", "minor_cable"]},
    "power_plants": {"power": ["plant"]},
    "power_generators": {"power": ["generator"]},
    "substations": {"power": ["substation"]},
    "power_towers": {"power": ["tower", "pole", "portal"]},
    "switchgear": {
        "power": ["switch", "transformer", "compensator", "converter"],
    },
    "pipelines": {"man_made": ["pipeline"]},
    "petroleum_sites": {
        "industrial": [
            "oil", "gas", "refinery", "fracking",
            "oil_storage", "gas_storage", "petroleum_terminal",
        ],
    },
    "petroleum_wells": {
        "man_made": ["petroleum_well", "oil_well", "offshore_platform"],
    },
    "telecoms": {
        "communication": ["line", "cable"],
        "telecom": True,
        "man_made": ["mast", "communications_tower"],
        "tower:type": ["communication"],
        "building": ["data_center", "telephone_exchange"],
    },
    "water_infra": {
        "man_made": [
            "water_works", "desalination_plant", "wastewater_plant",
            "pumping_station", "water_tower", "water_well",
        ],
        "waterway": ["pressurised"],
    },
}


def extract_category(name, tags, geom_filter, snapshot):
    """Extract one category, write flat GeoParquet with H3 indexing."""
    raw_path = OUT / f"{name}.raw.parquet"
    out_path = OUT / f"{name}.parquet"
    log.info("Extracting %s (tags=%s)", name, tags)
    t0 = time.monotonic()

    try:
        gdf = qosm.convert_geometry_to_geodataframe(
            geometry_filter=geom_filter,
            tags_filter=tags,
            explode_tags=True,
            allow_uncovered_geometry=True,
        )
    except Exception:
        log.exception("QuackOSM extraction failed for %s", name)
        return 0

    n = len(gdf)
    log.info("  %s: %d features extracted in %.1fs", name, n, time.monotonic() - t0)

    if n == 0:
        # Still write a tiny empty-schema file so downstream tooling is happy
        empty_cols = ["geometry"]
        gdf = gdf.reindex(columns=empty_cols)

    gdf.to_parquet(raw_path, compression="zstd")

    db = duckdb.connect()
    db.execute("INSTALL spatial; LOAD spatial;")
    db.execute("INSTALL h3 FROM community; LOAD h3;")
    db.execute("SET geometry_always_xy = true;")

    db.execute(f"""
        COPY (
            WITH base AS (
                SELECT *
                FROM read_parquet('{raw_path}')
            ),
            enriched AS (
                SELECT
                    * EXCLUDE (geometry),
                    ST_SetCRS(geometry, 'EPSG:4326') AS geometry,
                    ST_PointOnSurface(geometry) AS rep_point,
                    '{snapshot}'::TIMESTAMP AS snapshot_time
                FROM base
            )
            SELECT
                * EXCLUDE (rep_point),
                h3_latlng_to_cell(ST_Y(rep_point), ST_X(rep_point), 8)::UBIGINT AS h3_r8,
                h3_latlng_to_cell(ST_Y(rep_point), ST_X(rep_point), 10)::UBIGINT AS h3_r10
            FROM enriched
            ORDER BY ST_Hilbert(rep_point)
        ) TO '{out_path}' (
            FORMAT PARQUET,
            COMPRESSION ZSTD,
            COMPRESSION_LEVEL 15,
            ROW_GROUP_SIZE 100000,
            GEOPARQUET_VERSION 'V2'
        )
    """)
    db.close()
    raw_path.unlink(missing_ok=True)
    return n


def main():
    t0 = time.monotonic()
    mode = "dry-run" if DRY_RUN else "extract"
    log.info("Starting %s, output_dir=%s", mode, OUT)
    OUT.mkdir(parents=True, exist_ok=True)

    from datetime import datetime, timezone
    snapshot = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    geom = DRY_BBOX if DRY_RUN else FULL_BBOX
    log.info("BBOX: %s", geom.bounds)
    log.info("Snapshot: %s", snapshot)

    summary = {}
    for name, tags in CATEGORIES.items():
        summary[name] = extract_category(name, tags, geom, snapshot)

    elapsed = time.monotonic() - t0
    log.info("%s summary (%.1fs):", mode, elapsed)
    for name, count in summary.items():
        log.info("  %s: %d", name, count)


if __name__ == "__main__":
    main()
