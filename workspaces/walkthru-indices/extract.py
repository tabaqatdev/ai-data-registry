"""Extract Walkthru.Earth indices (population, buildings, terrain) for SA + Gulf.

Three source Parquet files on source.coop (HTTPS), each already H3-indexed at
res 5 globally. We apply a bbox filter (25-62°E, 12-38°N) on `h3_cell_to_lat/lng`
and write flat H3-keyed tables with no geometry column. Consumers reconstruct
geometry from the cell ID via `h3_cell_to_boundary_wkt()` at query time.

Mode `replace`: upstream publishes static releases (quarterly at most).
Historical walkthru versions remain queryable on source.coop directly if ever
needed.
"""

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
DRY_RUN = os.environ.get("DRY_RUN", "0") == "1"

BBOX = {"lat_min": 12.0, "lat_max": 38.0, "lon_min": 25.0, "lon_max": 62.0}

SOURCES = {
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

    for table_name, cfg in SOURCES.items():
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
        log.info(
            "  %s: %d rows, %.2f MB (%.1fs)",
            table_name, n, size_mb, time.monotonic() - t1,
        )

    db.close()
    elapsed = time.monotonic() - t0
    log.info("%s complete (%.1fs)", mode, elapsed)


if __name__ == "__main__":
    main()
