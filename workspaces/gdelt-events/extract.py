"""Extract GDELT events for Saudi Arabia from the tabaqat/gdelt-sa Source Coop bucket.

Reads hive-partitioned Parquet from the public S3 bucket, filters to the last
7 days, drops `ArticleContent` / `ArticleTitle` / `ArticleAuthor` to avoid
third-party publisher copyright (URLs only), adds H3 r5/r8/r10 on
`ActionGeo_Lat/Long`, and writes a single flat GeoParquet 2.0 file.

Append mode keyed on `(globaleventid, snapshot_date)`. Each daily run produces
a new Parquet file, DuckLake retains the full history.
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

SOURCE_PARQUET = (
    "s3://us-west-2.opendata.source.coop/tabaqat/gdelt-sa/"
    "country=SA/year=*/*.parquet"
)
# Upstream publishes with ~10 day lag. 30-day window ensures each run
# catches the freshest available data.
DAYS_BACK = 30


def main():
    t0 = time.monotonic()
    mode = "dry-run" if DRY_RUN else "extract"
    log.info("Starting %s, output_dir=%s", mode, OUT)

    os.makedirs(OUT, exist_ok=True)
    out_path = f"{OUT}/events.parquet"

    now = datetime.now(timezone.utc)
    snapshot = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    snapshot_date = now.strftime("%Y-%m-%d")
    log.info("Snapshot: %s (date: %s)", snapshot, snapshot_date)

    db = duckdb.connect()
    db.execute("INSTALL httpfs; LOAD httpfs;")
    db.execute("INSTALL spatial; LOAD spatial;")
    db.execute("INSTALL h3 FROM community; LOAD h3;")
    db.execute("SET geometry_always_xy = true;")
    db.execute("SET s3_region='us-west-2';")
    db.execute("SET s3_url_style='path';")

    log.info("Reading from %s (last %d days)...", SOURCE_PARQUET, DAYS_BACK)

    limit_clause = "LIMIT 200" if DRY_RUN else ""

    try:
        db.execute(f"""
            CREATE TABLE raw AS
            SELECT *
            FROM read_parquet('{SOURCE_PARQUET}', hive_partitioning=1)
            WHERE SQLDATE >= CAST(
                strftime(CURRENT_DATE - INTERVAL {DAYS_BACK} DAY, '%Y%m%d') AS INTEGER
            )
            {limit_clause}
        """)
    except duckdb.Error:
        log.exception("Failed to read upstream parquet. Check Source Coop availability.")
        raise SystemExit(1)

    n_raw = db.execute("SELECT COUNT(*) FROM raw").fetchone()[0]
    log.info("Fetched %d raw events", n_raw)
    if n_raw == 0:
        log.warning("Zero events returned from upstream. Writing empty file.")

    log.info("Writing %s (H3 r5/r8/r10, Hilbert sort)...", out_path)
    db.execute(f"""
        COPY (
            SELECT
                GLOBALEVENTID AS globaleventid,
                SQLDATE AS sqldate,
                strptime(SQLDATE::VARCHAR, '%Y%m%d')::DATE AS event_date,
                DATEADDED AS dateadded,
                FractionDate AS fraction_date,
                Actor1Code AS actor1_code,
                Actor1Name AS actor1_name,
                Actor1CountryCode AS actor1_country,
                Actor1Type1Code AS actor1_type,
                Actor2Code AS actor2_code,
                Actor2Name AS actor2_name,
                Actor2CountryCode AS actor2_country,
                Actor2Type1Code AS actor2_type,
                IsRootEvent::BOOLEAN AS is_root_event,
                EventCode AS event_code,
                EventBaseCode AS event_base_code,
                EventRootCode AS event_root_code,
                QuadClass AS quad_class,
                GoldsteinScale AS goldstein_scale,
                NumMentions AS num_mentions,
                NumSources AS num_sources,
                NumArticles AS num_articles,
                AvgTone AS avg_tone,
                ActionGeo_FullName AS geo_name,
                ActionGeo_CountryCode AS geo_country,
                ActionGeo_ADM1Code AS geo_adm1,
                ActionGeo_Lat AS latitude,
                ActionGeo_Long AS longitude,
                SOURCEURL AS source_url,
                quality_score,
                url_rank,
                NearestCity AS nearest_city,
                CityPopulation AS city_population,
                DistanceToCity_km AS distance_to_city_km,
                CoordQuality AS coord_quality,
                country,
                h3_latlng_to_cell(ActionGeo_Lat, ActionGeo_Long, 5)::UBIGINT AS h3_r5,
                h3_latlng_to_cell(ActionGeo_Lat, ActionGeo_Long, 8)::UBIGINT AS h3_r8,
                h3_latlng_to_cell(ActionGeo_Lat, ActionGeo_Long, 10)::UBIGINT AS h3_r10,
                '{snapshot}'::TIMESTAMP AS snapshot_time,
                '{snapshot_date}'::DATE AS snapshot_date,
                ST_SetCRS(ST_Point(ActionGeo_Long, ActionGeo_Lat), 'EPSG:4326') AS geometry
            FROM raw
            WHERE ActionGeo_Lat IS NOT NULL
              AND ActionGeo_Long IS NOT NULL
            ORDER BY ST_Hilbert(ST_Point(ActionGeo_Long, ActionGeo_Lat)), SQLDATE
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
