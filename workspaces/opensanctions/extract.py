"""Extract OpenSanctions targets filtered to the SA influence region.

Reads `targets.simple.csv` (the consolidated default dataset, ~435 MB) directly
over HTTPS via DuckDB, filters to entities linked to 18 region country codes,
enriches with derived fields (entity_category, country_list, sanction_list),
and writes a single flat Parquet file.

Non-spatial workspace. No geometry, no H3. `mode = "append"` with a `snapshot_date`
column so DuckLake retains a full historical trail of daily snapshots. Unique key:
`(id, snapshot_date)`. Each daily run uploads a new timestamped Parquet file.
Consumers can reconstruct "state as of day X" by filtering on `snapshot_date`.
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

SOURCE_URL = "https://data.opensanctions.org/datasets/latest/default/targets.simple.csv"
OUT = os.environ.get("OUTPUT_DIR", "output")
DRY_RUN = os.environ.get("DRY_RUN", "0") == "1"

REGION_CODES = [
    "sa", "ir", "iq", "eg", "sy", "ye", "jo", "lb", "ps", "il",
    "ae", "kw", "bh", "qa", "om", "ru", "kp", "af",
]


def main():
    t0 = time.monotonic()
    mode = "dry-run" if DRY_RUN else "extract"
    log.info("Starting %s, output_dir=%s", mode, OUT)

    os.makedirs(OUT, exist_ok=True)
    out_path = f"{OUT}/region_sanctions.parquet"

    db = duckdb.connect()
    db.execute("INSTALL httpfs; LOAD httpfs;")

    now_utc = datetime.now(timezone.utc)
    snapshot = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    snapshot_date = now_utc.strftime("%Y-%m-%d")
    log.info("Snapshot timestamp: %s (date: %s)", snapshot, snapshot_date)

    dry_run_limit = "LIMIT 500" if DRY_RUN else ""

    codes_sql = ", ".join(f"'{c}'" for c in REGION_CODES)
    log.info("Reading upstream CSV (%s) and filtering to %d region codes...",
             SOURCE_URL, len(REGION_CODES))

    db.execute(f"""
        CREATE TABLE raw AS
        SELECT *
        FROM read_csv(
            '{SOURCE_URL}',
            auto_detect=true,
            header=true,
            quote='"',
            escape='"'
        )
        WHERE countries IS NOT NULL
          AND EXISTS (
              SELECT 1
              FROM (SELECT unnest([{codes_sql}]) AS code) rc
              WHERE lower(countries) LIKE '%' || rc.code || '%'
          )
        {dry_run_limit}
    """)

    raw_count = db.execute("SELECT COUNT(*) FROM raw").fetchone()[0]
    log.info("Region-filtered rows: %d", raw_count)
    if raw_count == 0:
        log.error("No rows matched region filter. Upstream schema may have changed.")
        raise SystemExit(1)

    log.info("Writing %s...", out_path)
    db.execute(f"""
        COPY (
            SELECT
                id,
                schema AS entity_type,
                name,
                aliases,
                birth_date,
                countries,
                addresses,
                identifiers,
                sanctions,
                phones,
                emails,
                program_ids,
                dataset,
                first_seen,
                last_seen,
                last_change,
                string_split(countries, ';') AS country_list,
                string_split(sanctions, ';') AS sanction_list,
                CASE
                    WHEN schema = 'Person' THEN 'person'
                    WHEN schema IN ('Company', 'PublicBody') THEN 'company'
                    WHEN schema = 'Organization' THEN 'organization'
                    WHEN schema IN ('Vessel', 'Airplane') THEN 'asset'
                    ELSE 'other'
                END AS entity_category,
                '{snapshot}'::TIMESTAMP AS snapshot_time,
                '{snapshot_date}'::DATE AS snapshot_date
            FROM raw
            ORDER BY last_change DESC NULLS LAST
        ) TO '{out_path}' (
            FORMAT PARQUET,
            COMPRESSION ZSTD,
            COMPRESSION_LEVEL 15,
            ROW_GROUP_SIZE 100000
        )
    """)

    final_count = db.execute(
        f"SELECT COUNT(*) FROM read_parquet('{out_path}')"
    ).fetchone()[0]
    file_bytes = os.path.getsize(out_path)
    log.info(
        "Wrote %s (%d rows, %.1f MB)",
        out_path, final_count, file_bytes / 1024 / 1024,
    )

    db.close()
    elapsed = time.monotonic() - t0
    log.info("%s complete: %d rows in %.1fs", mode, final_count, elapsed)


if __name__ == "__main__":
    main()
