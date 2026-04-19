"""Validate submarine cables output."""

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


def main():
    t0 = time.monotonic()
    db = duckdb.connect()
    db.execute("INSTALL spatial; LOAD spatial;")

    for table, min_rows, key in (
        ("cables", 500, "feature_id"),
        ("landing_points", 1000, "lp_id"),
    ):
        path = f"{OUT}/{table}.parquet"
        if not os.path.exists(path):
            raise SystemExit(f"missing {path}")
        n = db.execute(f"SELECT COUNT(*) FROM read_parquet('{path}')").fetchone()[0]
        log.info("%s: %d rows", table, n)
        assert n >= min_rows, f"{table}: {n} < {min_rows}"

        dup = db.execute(
            f"""SELECT COUNT(*) FROM (
                SELECT {key} FROM read_parquet('{path}')
                GROUP BY {key} HAVING COUNT(*) > 1
            )"""
        ).fetchone()[0]
        assert dup == 0, f"{table}: {dup} duplicate {key}"

        null_geom = db.execute(
            f"SELECT COUNT(*) FROM read_parquet('{path}') WHERE geometry IS NULL"
        ).fetchone()[0]
        assert null_geom == 0, f"{table}: {null_geom} null geometry"

    # global cable total length (km, spheroid); NaN from antimeridian-crossers
    # is ignored so the total is a lower bound.
    total_km, n_nan = db.execute(f"""
        SELECT
            SUM(CASE WHEN isfinite(ST_Length_Spheroid(geometry))
                     THEN ST_Length_Spheroid(geometry) END)/1000,
            SUM(CASE WHEN NOT isfinite(ST_Length_Spheroid(geometry)) THEN 1 ELSE 0 END)
        FROM read_parquet('{OUT}/cables.parquet')
    """).fetchone()
    log.info("global cables total length: %.0f km (%d antimeridian segments skipped)",
             total_km or 0, n_nan or 0)

    # landing points per country hint (simplified: use name suffix after last comma)
    sample = db.execute(f"""
        SELECT regexp_extract(lp_name, ', ([^,]+)$', 1) AS country, COUNT(*)
        FROM read_parquet('{OUT}/landing_points.parquet')
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10
    """).fetchall()
    log.info("landing points top countries: %s",
             ", ".join(f"{c}={n}" for c, n in sample if c))

    log.info("Validation passed (%.1fs)", time.monotonic() - t0)


if __name__ == "__main__":
    main()
