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
        ("cables", 10, "feature_id"),
        ("landing_points", 10, "lp_id"),
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

    # cables total length in ME bbox (km, geodesic-ish approximation)
    total_km = db.execute(f"""
        SELECT SUM(ST_Length_Spheroid(geometry))/1000 FROM read_parquet('{OUT}/cables.parquet')
    """).fetchone()[0]
    log.info("cables total length in AOI: %.0f km", total_km or 0)

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
