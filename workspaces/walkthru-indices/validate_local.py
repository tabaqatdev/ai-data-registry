"""Validate Walkthru.Earth indices Parquet outputs locally."""

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

TABLES = {
    "population_h3r5": {"min_full": 5000, "key_col": "pop_2025"},
    "buildings_h3r5": {"min_full": 5000, "key_col": "building_count"},
    "terrain_h3r5": {"min_full": 10000, "key_col": "elev"},
}


def main():
    t0 = time.monotonic()
    db = duckdb.connect()
    db.execute("INSTALL h3 FROM community; LOAD h3;")

    min_factor = 0.02 if DRY_RUN else 1.0

    for name, cfg in TABLES.items():
        path = f"{OUT}/{name}.parquet"
        if not os.path.exists(path):
            log.error("%s: missing at %s", name, path)
            raise SystemExit(1)

        n = db.execute(f"SELECT COUNT(*) FROM read_parquet('{path}')").fetchone()[0]
        threshold = int(cfg["min_full"] * min_factor)
        log.info("%s: %d rows (threshold >= %d)", name, n, threshold)
        assert n >= threshold, f"{name}: {n} < {threshold}"

        uniq = db.execute(
            f"SELECT COUNT(DISTINCT h3_index) FROM read_parquet('{path}')"
        ).fetchone()[0]
        assert uniq == n, f"{name}: h3_index not unique ({uniq}/{n})"

        cols = db.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()
        col_names = {c[0] for c in cols}
        for required in ("h3_index", "lat", "lng", "snapshot_time", cfg["key_col"]):
            assert required in col_names, f"{name}: missing {required}"

        key_stats = db.execute(f"""
            SELECT MIN({cfg['key_col']}), MAX({cfg['key_col']}), AVG({cfg['key_col']})
            FROM read_parquet('{path}')
        """).fetchone()
        log.info("  %s - min=%s max=%s avg=%s", cfg["key_col"], *key_stats)

    elapsed = time.monotonic() - t0
    log.info("Validation passed (%.1fs)", elapsed)


if __name__ == "__main__":
    main()
