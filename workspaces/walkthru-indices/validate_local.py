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

# key_col is one representative column we assert MIN/MAX/AVG on as a sanity check.
# min_full is the lower-bound row count for a full SA + Gulf extract.
TABLES = {
    "population_h3r5":         {"min_full": 5000,  "key_col": "pop_2025",      "overture": False},
    "buildings_h3r5":          {"min_full": 5000,  "key_col": "building_count", "overture": False},
    "terrain_h3r5":            {"min_full": 10000, "key_col": "elev",          "overture": False},
    "base_h3r5":               {"min_full": 5000,  "key_col": "infra_count",    "overture": True},
    "buildings_overture_h3r5": {"min_full": 5000,  "key_col": "building_count", "overture": True},
    "places_h3r5":             {"min_full": 3000,  "key_col": "place_count",    "overture": True},
    "transportation_h3r5":     {"min_full": 5000,  "key_col": "segment_count",  "overture": True},
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
        required = {"h3_index", "lat", "lng", "snapshot_time", cfg["key_col"]}
        if cfg["overture"]:
            required.add("overture_release")
        missing = required - col_names
        assert not missing, f"{name}: missing {missing}"

        key_stats = db.execute(f"""
            SELECT MIN({cfg['key_col']}), MAX({cfg['key_col']}), AVG({cfg['key_col']})
            FROM read_parquet('{path}')
        """).fetchone()
        log.info("  %s - min=%s max=%s avg=%s", cfg["key_col"], *key_stats)

        if cfg["overture"]:
            rel = db.execute(
                f"SELECT DISTINCT overture_release FROM read_parquet('{path}')"
            ).fetchall()
            assert len(rel) == 1, f"{name}: mixed overture_release values {rel}"
            log.info("  overture_release=%s", rel[0][0])

    log.info("Validation passed (%.1fs)", time.monotonic() - t0)


if __name__ == "__main__":
    main()
