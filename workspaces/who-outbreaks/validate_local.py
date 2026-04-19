"""Validate WHO outbreaks output."""

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


def main():
    t0 = time.monotonic()
    path = f"{OUT}/outbreaks.parquet"
    if not os.path.exists(path):
        raise SystemExit(f"missing {path}")

    db = duckdb.connect()
    n = db.execute(f"SELECT COUNT(*) FROM read_parquet('{path}')").fetchone()[0]
    threshold = 100 if DRY_RUN else 1000
    log.info("outbreaks: %d rows (min %d)", n, threshold)
    assert n >= threshold, f"rows: {n} < {threshold}"

    cols = {c[0] for c in db.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{path}')"
    ).fetchall()}
    required = {"id", "title", "publication_time", "me_countries",
                "is_me_related", "snapshot_time"}
    missing = required - cols
    assert not missing, f"missing columns: {missing}"

    dup = db.execute(f"""
        SELECT COUNT(*) FROM (
            SELECT id FROM read_parquet('{path}') GROUP BY id HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    assert dup == 0, f"{dup} duplicate id"

    n_me = db.execute(
        f"SELECT COUNT(*) FROM read_parquet('{path}') WHERE is_me_related"
    ).fetchone()[0]
    log.info("ME-related: %d (%.1f%%)", n_me, 100 * n_me / n)

    top_me = db.execute(f"""
        SELECT me_countries, COUNT(*)
        FROM read_parquet('{path}')
        WHERE is_me_related
        GROUP BY me_countries ORDER BY 2 DESC LIMIT 10
    """).fetchall()
    log.info("top ME country-sets: %s",
             ", ".join(f"{c}={n}" for c, n in top_me))

    date_range = db.execute(
        f"SELECT MIN(publication_time), MAX(publication_time) FROM read_parquet('{path}')"
    ).fetchone()
    log.info("publication range: %s .. %s", *date_range)

    log.info("Validation passed (%.1fs)", time.monotonic() - t0)


if __name__ == "__main__":
    main()
