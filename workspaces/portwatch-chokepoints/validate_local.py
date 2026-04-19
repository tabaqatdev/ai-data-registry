"""Validate PortWatch chokepoints output."""

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
    db = duckdb.connect()
    db.execute("INSTALL spatial; LOAD spatial;")

    for table, min_rows in (("chokepoints", 20), ("daily_transits", 100 if DRY_RUN else 1000)):
        path = f"{OUT}/{table}.parquet"
        if not os.path.exists(path):
            raise SystemExit(f"missing {path}")
        n = db.execute(f"SELECT COUNT(*) FROM read_parquet('{path}')").fetchone()[0]
        log.info("%s: %d rows (min %d)", table, n, min_rows)
        assert n >= min_rows, f"{table}: {n} < {min_rows}"
        cols = {c[0] for c in db.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{path}')"
        ).fetchall()}
        assert "geometry" in cols, f"{table}: no geometry column"
        assert "snapshot_time" in cols, f"{table}: no snapshot_time"
        assert "h3_r5" in cols, f"{table}: no h3_r5"

    # chokepoints uniqueness
    dup = db.execute(f"""
        SELECT COUNT(*) FROM (
            SELECT portid FROM read_parquet('{OUT}/chokepoints.parquet')
            GROUP BY portid HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    assert dup == 0, f"chokepoints: {dup} duplicate portid"

    # daily_transits: check ME coverage
    me = db.execute(f"""
        SELECT DISTINCT portname FROM read_parquet('{OUT}/daily_transits.parquet')
        ORDER BY portname
    """).fetchall()
    log.info("daily_transits ports: %s", ", ".join(r[0] for r in me))
    assert len(me) >= 1, "expected at least one ME chokepoint"

    date_range = db.execute(
        f"SELECT MIN(date), MAX(date) FROM read_parquet('{OUT}/daily_transits.parquet')"
    ).fetchone()
    log.info("daily_transits date range: %s .. %s", *date_range)

    log.info("Validation passed (%.1fs)", time.monotonic() - t0)


if __name__ == "__main__":
    main()
