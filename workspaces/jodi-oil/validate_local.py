"""Validate JODI oil output."""

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
    path = f"{OUT}/monthly_flows.parquet"
    if not os.path.exists(path):
        raise SystemExit(f"missing {path}")

    db = duckdb.connect()
    n = db.execute(f"SELECT COUNT(*) FROM read_parquet('{path}')").fetchone()[0]
    threshold = 1000 if DRY_RUN else 50000
    log.info("monthly_flows: %d rows (min %d)", n, threshold)
    assert n >= threshold, f"rows: {n} < {threshold}"

    cols = {c[0] for c in db.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{path}')"
    ).fetchall()}
    required = {"ref_area", "time_period", "period_date", "energy_product",
                "flow_breakdown", "unit_measure", "obs_value", "assessment_code",
                "snapshot_time"}
    missing = required - cols
    assert not missing, f"missing columns: {missing}"

    # SA and a few neighbours must be present
    countries = [r[0] for r in db.execute(
        f"SELECT DISTINCT ref_area FROM read_parquet('{path}') ORDER BY 1"
    ).fetchall()]
    log.info("countries present: %s", ", ".join(countries))
    for must in ("SA", "IR", "IQ"):
        assert must in countries, f"expected {must} in data"

    date_range = db.execute(
        f"SELECT MIN(period_date), MAX(period_date) FROM read_parquet('{path}')"
    ).fetchone()
    log.info("period range: %s .. %s", *date_range)

    # Uniqueness on the composite key
    dup = db.execute(f"""
        SELECT COUNT(*) FROM (
            SELECT ref_area, time_period, energy_product, flow_breakdown, unit_measure
            FROM read_parquet('{path}')
            GROUP BY ALL HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    assert dup == 0, f"{dup} duplicate key rows"

    log.info("Validation passed (%.1fs)", time.monotonic() - t0)


if __name__ == "__main__":
    main()
