"""Validate OSM infrastructure GeoParquet outputs locally.

Uses per-table thresholds from the declared contract. Tables marked
`optional = true` are allowed to be empty or missing.
"""

import logging
import os
import time
from pathlib import Path

import duckdb

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    level=logging.DEBUG if os.environ.get("DRY_RUN") else logging.INFO,
)
log = logging.getLogger(__name__)

OUT = Path(os.environ.get("OUTPUT_DIR", "output"))
DRY_RUN = os.environ.get("DRY_RUN", "0") == "1"

# (table, min_rows_full, optional)
TABLES = [
    ("power_lines", 1000, False),
    ("power_plants", 10, True),
    ("power_generators", 100, False),
    ("substations", 100, False),
    ("power_towers", 1000, False),
    ("switchgear", 10, True),
    ("pipelines", 100, False),
    ("petroleum_sites", 10, True),
    ("petroleum_wells", 50, False),
    ("telecoms", 100, False),
    ("water_infra", 50, False),
]


def validate_table(db, name, min_rows, optional):
    path = OUT / f"{name}.parquet"
    if not path.exists():
        if optional:
            log.warning("%s: optional, skipping (file missing)", name)
            return True
        log.error("%s: required file missing at %s", name, path)
        return False

    n = db.execute(f"SELECT COUNT(*) FROM read_parquet('{path}')").fetchone()[0]
    log.info("%s: %d rows", name, n)

    if DRY_RUN:
        # Dry-run uses a 0.5deg Riyadh bbox — can legitimately have 0 features
        if n == 0 and not optional:
            log.warning("%s: zero features in dry-run bbox (acceptable)", name)
        return True

    if n < min_rows:
        if optional:
            log.warning("%s: %d < %d but optional", name, n, min_rows)
            return True
        log.error("%s: %d < required %d", name, n, min_rows)
        return False

    if n == 0:
        return True

    cols = db.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()
    col_names = {c[0] for c in cols}
    for required in ("geometry", "h3_r8", "h3_r10", "snapshot_time"):
        if required not in col_names:
            log.error("%s: missing column %s", name, required)
            return False
    return True


def main():
    t0 = time.monotonic()
    db = duckdb.connect()
    db.execute("INSTALL spatial; LOAD spatial;")

    ok = True
    for name, min_rows, optional in TABLES:
        ok &= validate_table(db, name, min_rows, optional)

    elapsed = time.monotonic() - t0
    if not ok:
        log.error("Validation FAILED (%.1fs)", elapsed)
        raise SystemExit(1)
    log.info("Validation passed (%.1fs)", elapsed)


if __name__ == "__main__":
    main()
