"""Validate extracted Parquet output locally."""

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

t0 = time.monotonic()

out = os.environ.get("OUTPUT_DIR", "output")
path = f"{out}/data.parquet"

r = duckdb.sql(f"SELECT COUNT(*) AS n FROM read_parquet('{path}')").fetchone()
log.info("Row count: %d", r[0])
assert r[0] >= 10, f"Too few rows: {r[0]}"
log.info("Validation passed.")
log.info("Elapsed: %.2fs", time.monotonic() - t0)
