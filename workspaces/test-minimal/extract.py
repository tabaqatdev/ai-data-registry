"""Generate test Parquet output."""

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
dry_run = os.environ.get("DRY_RUN", "0") == "1"
row_count = 50 if dry_run else 200

os.makedirs(out, exist_ok=True)

duckdb.sql(f"""
    COPY (
        SELECT i AS id,
               'item_' || i AS name,
               37.77 + random()*0.01 AS lat,
               -122.42 + random()*0.01 AS lon
        FROM range({row_count}) t(i)
    ) TO '{out}/data.parquet' (FORMAT PARQUET)
""")

label = "Dry run" if dry_run else "Extract"
log.info("%s: wrote %s/data.parquet (%d rows)", label, out, row_count)
log.info("Elapsed: %.2fs", time.monotonic() - t0)
