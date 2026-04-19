"""Validate USGS earthquakes output."""

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
    path = f"{OUT}/events.parquet"
    if not os.path.exists(path):
        raise SystemExit(f"missing {path}")

    db = duckdb.connect()
    db.execute("INSTALL spatial; LOAD spatial;")

    n = db.execute(f"SELECT COUNT(*) FROM read_parquet('{path}')").fetchone()[0]
    log.info("events: %d rows", n)

    cols = {c[0] for c in db.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{path}')"
    ).fetchall()}
    required = {"event_id", "mag", "place", "event_time", "latitude", "longitude",
                "depth_km", "h3_r5", "h3_r8", "snapshot_time", "geometry"}
    missing = required - cols
    assert not missing, f"missing columns: {missing}"

    bbox = db.execute(f"""
        SELECT MIN(latitude), MAX(latitude), MIN(longitude), MAX(longitude)
        FROM read_parquet('{path}')
    """).fetchone()
    log.info("BBOX actual: lat [%.2f, %.2f], lon [%.2f, %.2f]", *bbox) if n else None

    if n:
        mag_stats = db.execute(
            f"SELECT MIN(mag), MAX(mag), AVG(mag) FROM read_parquet('{path}') WHERE mag IS NOT NULL"
        ).fetchone()
        log.info("magnitude: min=%.2f max=%.2f avg=%.2f", *mag_stats)

    null_geom = db.execute(
        f"SELECT COUNT(*) FROM read_parquet('{path}') WHERE geometry IS NULL"
    ).fetchone()[0]
    assert null_geom == 0, f"{null_geom} rows with null geometry"

    log.info("Validation passed (%.1fs)", time.monotonic() - t0)


if __name__ == "__main__":
    main()
