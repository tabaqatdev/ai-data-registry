"""Validate extracted OpenSanctions region_sanctions Parquet output locally."""

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
PATH = f"{OUT}/region_sanctions.parquet"
DRY_RUN = os.environ.get("DRY_RUN", "0") == "1"

EXPECTED_COLS = {
    "id", "entity_type", "name", "aliases", "birth_date", "countries",
    "addresses", "identifiers", "sanctions", "phones", "emails",
    "program_ids", "dataset", "first_seen", "last_seen", "last_change",
    "country_list", "sanction_list", "entity_category",
    "snapshot_time", "snapshot_date",
}

MIN_ROWS_FULL = 5_000
MIN_ROWS_DRY = 100


def main():
    t0 = time.monotonic()
    db = duckdb.connect()

    log.info("Validating %s...", PATH)
    if not os.path.exists(PATH):
        log.error("Output not found at %s", PATH)
        raise SystemExit(1)

    count = db.execute(f"SELECT COUNT(*) FROM read_parquet('{PATH}')").fetchone()[0]
    log.info("Row count: %d", count)
    threshold = MIN_ROWS_DRY if DRY_RUN else MIN_ROWS_FULL
    assert count >= threshold, f"Too few rows: {count} (expected >= {threshold})"

    cols = db.execute(f"DESCRIBE SELECT * FROM read_parquet('{PATH}')").fetchall()
    col_names = {c[0] for c in cols}
    log.info("Columns: %d", len(col_names))
    missing = EXPECTED_COLS - col_names
    extra = col_names - EXPECTED_COLS
    assert not missing, f"Missing expected columns: {missing}"
    if extra:
        log.warning("Unexpected extra columns: %s", extra)

    id_check = db.execute(f"""
        SELECT COUNT(*),
               COUNT(DISTINCT (id, snapshot_date)),
               COUNT(*) FILTER (WHERE id IS NULL),
               COUNT(DISTINCT snapshot_date)
        FROM read_parquet('{PATH}')
    """).fetchone()
    total, unique_pairs, null_ids, snap_dates = id_check
    log.info(
        "Uniqueness - total: %d, distinct (id,snapshot_date): %d, null id: %d, snapshot_dates: %d",
        total, unique_pairs, null_ids, snap_dates,
    )
    assert null_ids == 0, f"{null_ids} rows have NULL id"
    assert unique_pairs == total, (
        f"(id, snapshot_date) not unique: {total - unique_pairs} duplicates"
    )
    assert snap_dates == 1, (
        f"Expected one snapshot_date per file, got {snap_dates}"
    )

    cat_counts = db.execute(f"""
        SELECT entity_category, COUNT(*) AS n
        FROM read_parquet('{PATH}')
        GROUP BY entity_category
        ORDER BY n DESC
    """).fetchall()
    log.info("Entity categories: %s",
             ", ".join(f"{c}={n}" for c, n in cat_counts))

    snap_check = db.execute(f"""
        SELECT COUNT(DISTINCT snapshot_time), MIN(snapshot_time), MAX(snapshot_time)
        FROM read_parquet('{PATH}')
    """).fetchone()
    log.info("Snapshot timestamps: distinct=%d, min=%s, max=%s", *snap_check)
    assert snap_check[0] == 1, (
        f"Expected one snapshot_time, got {snap_check[0]}"
    )

    log.info("Summary of timestamp columns:")
    summary = db.execute(f"""
        SUMMARIZE SELECT first_seen, last_seen, last_change
        FROM read_parquet('{PATH}')
    """).fetchall()
    for row in summary:
        # SUMMARIZE cols: column_name, column_type, min, max, approx_unique,
        # avg, std, q25, q50, q75, count, null_percentage
        col, min_v, max_v, null_pct = row[0], row[2], row[3], row[11]
        log.info("  %s: min=%s max=%s null_pct=%s%%", col, min_v, max_v, null_pct)

    elapsed = time.monotonic() - t0
    log.info("Validation passed (%.1fs)", elapsed)


if __name__ == "__main__":
    main()
