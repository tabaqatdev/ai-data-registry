"""Validate extracted GDELT events GeoParquet output locally."""

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
PATH = f"{OUT}/events.parquet"

REQUIRED_COLS = {
    "globaleventid", "sqldate", "event_date", "dateadded",
    "actor1_code", "actor2_code", "event_code",
    "goldstein_scale", "avg_tone", "num_articles",
    "latitude", "longitude",
    "source_url", "country",
    "h3_r5", "h3_r8", "h3_r10",
    "snapshot_time", "snapshot_date", "geometry",
}

FORBIDDEN_COLS = {
    "ArticleContent", "ArticleTitle", "ArticleAuthor",
    "article_content", "article_title", "article_author",
}


def main():
    t0 = time.monotonic()
    db = duckdb.connect()
    db.execute("INSTALL spatial; LOAD spatial;")

    log.info("Validating %s...", PATH)
    if not os.path.exists(PATH):
        log.error("Output not found at %s", PATH)
        raise SystemExit(1)

    count = db.execute(f"SELECT COUNT(*) FROM read_parquet('{PATH}')").fetchone()[0]
    log.info("Row count: %d", count)

    cols = db.execute(f"DESCRIBE SELECT * FROM read_parquet('{PATH}')").fetchall()
    col_names = {c[0] for c in cols}
    missing = REQUIRED_COLS - col_names
    assert not missing, f"Missing required columns: {missing}"

    leaked = FORBIDDEN_COLS & col_names
    assert not leaked, (
        f"Article content columns leaked into output: {leaked}. "
        "These must be dropped to stay within CC0-1.0 scope."
    )
    log.info("Schema OK (%d columns, no article content leak)", len(col_names))

    if count == 0:
        log.warning("Zero events in window. Valid if upstream had no SA events.")
        elapsed = time.monotonic() - t0
        log.info("Validation passed (empty window) (%.1fs)", elapsed)
        return

    dupes = db.execute(f"""
        SELECT globaleventid, snapshot_date, COUNT(*) AS n
        FROM read_parquet('{PATH}')
        GROUP BY ALL
        HAVING n > 1
        LIMIT 1
    """).fetchone()
    assert dupes is None, f"Duplicate (globaleventid, snapshot_date): {dupes}"

    date_range = db.execute(f"""
        SELECT MIN(event_date), MAX(event_date)
        FROM read_parquet('{PATH}')
    """).fetchone()
    log.info("Event date range: %s to %s", *date_range)

    h3_nulls = db.execute(f"""
        SELECT
            COUNT(*) FILTER (WHERE h3_r5 IS NULL),
            COUNT(*) FILTER (WHERE h3_r8 IS NULL),
            COUNT(*) FILTER (WHERE h3_r10 IS NULL)
        FROM read_parquet('{PATH}')
    """).fetchone()
    log.info("H3 nulls - r5: %d, r8: %d, r10: %d", *h3_nulls)
    assert all(n == 0 for n in h3_nulls), "H3 cells must be non-null"

    elapsed = time.monotonic() - t0
    log.info("Validation passed (%.1fs)", elapsed)


if __name__ == "__main__":
    main()
