"""Extract IODA internet-outage signals for 15 ME / Gulf countries.

Pulls the raw signals endpoint per country for a 48h rolling window. IODA's
response is a nested `data[]` where items may be either dicts or lists of
dicts (we flatten both shapes). Each series contains:

  {datasource, subtype, step, from, values[]}

We expand to one row per (country, datasource, subtype, timestamp).

Append mode keyed on `(country_code, datasource, subtype, timestamp, snapshot_time)`.
Each 4-hour run captures the freshest IODA state without dropping history.

License: CC-BY-NC-4.0 (CAIDA + Georgia Tech academic-research license; closest
SPDX identifier. The manifest validator warns on restrictive data licenses,
which is the intended signal for reviewers).
"""

import json
import logging
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

import duckdb

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    level=logging.DEBUG if os.environ.get("DRY_RUN") else logging.INFO,
)
log = logging.getLogger(__name__)

OUT = os.environ.get("OUTPUT_DIR", "output")
DRY_RUN = os.environ.get("DRY_RUN", "0") == "1"

IODA_BASE = "https://api.ioda.inetintel.cc.gatech.edu/v2/signals/raw/country"
HOURS_BACK = 48

COUNTRIES = [
    ("SA", "Saudi Arabia"),
    ("AE", "United Arab Emirates"),
    ("KW", "Kuwait"),
    ("BH", "Bahrain"),
    ("QA", "Qatar"),
    ("OM", "Oman"),
    ("YE", "Yemen"),
    ("IQ", "Iraq"),
    ("IR", "Iran"),
    ("JO", "Jordan"),
    ("EG", "Egypt"),
    ("SY", "Syria"),
    ("LB", "Lebanon"),
    ("PS", "Palestine"),
    ("IL", "Israel"),
]


def fetch_country(code, from_ts, until_ts):
    url = f"{IODA_BASE}/{code}?from={from_ts}&until={until_ts}"
    req = urllib.request.Request(
        url, headers={"User-Agent": "ai-data-registry/ioda-signals"}
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read())
        return data.get("data", [])
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
        log.exception("IODA fetch failed for %s", code)
        return []


def flatten_series(code, name, signals):
    """Handle both shapes: [ [series, ...], ... ] and [ series, ... ]."""
    flat = []
    for item in signals:
        if isinstance(item, list):
            flat.extend(x for x in item if isinstance(x, dict))
        elif isinstance(item, dict):
            flat.append(item)

    rows = []
    for series in flat:
        datasource = series.get("datasource", "unknown")
        subtype = series.get("subtype", "") or ""
        step = series.get("step", 1800)
        from_ts = series.get("from", 0)
        values = series.get("values", []) or []
        for i, val in enumerate(values):
            if val is None or not isinstance(val, (int, float)):
                continue
            ts = from_ts + i * step
            rows.append((
                code, name, datasource, subtype,
                ts, step, float(val),
            ))
    return rows


def main():
    t0 = time.monotonic()
    mode = "dry-run" if DRY_RUN else "extract"
    log.info("Starting %s, output_dir=%s", mode, OUT)
    os.makedirs(OUT, exist_ok=True)
    out_path = f"{OUT}/signals.parquet"

    now = datetime.now(timezone.utc)
    snapshot = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    until_ts = int(now.timestamp())
    from_ts = int((now - timedelta(hours=HOURS_BACK)).timestamp())

    log.info("Window: %s to %s (%dh)", from_ts, until_ts, HOURS_BACK)
    log.info("Snapshot: %s", snapshot)

    countries = COUNTRIES[:3] if DRY_RUN else COUNTRIES
    all_rows = []
    for code, name in countries:
        log.info("Fetching %s (%s)...", code, name)
        signals = fetch_country(code, from_ts, until_ts)
        rows = flatten_series(code, name, signals)
        log.info("  %s: %d signal points", code, len(rows))
        all_rows.extend(rows)

    log.info("Total signal points: %d", len(all_rows))
    if len(all_rows) == 0:
        log.warning("No signals returned. Writing empty file.")

    db = duckdb.connect()

    db.execute("""
        CREATE TABLE raw (
            country_code VARCHAR, country_name VARCHAR,
            datasource VARCHAR, subtype VARCHAR,
            timestamp BIGINT, step INTEGER, value DOUBLE
        )
    """)
    if all_rows:
        db.executemany(
            "INSERT INTO raw VALUES (?, ?, ?, ?, ?, ?, ?)",
            all_rows,
        )

    log.info("Writing %s...", out_path)
    db.execute(f"""
        COPY (
            SELECT
                country_code,
                country_name,
                datasource,
                subtype,
                timestamp,
                to_timestamp(timestamp) AS timestamp_utc,
                step AS step_seconds,
                value,
                '{snapshot}'::TIMESTAMP AS snapshot_time
            FROM raw
            ORDER BY country_code, datasource, subtype, timestamp
        ) TO '{out_path}' (
            FORMAT PARQUET,
            COMPRESSION ZSTD,
            COMPRESSION_LEVEL 15,
            ROW_GROUP_SIZE 100000
        )
    """)

    final = db.execute(
        f"SELECT COUNT(*) FROM read_parquet('{out_path}')"
    ).fetchone()[0]
    size_kb = os.path.getsize(out_path) / 1024
    log.info("Wrote %s (%d rows, %.1f KB)", out_path, final, size_kb)

    db.close()
    elapsed = time.monotonic() - t0
    log.info("%s complete: %d rows in %.1fs", mode, final, elapsed)


if __name__ == "__main__":
    main()
