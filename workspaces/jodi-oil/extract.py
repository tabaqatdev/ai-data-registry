"""Extract JODI World Primary oil data, filtered to ME/Gulf producers.

Source: https://www.jodidata.org/_resources/files/downloads/oil-data/world_primary_csv.zip
- Single CSV inside: NewProcedure_Primary_CSV.csv (~280 MB uncompressed)
- Schema: REF_AREA, TIME_PERIOD (YYYY-MM), ENERGY_PRODUCT, FLOW_BREAKDOWN,
  UNIT_MEASURE, OBS_VALUE, ASSESSMENT_CODE
- OBS_VALUE can be numeric or '-' / 'x' (null / not available) -> cleaned to DOUBLE

Mode "replace": JODI publishes the full history with every monthly update.
"""

import logging
import os
import time
import urllib.request
import zipfile

import duckdb

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    level=logging.DEBUG if os.environ.get("DRY_RUN") else logging.INFO,
)
log = logging.getLogger(__name__)

OUT = os.environ.get("OUTPUT_DIR", "output")
DRY_RUN = os.environ.get("DRY_RUN", "0") == "1"

ZIP_URL = "https://www.jodidata.org/_resources/files/downloads/oil-data/world_primary_csv.zip"

# ME/Gulf producers + neighbours with material oil data
ME_CODES = (
    "SA", "AE", "KW", "QA", "OM", "BH",   # GCC
    "IR", "IQ", "YE",                     # Iran, Iraq, Yemen
    "EG", "SD", "DZ", "LY", "TN", "MA",   # North Africa
    "JO", "LB", "SY", "IL", "TR",          # Levant + Turkey
)

USER_AGENT = "ai-data-registry/jodi-oil"
HTTP_TIMEOUT_SECS = 120
HTTP_RETRIES = 4
HTTP_RETRY_WAIT_SECS = 10


def fetch_to_file(url, dest_path):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    last_err = None
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_SECS) as resp:
                with open(dest_path, "wb") as f:
                    while True:
                        chunk = resp.read(1024 * 256)
                        if not chunk:
                            break
                        f.write(chunk)
            return
        except Exception as e:
            last_err = e
            code = getattr(e, "code", None)
            if code is not None and 400 <= code < 500 and code != 429:
                log.error("HTTP %d (terminal) on %s", code, url)
                raise
            log.warning("attempt %d/%d failed (%s); retrying in %ds",
                        attempt, HTTP_RETRIES, e, HTTP_RETRY_WAIT_SECS * attempt)
            if attempt < HTTP_RETRIES:
                time.sleep(HTTP_RETRY_WAIT_SECS * attempt)
    raise RuntimeError(f"giving up on {url} after {HTTP_RETRIES} attempts: {last_err}")


def main():
    t0 = time.monotonic()
    mode = "dry-run" if DRY_RUN else "extract"
    log.info("Starting %s, output_dir=%s", mode, OUT)
    os.makedirs(OUT, exist_ok=True)

    zip_path = f"{OUT}/_world_primary.zip"
    log.info("Downloading %s", ZIP_URL)
    fetch_to_file(ZIP_URL, zip_path)
    size_mb = os.path.getsize(zip_path) / 1024 / 1024
    log.info("Downloaded %.1f MB", size_mb)

    log.info("Unzipping...")
    with zipfile.ZipFile(zip_path) as zf:
        csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
        if not csv_names:
            raise RuntimeError(f"no CSV inside {ZIP_URL}")
        csv_name = csv_names[0]
        zf.extract(csv_name, OUT)
    csv_path = f"{OUT}/{csv_name}"
    log.info("Extracted %s (%.1f MB)", csv_name,
             os.path.getsize(csv_path) / 1024 / 1024)

    from datetime import datetime, timezone
    snapshot = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    db = duckdb.connect()
    codes_sql = ", ".join(f"'{c}'" for c in ME_CODES)
    limit_clause = "LIMIT 5000" if DRY_RUN else ""

    out_path = f"{OUT}/monthly_flows.parquet"
    db.execute(f"""
        COPY (
            SELECT
                REF_AREA AS ref_area,
                TIME_PERIOD AS time_period,
                (TIME_PERIOD || '-01')::DATE AS period_date,
                ENERGY_PRODUCT AS energy_product,
                FLOW_BREAKDOWN AS flow_breakdown,
                UNIT_MEASURE AS unit_measure,
                TRY_CAST(NULLIF(NULLIF(OBS_VALUE, '-'), 'x') AS DOUBLE) AS obs_value,
                ASSESSMENT_CODE::INTEGER AS assessment_code,
                '{snapshot}'::TIMESTAMP AS snapshot_time
            FROM read_csv_auto('{csv_path}', header=true, sample_size=-1)
            WHERE REF_AREA IN ({codes_sql})
            ORDER BY ref_area, time_period, energy_product, flow_breakdown, unit_measure
            {limit_clause}
        ) TO '{out_path}' (
            FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 15,
            ROW_GROUP_SIZE 100000
        )
    """)

    n = db.execute(f"SELECT COUNT(*) FROM read_parquet('{out_path}')").fetchone()[0]
    size_mb_out = os.path.getsize(out_path) / 1024 / 1024
    log.info("Wrote %s (%d rows, %.2f MB)", out_path, n, size_mb_out)

    distinct = db.execute(f"""
        SELECT
            COUNT(DISTINCT ref_area) AS countries,
            MIN(period_date) AS first_period,
            MAX(period_date) AS last_period
        FROM read_parquet('{out_path}')
    """).fetchone()
    log.info("  countries=%d, periods %s .. %s", *distinct)

    os.unlink(zip_path)
    os.unlink(csv_path)
    db.close()
    log.info("%s complete (%.1fs)", mode, time.monotonic() - t0)


if __name__ == "__main__":
    main()
