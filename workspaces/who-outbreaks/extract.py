"""Extract WHO Disease Outbreak News (global) with ME country tagging.

Source: https://www.who.int/api/news/diseaseoutbreaknews (OData v4)
- ~3,100 historical items, paginated ($top=100, $skip).
- Non-spatial, keyless.
- Country is not structured; derive `me_countries` from regex matches on
  Title + Overview against a curated ME country name list.

Mode "replace": full history re-published each run; WHO publishes new DONs
sporadically (several per month).
"""

import logging
import os
import re
import time
import urllib.parse
import urllib.request

import duckdb

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    level=logging.DEBUG if os.environ.get("DRY_RUN") else logging.INFO,
)
log = logging.getLogger(__name__)

OUT = os.environ.get("OUTPUT_DIR", "output")
DRY_RUN = os.environ.get("DRY_RUN", "0") == "1"

WHO_URL = "https://www.who.int/api/news/diseaseoutbreaknews"
PAGE_SIZE = 100

# ME + North Africa country name -> ISO3 alpha-3
ME_COUNTRIES = {
    "Saudi Arabia": "SAU", "United Arab Emirates": "ARE", "Kuwait": "KWT",
    "Bahrain": "BHR", "Qatar": "QAT", "Oman": "OMN",
    "Iran": "IRN", "Iraq": "IRQ", "Yemen": "YEM",
    "Egypt": "EGY", "Jordan": "JOR", "Lebanon": "LBN", "Syria": "SYR",
    "Israel": "ISR", "Palestine": "PSE", "Turkey": "TUR", "T\u00fcrkiye": "TUR",
    "Sudan": "SDN", "Libya": "LBY", "Tunisia": "TUN", "Algeria": "DZA",
    "Morocco": "MAR", "Djibouti": "DJI", "Somalia": "SOM", "Eritrea": "ERI",
    "Ethiopia": "ETH", "Afghanistan": "AFG", "Pakistan": "PAK",
}
# Compile regex once, case-insensitive whole-word match
ME_PATTERN = re.compile(
    r"\b(" + "|".join(re.escape(n) for n in ME_COUNTRIES) + r")\b",
    re.IGNORECASE,
)

USER_AGENT = "ai-data-registry/who-outbreaks"
HTTP_TIMEOUT_SECS = 60
HTTP_RETRIES = 4
HTTP_RETRY_WAIT_SECS = 5

import json


def fetch_json(url):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    last_err = None
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_SECS) as resp:
                return json.loads(resp.read())
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


def match_me(*texts):
    """Return sorted comma-joined list of ISO3s found in any text, or empty."""
    found = set()
    for t in texts:
        if not t:
            continue
        for m in ME_PATTERN.finditer(t):
            name = m.group(1)
            # Canonicalize case via exact dict lookup (case-insensitive)
            for k in ME_COUNTRIES:
                if k.lower() == name.lower():
                    found.add(ME_COUNTRIES[k])
                    break
    return ",".join(sorted(found))


def main():
    t0 = time.monotonic()
    mode = "dry-run" if DRY_RUN else "extract"
    log.info("Starting %s, output_dir=%s", mode, OUT)
    os.makedirs(OUT, exist_ok=True)

    # Paginate through all items
    skip = 0
    all_rows = []
    max_items = 200 if DRY_RUN else None
    while True:
        params = f"?$top={PAGE_SIZE}&$skip={skip}&$orderby=PublicationDateAndTime%20desc"
        url = f"{WHO_URL}{params}"
        log.debug("GET skip=%d", skip)
        data = fetch_json(url)
        batch = data.get("value", [])
        if not batch:
            break
        all_rows.extend(batch)
        skip += len(batch)
        if max_items and len(all_rows) >= max_items:
            all_rows = all_rows[:max_items]
            break
        if len(batch) < PAGE_SIZE:
            break
    log.info("Fetched %d items", len(all_rows))

    # Enrich with me_countries regex match
    for r in all_rows:
        r["me_countries"] = match_me(
            r.get("Title"), r.get("OverrideTitle"), r.get("Overview")
        )

    # Dump to temp ndjson so DuckDB can consume
    ndjson_path = f"{OUT}/_don.ndjson"
    with open(ndjson_path, "w") as f:
        for r in all_rows:
            f.write(json.dumps(r))
            f.write("\n")

    from datetime import datetime, timezone
    snapshot = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    db = duckdb.connect()
    out_path = f"{OUT}/outbreaks.parquet"
    db.execute(f"""
        COPY (
            SELECT
                Id AS id,
                UrlName AS url_name,
                Title AS title,
                OverrideTitle AS override_title,
                PublicationDateAndTime::TIMESTAMP AS publication_time,
                LastModified::TIMESTAMP AS last_modified,
                DateCreated::TIMESTAMP AS date_created,
                DonId AS don_id,
                Provider AS provider,
                regexp_replace(COALESCE(Overview, ''), '<[^>]+>', '', 'g') AS overview_text,
                regexp_replace(COALESCE(Epidemiology, ''), '<[^>]+>', '', 'g') AS epidemiology_text,
                regexp_replace(COALESCE(Assessment, ''), '<[^>]+>', '', 'g') AS assessment_text,
                regexp_replace(COALESCE(Advice, ''), '<[^>]+>', '', 'g') AS advice_text,
                regexp_replace(COALESCE(Response, ''), '<[^>]+>', '', 'g') AS response_text,
                regexp_replace(COALESCE(Summary, ''), '<[^>]+>', '', 'g') AS summary_text,
                'https://www.who.int' || ItemDefaultUrl AS source_url,
                me_countries,
                (me_countries != '') AS is_me_related,
                '{snapshot}'::TIMESTAMP AS snapshot_time
            FROM read_json_auto('{ndjson_path}', format='newline_delimited', maximum_object_size=16777216)
            ORDER BY publication_time DESC
        ) TO '{out_path}' (
            FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 15,
            ROW_GROUP_SIZE 10000
        )
    """)

    n = db.execute(f"SELECT COUNT(*) FROM read_parquet('{out_path}')").fetchone()[0]
    n_me = db.execute(
        f"SELECT COUNT(*) FROM read_parquet('{out_path}') WHERE is_me_related"
    ).fetchone()[0]
    size_kb = os.path.getsize(out_path) / 1024
    log.info("Wrote %s (%d rows, %d ME-related, %.1f KB)",
             out_path, n, n_me, size_kb)

    os.unlink(ndjson_path)
    db.close()
    log.info("%s complete (%.1fs)", mode, time.monotonic() - t0)


if __name__ == "__main__":
    main()
