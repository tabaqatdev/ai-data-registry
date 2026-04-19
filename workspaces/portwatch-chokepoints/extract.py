"""Extract IMF PortWatch chokepoint daily transits for the Middle East.

Two outputs:
- chokepoints.parquet : 28 global chokepoints with lat/lon (static reference)
- daily_transits.parquet : daily ship-transit counts + capacity for the three
  ME chokepoints (Suez, Bab el-Mandeb, Strait of Hormuz)

Source: IMF PortWatch ArcGIS FeatureServer (services9.arcgis.com/weJ1QsnbMYJlCHdG).
Mode is "replace": full history fits in ~8k rows, always publish latest snapshot.
"""

import logging
import os
import time
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

FS = "https://services9.arcgis.com/weJ1QsnbMYJlCHdG/ArcGIS/rest/services"
CHOKEPOINTS_LAYER = f"{FS}/PortWatch_chokepoints_database/FeatureServer/0"
DAILY_LAYER = f"{FS}/Daily_Chokepoints_Data/FeatureServer/0"

# Suez, Bab el-Mandeb, Strait of Hormuz (primary ME oil-export chokepoints)
ME_PORTIDS = ("chokepoint1", "chokepoint4", "chokepoint6")
PAGE_SIZE = 1000

USER_AGENT = "ai-data-registry/portwatch-chokepoints"
HTTP_TIMEOUT_SECS = 60
HTTP_RETRIES = 4
HTTP_RETRY_WAIT_SECS = 5


def fetch_to_file(url, dest_path):
    """GET with linear-backoff retries; terminal on 4xx (except 429)."""
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


def count_features(layer_url, where):
    url = f"{layer_url}/query?where={urllib.parse.quote(where)}&returnCountOnly=true&f=json"
    tmp = f"{OUT}/_count.json"
    fetch_to_file(url, tmp)
    import json
    n = json.loads(open(tmp).read()).get("count", 0)
    os.unlink(tmp)
    return int(n)


def fetch_all_pages(layer_url, where, page_dir):
    """Paginate with orderByFields=ObjectId+ASC, save each page as a numbered JSON file.

    Returns list of local file paths for DuckDB's read_json_auto to consume.
    """
    os.makedirs(page_dir, exist_ok=True)
    total = count_features(layer_url, where)
    log.info("  total matching: %d", total)
    if DRY_RUN:
        total = min(total, PAGE_SIZE)
        log.info("  DRY_RUN: capping to first %d rows", total)
    paths = []
    offset = 0
    page_idx = 0
    while offset < total:
        params = (
            f"where={urllib.parse.quote(where)}"
            "&outFields=*&outSR=4326&returnGeometry=false"
            "&orderByFields=ObjectId+ASC"
            f"&resultOffset={offset}&resultRecordCount={PAGE_SIZE}&f=geojson"
        )
        url = f"{layer_url}/query?{params}"
        path = f"{page_dir}/page_{page_idx:04d}.geojson"
        log.debug("  GET page %d (offset=%d)", page_idx, offset)
        fetch_to_file(url, path)
        paths.append(path)
        offset += PAGE_SIZE
        page_idx += 1
    return paths, total


import urllib.parse  # noqa: E402 (used by fetch_all_pages above)


def main():
    t0 = time.monotonic()
    mode = "dry-run" if DRY_RUN else "extract"
    log.info("Starting %s, output_dir=%s", mode, OUT)
    os.makedirs(OUT, exist_ok=True)

    db = duckdb.connect()
    db.execute("INSTALL spatial; LOAD spatial;")
    db.execute("INSTALL h3 FROM community; LOAD h3;")
    db.execute("SET geometry_always_xy = true;")

    from datetime import datetime, timezone
    snapshot = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # 1. Chokepoints lookup (28 rows, single page, geometry present)
    log.info("Fetching chokepoint reference layer...")
    cp_url = (
        f"{CHOKEPOINTS_LAYER}/query?where={urllib.parse.quote('1=1')}"
        "&outFields=*&outSR=4326&returnGeometry=true&f=geojson"
    )
    cp_path = f"{OUT}/_chokepoints.geojson"
    fetch_to_file(cp_url, cp_path)

    out_chokepoints = f"{OUT}/chokepoints.parquet"
    db.execute(f"""
        COPY (
            SELECT
                f.properties.portid AS portid,
                f.properties.portname AS portname,
                f.properties.country AS country,
                f.properties.ISO3 AS iso3,
                f.properties.continent AS continent,
                f.properties.fullname AS fullname,
                f.properties.LOCODE AS locode,
                f.properties.pageid AS pageid,
                f.properties.lat::DOUBLE AS lat,
                f.properties.lon::DOUBLE AS lon,
                f.properties.vessel_count_total::DOUBLE AS vessel_count_total,
                f.properties.vessel_count_container::DOUBLE AS vessel_count_container,
                f.properties.vessel_count_dry_bulk::DOUBLE AS vessel_count_dry_bulk,
                f.properties.vessel_count_general_cargo::DOUBLE AS vessel_count_general_cargo,
                f.properties.vessel_count_RoRo::DOUBLE AS vessel_count_roro,
                f.properties.vessel_count_tanker::DOUBLE AS vessel_count_tanker,
                f.properties.industry_top1 AS industry_top1,
                f.properties.industry_top2 AS industry_top2,
                f.properties.industry_top3 AS industry_top3,
                f.properties.share_country_maritime_import::DOUBLE AS share_maritime_import,
                f.properties.share_country_maritime_export::DOUBLE AS share_maritime_export,
                h3_latlng_to_cell(f.properties.lat::DOUBLE, f.properties.lon::DOUBLE, 5)::UBIGINT AS h3_r5,
                '{snapshot}'::TIMESTAMP AS snapshot_time,
                ST_SetCRS(ST_Point(f.properties.lon::DOUBLE, f.properties.lat::DOUBLE), 'EPSG:4326') AS geometry
            FROM (SELECT unnest(features) AS f FROM read_json_auto('{cp_path}', maximum_object_size=67108864))
            ORDER BY ST_Hilbert(ST_Point(f.properties.lon::DOUBLE, f.properties.lat::DOUBLE))
        ) TO '{out_chokepoints}' (
            FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 15,
            ROW_GROUP_SIZE 100000, GEOPARQUET_VERSION 'V2'
        )
    """)
    n_cp = db.execute(
        f"SELECT COUNT(*) FROM read_parquet('{out_chokepoints}')"
    ).fetchone()[0]
    log.info("  chokepoints.parquet: %d rows", n_cp)
    os.unlink(cp_path)

    # 2. Daily transits for the 3 ME chokepoints (paginated)
    where = "portid IN ('" + "','".join(ME_PORTIDS) + "')"
    log.info("Fetching daily transits where %s", where)
    page_dir = f"{OUT}/_daily_pages"
    paths, total = fetch_all_pages(DAILY_LAYER, where, page_dir)

    paths_sql = ", ".join(f"'{p}'" for p in paths)
    out_daily = f"{OUT}/daily_transits.parquet"
    db.execute(f"""
        COPY (
            WITH raw AS (
                SELECT unnest(features) AS f
                FROM read_json_auto([{paths_sql}], maximum_object_size=268435456)
            ),
            joined AS (
                SELECT
                    r.f.properties.portid AS portid,
                    r.f.properties.portname AS portname,
                    epoch_ms(r.f.properties.date::BIGINT)::DATE AS date,
                    r.f.properties.year::INTEGER AS year,
                    r.f.properties.month::INTEGER AS month,
                    r.f.properties.day::INTEGER AS day,
                    r.f.properties.n_container::INTEGER AS n_container,
                    r.f.properties.n_dry_bulk::INTEGER AS n_dry_bulk,
                    r.f.properties.n_general_cargo::INTEGER AS n_general_cargo,
                    r.f.properties.n_roro::INTEGER AS n_roro,
                    r.f.properties.n_tanker::INTEGER AS n_tanker,
                    r.f.properties.n_cargo::INTEGER AS n_cargo,
                    r.f.properties.n_total::INTEGER AS n_total,
                    r.f.properties.capacity_container::DOUBLE AS capacity_container,
                    r.f.properties.capacity_dry_bulk::DOUBLE AS capacity_dry_bulk,
                    r.f.properties.capacity_general_cargo::DOUBLE AS capacity_general_cargo,
                    r.f.properties.capacity_roro::DOUBLE AS capacity_roro,
                    r.f.properties.capacity_tanker::DOUBLE AS capacity_tanker,
                    r.f.properties.capacity_cargo::DOUBLE AS capacity_cargo,
                    r.f.properties.capacity::DOUBLE AS capacity
                FROM raw r
            )
            SELECT
                j.*,
                c.lat AS lat,
                c.lon AS lon,
                h3_latlng_to_cell(c.lat, c.lon, 5)::UBIGINT AS h3_r5,
                '{snapshot}'::TIMESTAMP AS snapshot_time,
                ST_SetCRS(ST_Point(c.lon, c.lat), 'EPSG:4326') AS geometry
            FROM joined j
            LEFT JOIN read_parquet('{out_chokepoints}') c USING (portid)
            ORDER BY j.date DESC, j.portid
        ) TO '{out_daily}' (
            FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 15,
            ROW_GROUP_SIZE 100000, GEOPARQUET_VERSION 'V2'
        )
    """)
    n_dt = db.execute(
        f"SELECT COUNT(*) FROM read_parquet('{out_daily}')"
    ).fetchone()[0]
    log.info("  daily_transits.parquet: %d rows (fetched %d pages)", n_dt, len(paths))

    for p in paths:
        os.unlink(p)
    os.rmdir(page_dir)

    db.close()
    log.info("%s complete (%.1fs)", mode, time.monotonic() - t0)


if __name__ == "__main__":
    main()
