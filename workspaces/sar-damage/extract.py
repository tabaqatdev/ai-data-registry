"""Extract SAR damage indicators via Pixel-Wise T-Test on Sentinel-1 GRD.

Event-driven workspace, parameterized via env vars:
  - SAR_AOI: preset key (see AOI_PRESETS), e.g. "iran_isfahan"
  - SAR_EVENT_DATE: YYYY-MM-DD
  - SAR_PRE_MONTHS: int (default 12)
  - SAR_POST_DAYS: int (default 24)
  - SAR_T_THRESHOLD: float (default 2.5; ~99% confidence)
  - SAR_H3_RES: int (default 7 + 9 aggregated in output)
  - SAR_CATALOG: one of earth-search | planetary-computer | eopf

Pipeline:
  1. Search STAC catalog for Sentinel-1 GRD scenes pre/post event
  2. Load VV + VH bands as xarray stacks (odc-stac)
  3. Lee speckle filter
  4. Welch's t-test per pixel, per band, take max |t|
  5. Aggregate damaged vs total pixels per H3 r7 + r9 cell
  6. Write GeoParquet 2.0

DRY_RUN with `--search-only` flag checks that the catalog returns items for
the given AOI / date without running the heavy image processing. This keeps
PR validation under the 5-minute GitHub Actions timeout.

Append mode keyed on (h3_index, aoi, event_date) so repeated runs of the
same AOI / event build a time series of follow-up analyses.
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    level=logging.DEBUG if os.environ.get("DRY_RUN") else logging.INFO,
)
log = logging.getLogger(__name__)

OUT = os.environ.get("OUTPUT_DIR", "output")
DRY_RUN = os.environ.get("DRY_RUN", "0") == "1"

AOI_PRESETS = {
    "iran_isfahan":       [51.4, 32.5, 52.0, 32.9],
    "iran_tehran":        [51.0, 35.5, 51.8, 35.9],
    "iran_shiraz":        [52.3, 29.5, 52.8, 29.8],
    "iran_tabriz":        [46.1, 37.9, 46.5, 38.2],
    "iran_bandar_abbas":  [56.1, 27.0, 56.5, 27.3],
    "gaza":               [34.2, 31.2, 34.6, 31.6],
    "sanaa":              [44.1, 15.2, 44.4, 15.5],
    "aden":               [44.9, 12.7, 45.2, 13.0],
    "aleppo":             [36.9, 36.1, 37.3, 36.3],
    "damascus":           [36.2, 33.4, 36.5, 33.6],
    "beirut":             [35.3, 33.7, 35.7, 34.0],
    "mosul":              [42.9, 36.2, 43.3, 36.5],
    "baghdad":            [44.2, 33.2, 44.6, 33.5],
}

STAC_CATALOGS = {
    "earth-search": {
        "url": "https://earth-search.aws.element84.com/v1",
        "collection": "sentinel-1-grd",
    },
    "planetary-computer": {
        "url": "https://planetarycomputer.microsoft.com/api/stac/v1",
        "collection": "sentinel-1-grd",
    },
    "eopf": {
        "url": "https://stac.core.eopf.eodc.eu",
        "collection": "SENTINEL-1",
    },
}


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--aoi", default=os.environ.get("SAR_AOI", "iran_isfahan"))
    parser.add_argument("--event-date",
                        default=os.environ.get("SAR_EVENT_DATE", "2026-02-28"))
    parser.add_argument("--pre-months", type=int,
                        default=int(os.environ.get("SAR_PRE_MONTHS", "12")))
    parser.add_argument("--post-days", type=int,
                        default=int(os.environ.get("SAR_POST_DAYS", "24")))
    parser.add_argument("--t-threshold", type=float,
                        default=float(os.environ.get("SAR_T_THRESHOLD", "2.5")))
    parser.add_argument("--h3-res", type=int,
                        default=int(os.environ.get("SAR_H3_RES", "7")))
    parser.add_argument("--catalog",
                        default=os.environ.get("SAR_CATALOG", "earth-search"))
    parser.add_argument("--search-only", action="store_true")
    return parser.parse_args()


def search_scenes(bbox, pre_start, pre_end, post_start, post_end, catalog_key):
    from pystac_client import Client
    cat = STAC_CATALOGS[catalog_key]
    log.info("STAC: %s (%s)", catalog_key, cat["url"])
    client = Client.open(cat["url"])

    log.info("Searching pre-event scenes %s to %s", pre_start, pre_end)
    pre = list(client.search(
        collections=[cat["collection"]],
        bbox=bbox,
        datetime=f"{pre_start}/{pre_end}",
        query={"sar:instrument_mode": {"eq": "IW"}},
        max_items=200,
    ).items())

    log.info("Searching post-event scenes %s to %s", post_start, post_end)
    post = list(client.search(
        collections=[cat["collection"]],
        bbox=bbox,
        datetime=f"{post_start}/{post_end}",
        query={"sar:instrument_mode": {"eq": "IW"}},
        max_items=100,
    ).items())

    log.info("Pre: %d scenes, Post: %d scenes", len(pre), len(post))
    return pre, post


def run_full(args, bbox):
    import numpy as np
    import odc.stac
    from scipy.ndimage import uniform_filter
    import h3 as h3lib
    import duckdb

    event_dt = datetime.strptime(args.event_date, "%Y-%m-%d")
    pre_start = (event_dt - timedelta(days=args.pre_months * 30)).strftime("%Y-%m-%d")
    pre_end = args.event_date
    post_start = args.event_date
    post_end = (event_dt + timedelta(days=args.post_days)).strftime("%Y-%m-%d")

    pre_items, post_items = search_scenes(
        bbox, pre_start, pre_end, post_start, post_end, args.catalog,
    )
    if not pre_items or not post_items:
        log.error("No scenes returned; try another catalog or date range")
        raise SystemExit(1)

    log.info("Loading pre-event stack (%d scenes)...", len(pre_items))
    pre_ds = odc.stac.load(
        pre_items, bbox=bbox, bands=["vv", "vh"],
        resolution=20, groupby="solar_day",
        chunks={"x": 1024, "y": 1024, "time": 1},
    )
    log.info("Loading post-event stack (%d scenes)...", len(post_items))
    post_ds = odc.stac.load(
        post_items, bbox=bbox, bands=["vv", "vh"],
        resolution=20, groupby="solar_day",
        chunks={"x": 1024, "y": 1024, "time": 1},
    )

    def lee(arr, window=7):
        m = uniform_filter(arr, size=window)
        s = uniform_filter(arr**2, size=window)
        v = s - m**2
        ov = np.nanvar(arr)
        if ov == 0:
            return arr
        w = v / (v + ov)
        return m + w * (arr - m)

    log.info("Applying Lee filter + Welch's t-test...")
    results = {}
    for band in ("vv", "vh"):
        pre = pre_ds[band].values.astype(np.float32)
        post = post_ds[band].values.astype(np.float32)
        pre_f = np.stack([lee(pre[t]) for t in range(pre.shape[0])])
        post_f = np.stack([lee(post[t]) for t in range(post.shape[0])])
        pre_mean = np.nanmean(pre_f, axis=0)
        post_mean = np.nanmean(post_f, axis=0)
        pre_std = np.nanstd(pre_f, axis=0, ddof=1)
        post_std = np.nanstd(post_f, axis=0, ddof=1)
        denom = np.sqrt(
            pre_std**2 / max(pre_f.shape[0], 1)
            + post_std**2 / max(post_f.shape[0], 1)
        )
        denom = np.where(denom == 0, np.nan, denom)
        t_stat = np.abs((pre_mean - post_mean) / denom)
        results[f"t_{band}"] = t_stat
    t_max = np.nanmax(np.stack([results["t_vv"], results["t_vh"]]), axis=0)

    west, south, east, north = bbox
    ny, nx = t_max.shape
    lngs = np.linspace(west, east, nx)
    lats = np.linspace(north, south, ny)

    log.info("Aggregating to H3 r%d...", args.h3_res)
    cells = {}
    for iy in range(0, ny, 5):
        for ix in range(0, nx, 5):
            t = t_max[iy, ix]
            if np.isnan(t):
                continue
            lat, lng = lats[iy], lngs[ix]
            cell = h3lib.latlng_to_cell(lat, lng, args.h3_res)
            stats = cells.setdefault(
                cell, {"t_vals": [], "damaged": 0, "total": 0},
            )
            stats["t_vals"].append(float(t))
            stats["total"] += 1
            if t > args.t_threshold:
                stats["damaged"] += 1

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = []
    for cell, stats in cells.items():
        lat, lng = h3lib.cell_to_latlng(cell)
        r2 = args.h3_res + 2
        h3_r9 = h3lib.latlng_to_cell(lat, lng, r2)
        rows.append((
            h3lib.cell_to_int(cell), cell,
            int(h3lib.cell_to_int(h3_r9)), h3lib.int_to_cell(h3lib.cell_to_int(h3_r9)),
            round(lat, 6), round(lng, 6),
            stats["total"], stats["damaged"],
            round(float(sum(stats["t_vals"]) / len(stats["t_vals"])), 3),
            round(float(max(stats["t_vals"])), 3),
            round(stats["damaged"] / max(stats["total"], 1), 4),
            args.aoi, args.event_date, now,
        ))

    os.makedirs(OUT, exist_ok=True)
    out_path = f"{OUT}/damage_cells.parquet"

    db = duckdb.connect()
    db.execute("INSTALL spatial; LOAD spatial;")
    db.execute("SET geometry_always_xy = true;")
    db.execute("""
        CREATE TABLE staged (
            h3_index BIGINT, h3_hex VARCHAR,
            h3_r9 BIGINT, h3_r9_hex VARCHAR,
            latitude DOUBLE, longitude DOUBLE,
            pixel_count BIGINT, damaged_pixels BIGINT,
            avg_t_score DOUBLE, max_t_score DOUBLE,
            damage_ratio DOUBLE,
            aoi VARCHAR, event_date VARCHAR, snapshot_time VARCHAR
        )
    """)
    db.executemany(
        "INSERT INTO staged VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )

    db.execute(f"""
        COPY (
            SELECT
                h3_index, h3_hex, h3_r9, h3_r9_hex,
                latitude, longitude,
                pixel_count, damaged_pixels,
                avg_t_score, max_t_score, damage_ratio,
                aoi,
                event_date::DATE AS event_date,
                snapshot_time::TIMESTAMP AS snapshot_time,
                ST_SetCRS(ST_Point(longitude, latitude), 'EPSG:4326') AS geometry
            FROM staged
            ORDER BY damage_ratio DESC
        ) TO '{out_path}' (
            FORMAT PARQUET,
            COMPRESSION ZSTD,
            COMPRESSION_LEVEL 15,
            ROW_GROUP_SIZE 100000,
            GEOPARQUET_VERSION 'V2'
        )
    """)
    n = db.execute(f"SELECT COUNT(*) FROM read_parquet('{out_path}')").fetchone()[0]
    log.info("Wrote %s (%d rows)", out_path, n)


def main():
    args = parse_args()
    t0 = time.monotonic()

    if args.aoi not in AOI_PRESETS:
        log.error("Unknown AOI '%s'. Choices: %s", args.aoi, list(AOI_PRESETS))
        raise SystemExit(1)
    if args.catalog not in STAC_CATALOGS:
        log.error("Unknown catalog '%s'. Choices: %s",
                  args.catalog, list(STAC_CATALOGS))
        raise SystemExit(1)

    bbox = AOI_PRESETS[args.aoi]
    log.info("AOI: %s bbox=%s event=%s", args.aoi, bbox, args.event_date)

    if args.search_only or DRY_RUN:
        # Dry-run = STAC search only, no heavy compute
        event_dt = datetime.strptime(args.event_date, "%Y-%m-%d")
        pre_start = (event_dt - timedelta(days=args.pre_months * 30)).strftime("%Y-%m-%d")
        pre_end = args.event_date
        post_start = args.event_date
        post_end = (event_dt + timedelta(days=args.post_days)).strftime("%Y-%m-%d")
        pre, post = search_scenes(bbox, pre_start, pre_end, post_start, post_end, args.catalog)
        # Write a marker file so the validate step has something to check
        os.makedirs(OUT, exist_ok=True)
        marker = os.path.join(OUT, "search_summary.json")
        with open(marker, "w") as f:
            json.dump({
                "aoi": args.aoi, "bbox": bbox,
                "event_date": args.event_date,
                "pre_scenes": len(pre), "post_scenes": len(post),
                "catalog": args.catalog,
            }, f, indent=2)
        log.info("Search summary -> %s", marker)
        elapsed = time.monotonic() - t0
        log.info("Search-only complete (%.1fs)", elapsed)
        return

    run_full(args, bbox)
    elapsed = time.monotonic() - t0
    log.info("Extract complete (%.1fs)", elapsed)


if __name__ == "__main__":
    main()
