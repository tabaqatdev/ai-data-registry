"""Extract NASA EONET natural events for the Middle East region.

Two API calls: open events + closed events in the last 30 days. Each event's
geometry is a time series; we keep the latest point per event. Output is
flat GeoParquet 2.0 with H3 r5/r8/r10 attribute columns.

Append mode with snapshot_time in the unique key so every 6h run retains
its own snapshot, enabling historical "what events were open on day X".
"""

import json
import logging
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

import duckdb

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    level=logging.DEBUG if os.environ.get("DRY_RUN") else logging.INFO,
)
log = logging.getLogger(__name__)

OUT = os.environ.get("OUTPUT_DIR", "output")
DRY_RUN = os.environ.get("DRY_RUN", "0") == "1"

# Middle East + Gulf + Horn of Africa bbox: west,south,east,north
BBOX = "25,5,65,40"
DAYS_CLOSED = 30
EONET_BASE = "https://eonet.gsfc.nasa.gov/api/v3/events"


def fetch(url):
    req = urllib.request.Request(
        url, headers={"User-Agent": "ai-data-registry/nasa-eonet"}
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        log.error("HTTP %d from %s: %s", e.code, url, e.reason)
        raise


def flatten_events(events, snapshot):
    rows = []
    for event in events:
        geoms = event.get("geometry") or []
        if not geoms:
            continue
        latest = geoms[-1]
        coords = latest.get("coordinates") or [None, None]
        if len(coords) < 2 or coords[0] is None or coords[1] is None:
            continue
        lon, lat = coords[0], coords[1]
        cat = (event.get("categories") or [{}])[0]
        sources = event.get("sources") or []
        rows.append({
            "event_id": event["id"],
            "title": event.get("title") or "",
            "description": event.get("description") or "",
            "category_id": cat.get("id") or "",
            "category_title": cat.get("title") or "",
            "is_closed": event.get("closed") is not None,
            "closed_date": event.get("closed"),
            "longitude": lon,
            "latitude": lat,
            "magnitude_value": latest.get("magnitudeValue"),
            "magnitude_unit": latest.get("magnitudeUnit") or "",
            "event_date": latest.get("date") or "",
            "geometry_count": len(geoms),
            "source_count": len(sources),
            "source_ids": ";".join(s.get("id", "") for s in sources),
            "source_urls": ";".join(s.get("url", "") for s in sources),
            "snapshot_time": snapshot,
        })
    return rows


def main():
    t0 = time.monotonic()
    mode = "dry-run" if DRY_RUN else "extract"
    log.info("Starting %s, output_dir=%s", mode, OUT)

    os.makedirs(OUT, exist_ok=True)
    out_path = f"{OUT}/events.parquet"
    snapshot = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    url_open = f"{EONET_BASE}?status=open&bbox={BBOX}&limit=500"
    log.info("Fetching open events: %s", url_open)
    data_open = fetch(url_open)
    open_events = data_open.get("events", [])
    log.info("Open events: %d", len(open_events))

    url_closed = f"{EONET_BASE}?status=closed&bbox={BBOX}&days={DAYS_CLOSED}&limit=500"
    log.info("Fetching closed events: %s", url_closed)
    data_closed = fetch(url_closed)
    closed_events = data_closed.get("events", [])
    log.info("Closed events (last %dd): %d", DAYS_CLOSED, len(closed_events))

    all_events = open_events + closed_events
    if DRY_RUN:
        all_events = all_events[:20]
        log.info("DRY_RUN: limiting to %d events", len(all_events))

    rows = flatten_events(all_events, snapshot)
    log.info("Flattened to %d rows (one per event, latest geometry point)", len(rows))

    db = duckdb.connect()
    db.execute("INSTALL spatial; LOAD spatial;")
    db.execute("INSTALL h3 FROM community; LOAD h3;")
    db.execute("SET geometry_always_xy = true;")

    db.execute("""
        CREATE TABLE raw (
            event_id VARCHAR, title VARCHAR, description VARCHAR,
            category_id VARCHAR, category_title VARCHAR,
            is_closed BOOLEAN, closed_date VARCHAR,
            longitude DOUBLE, latitude DOUBLE,
            magnitude_value DOUBLE, magnitude_unit VARCHAR,
            event_date VARCHAR,
            geometry_count INTEGER, source_count INTEGER,
            source_ids VARCHAR, source_urls VARCHAR,
            snapshot_time VARCHAR
        )
    """)
    if rows:
        placeholders = ", ".join(["?"] * 17)
        db.executemany(
            f"INSERT INTO raw VALUES ({placeholders})",
            [tuple(r.values()) for r in rows],
        )

    log.info("Writing %s with H3 r5/r8/r10...", out_path)
    db.execute(f"""
        COPY (
            SELECT
                event_id, title, description,
                category_id, category_title,
                is_closed, closed_date::TIMESTAMP AS closed_date,
                longitude, latitude,
                magnitude_value, magnitude_unit,
                event_date::TIMESTAMP AS event_date,
                geometry_count, source_count,
                source_ids, source_urls,
                h3_latlng_to_cell(latitude, longitude, 5)::UBIGINT AS h3_r5,
                h3_latlng_to_cell(latitude, longitude, 8)::UBIGINT AS h3_r8,
                h3_latlng_to_cell(latitude, longitude, 10)::UBIGINT AS h3_r10,
                snapshot_time::TIMESTAMP AS snapshot_time,
                ST_SetCRS(ST_Point(longitude, latitude), 'EPSG:4326') AS geometry
            FROM raw
            ORDER BY ST_Hilbert(ST_Point(longitude, latitude)), event_date
        ) TO '{out_path}' (
            FORMAT PARQUET,
            COMPRESSION ZSTD,
            COMPRESSION_LEVEL 15,
            ROW_GROUP_SIZE 100000,
            GEOPARQUET_VERSION 'V2'
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
