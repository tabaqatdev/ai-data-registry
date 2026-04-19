# Watch-Center Migration Design

Migrate 9 legacy pipelines from `tabaqatdev/watch-center-pipeline` into the `ai-data-registry` workspace contract, one PR per workspace.

**Date:** 2026-04-19
**Status:** Draft for review

## Scope

9 candidate workspaces in the legacy repo. `open-meteo`, `opensky`, `weather` are already migrated and out of scope.

| Legacy | Target workspace | Shape |
|---|---|---|
| opensanctions | `opensanctions` | Non-spatial, daily CSV |
| nasa-firms | `nasa-firms` | Point events, 3 sensors, 24h rolling |
| nasa-eonet | `nasa-eonet` | Point events, daily |
| gdelt-sa | `gdelt-events` | Point events, S3-sourced, daily |
| celestrak | `celestrak-satellites` | Point positions, daily |
| ioda | `ioda-signals` | Time-series per country, daily |
| infra-osm | `infra-osm` | OSM lines/points, weekly |
| walkthru-indices | `walkthru-indices` | Pre-aggregated H3 grids, weekly |
| sar-damage | `sar-damage` | Event-driven, GPU SAR processing |

## 1. Methodology (per workspace)

Each workspace goes through 5 gates. Nothing writes to S3 until all 5 pass locally.

### Gate 1, source verification

For each endpoint, capture evidence into the card below:
- `curl -sI <url>` → `content-length`, `last-modified`, `cache-control`
- `curl -s <url>?<minimal> | head` → response shape
- `pixi run duckdb -c "DESCRIBE SELECT * FROM read_parquet('...')"` for S3 sources
- Record in this doc under "Gate 1 evidence" for each dataset

### Gate 2, contract decisions

Per dataset: cron schedule, `mode` (append | replace), geographic scope, backend + flavor, H3 strategy, per-table checks, storage target. Recorded below.

### Gate 3, contract draft

`[tool.registry]` block in `workspaces/<name>/pixi.toml` per `.claude/rules/workspace-contract.md`. Schema name verified unique via:
```bash
pixi run duckdb -c "SELECT schema, table FROM read_parquet('workspaces/*/pixi.toml')" # manual cross-check
```

### Gate 4, extract + dry-run

- `extract.py` or `extract.sql` per `.claude/rules/extract-patterns.md`
- Pure DuckDB SQL where endpoint is HTTP GET + JSON/CSV with no auth
- Python + `urllib` when batching, POST, auth, or pagination is needed
- Writes flat `$OUTPUT_DIR/<table>.parquet` per declared table
- `DRY_RUN=1` path produces smaller-but-valid output

### Gate 5, output validation (local, before PR)

```bash
cd workspaces/<name>
OUTPUT_DIR=output pixi run pipeline

# GeoParquet 2.x compliance + schema checks
pixi run --manifest-path ../../pixi.toml gpio check all output/<table>.parquet

# Uniqueness of declared unique_cols
pixi run duckdb -c "SELECT COUNT(*) total, COUNT(DISTINCT (<unique_cols>)) unique FROM 'output/<table>.parquet'"

# Eyeball 5 rows + column types
pixi run duckdb -c "DESCRIBE SELECT * FROM 'output/<table>.parquet' LIMIT 0"
pixi run duckdb -c "SELECT * FROM 'output/<table>.parquet' USING SAMPLE 5"

# Same checks the CI will run
python validate_local.py
```

## 2. Project-wide rules (new conventions)

These apply to every new workspace, including the 9 being migrated.

### 2.1 GeoParquet 2.0 as default output

All spatial outputs use `GEOPARQUET_VERSION 'V2'` in DuckDB `COPY`, which writes Parquet 2.11+ files with the native `GEOMETRY`/`GEOGRAPHY` logical types (GeoParquet 2.0.0 spec). No manual `bbox` column is needed, the native column statistics provide spatial filter pushdown.

Non-spatial outputs skip geometry but keep ZSTD + 100K row groups.

### 2.1a Historical retention via `mode = "append"`

When a dataset has a time dimension or the upstream state changes across runs (entities added/removed, events appearing, signals updating), the workspace uses `mode = "append"` with a `snapshot_time` (TIMESTAMP) column — and, for daily or slower cadence, a `snapshot_date` (DATE) column as part of the unique key. Each run produces a timestamped Parquet file on S3; `merge_catalog.py` registers the new file in DuckLake without dropping the previous one. Consumers reconstruct "state as of day X" with a simple `WHERE snapshot_date = ...` filter.

`mode = "replace"` is reserved for datasets where upstream genuinely replaces state (e.g. OSM infra snapshot, pre-aggregated indices). Even then, files on S3 are retained by the CI's timestamped filenames; only the DuckLake registration is pruned to the latest file.

### 2.2 Multi-resolution H3 attribute strategy

For **point datasets** (events, detections, positions), store 3 H3 cell IDs as `BIGINT` columns alongside the geometry, not replacing it:

- `h3_r5` → country-level aggregation (~8.5 km edge)
- `h3_r8` → neighborhood-level (~460 m edge)
- `h3_r10` → building-cluster-level (~66 m edge)

Rationale:
- Each column is 8 bytes per row, trivially cheap (~24 B/row total)
- Users choose aggregation resolution at query time without recomputing
- Geometry stays for accurate spatial joins and rendering

For **aggregated grid datasets** (pre-aggregated H3 cells, one row per cell), drop geometry from the output. Consumers can reconstruct it from the cell ID via `h3_cell_to_boundary_wkt()`. Saves 50-70% file size.

For **non-spatial datasets** (IODA country signals, OpenSanctions entities), no H3 columns.

### 2.3 Spatial sort + bbox covering

All point outputs end with:
```sql
ORDER BY ST_Hilbert(geometry), <time_col>
```

gpio `bbox` covering is added via `gpio add bbox` if the extract does not already compute it.

### 2.4 CRS discipline

All geometries created with CRS attached at creation time, never at export:
```sql
ST_SetCRS(ST_Point(lon, lat), 'EPSG:4326') AS geometry
```

### 2.5 Geographic scope per dataset class

| Class | Default scope | Rationale |
|---|---|---|
| Fast-moving point events | SA + Gulf bbox (25-62°E, 12-38°N) | Keeps file size + API weight manageable |
| Pre-aggregated indices | SA+Gulf bbox filtered from global parent | Source data already exists globally on source.coop |
| Global reference catalogs | Global, tag region at row level | Small totals (e.g. satellites); region flag is cheap |
| Non-spatial country rollups | 15 Gulf/ME countries | Matches existing legacy scope |

## 3. Migration order

Batches are independent. Each batch is a separate PR.

| Batch | Workspace | Rationale |
|---|---|---|
| 1 | opensanctions | No geometry, single CSV, reference for non-spatial workspaces |
| 2 | nasa-firms | Point + multi-res H3 reference, no auth |
| 3 | nasa-eonet | Same shape as firms, lower volume |
| 4 | gdelt-events | Validates S3-source read pattern |
| 5 | celestrak-satellites | Global reference catalog pattern |
| 6 | ioda-signals | Multi-country fan-out, no geometry |
| 7 | infra-osm | Heaviest, needs Hetzner backend, weekly |
| 8 | walkthru-indices | Pre-aggregated H3 grids |
| 9 | sar-damage | Event-driven GPU workload, HuggingFace backend |

---

## 4. Per-dataset cards

### 4.1 opensanctions

**Source:** `https://data.opensanctions.org/datasets/latest/default/targets.simple.csv`

**Gate 1 evidence (2026-04-19):**
- HTTP 200, `content-length: 456,244,442` (435 MB CSV)
- `last-modified: Sun, 19 Apr 2026 08:24:28 GMT` (less than 5 hours old at check time)
- `cache-control: public, max-age=86400` → refreshed daily
- License: CC-BY-4.0 (OpenSanctions consolidated default dataset)

**Gate 2 decisions:**
- Schedule: `30 10 * * *` (daily, 10:30 UTC, after upstream 08:24 refresh)
- Mode: `append` with a `snapshot_date` column so DuckLake retains the full daily history (entities are added / removed / re-classified frequently; historical state needs to be queryable). Unique key: `(id, snapshot_date)`.
- Scope: 18 country codes including SA + Iran + Iraq + GCC + major sanctioning contexts
- Backend: `github` / `ubuntu-latest`
- H3 strategy: none (non-spatial)
- Storage: default (eu-hetzner)

**Tables:**
- `region_sanctions` — one row per sanctioned entity linked to the region

**Contract:**
```toml
[tool.registry]
description = "OpenSanctions entities linked to SA and influence region (Gulf, ME, major sanctioning contexts)"
schedule = "30 10 * * *"
timeout = 20
tags = ["sanctions", "compliance", "entities", "middle-east"]
schema = "opensanctions"
table = "region_sanctions"
mode = "append"

[tool.registry.license]
code = "Apache-2.0"
data = "CC-BY-4.0"
data_source = "OpenSanctions"
mixed = false

[tool.registry.checks]
min_rows = 5000
max_null_pct = 60
unique_cols = ["id", "snapshot_date"]
```

**Extract strategy:** Pure DuckDB SQL. `read_csv()` pulls directly from HTTPS, filters by country list, writes flat Parquet.

**Risks:**
- 454 MB CSV parsed every run. `read_csv` streaming keeps memory bounded; github runner has 7 GB RAM.
- Dataset schema can add columns upstream. Pin explicit `SELECT` list, fail loudly on missing columns.

---

### 4.2 nasa-firms

**Sources (3 sensor CSVs):**
- `https://firms.modaps.eosdis.nasa.gov/data/active_fire/suomi-npp-viirs-c2/csv/SUOMI_VIIRS_C2_Global_24h.csv`
- `https://firms.modaps.eosdis.nasa.gov/data/active_fire/noaa-20-viirs-c2/csv/J1_VIIRS_C2_Global_24h.csv`
- `https://firms.modaps.eosdis.nasa.gov/data/active_fire/modis-c6.1/csv/MODIS_C6_1_Global_24h.csv`

**Gate 1 evidence (2026-04-19):**
- All 3 return HTTP 200
- Sizes: 4.5 MB, 4.5 MB, 0.7 MB (global 24h)
- `last-modified` updates within the last hour on all 3 (continuous ingestion)
- NOAA-20 VIIRS: 54,233 global rows in the current 24h window
- Schema confirmed: `latitude`, `longitude`, `bright_ti4/5`, `frp`, `confidence`, `acq_date`, `acq_time`, `scan`, `track`, `daynight`, `satellite`, `version`
- No API key needed for the 24h rolling endpoint
- License: "NASA data, no restrictions" → map to CC0-1.0

**Gate 2 decisions:**
- Schedule: `0 */3 * * *` (every 3 hours; upstream refreshes faster but we sample 8x/day to control row volume and S3 file count)
- Mode: `append` (each snapshot is a time-tagged slice, dedup on `(sensor, latitude, longitude, acq_date, acq_time, snapshot_time)`)
- Scope: SA+Gulf bbox `12-38°N, 25-62°E`. Expected ~200-2000 fires/run regionally out of ~50-100K global.
- Backend: `github` / `ubuntu-latest`
- H3 strategy: `h3_r5`, `h3_r8`, `h3_r10` attribute columns on each fire point
- Storage: default

**Tables:**
- `active_fires` — one row per fire detection with geometry + multi-res H3

**Contract:**
```toml
[tool.registry]
description = "Active fire detections from 3 NASA FIRMS sensors (SUOMI VIIRS, NOAA-20 VIIRS, MODIS) for SA + Gulf region, 24h rolling"
schedule = "0 */3 * * *"
timeout = 15
tags = ["fires", "nasa", "firms", "viirs", "modis", "middle-east", "emergency"]
schema = "nasa_firms"
table = "active_fires"
mode = "append"

[tool.registry.license]
code = "Apache-2.0"
data = "CC0-1.0"
data_source = "NASA FIRMS"
mixed = false

[tool.registry.checks]
min_rows = 0
max_null_pct = 10
geometry = true
unique_cols = ["sensor", "latitude", "longitude", "acq_date", "acq_time", "snapshot_time"]
schema_match = true
```

Note: `min_rows = 0` because fire activity is genuinely zero on quiet winter days.

**Extract strategy:** Pure DuckDB SQL via `read_csv(url)`, three sensors unioned, bbox filter, compute H3 via `h3_latlng_to_cell(lat, lon, res)` at 5/8/10, tag `snapshot_time = NOW()`, write flat Parquet with Hilbert sort.

**Risks:**
- MODIS sensor is aging (Aqua/Terra spacecraft in extended mission). If it drops offline, the UNION should gracefully skip that source. Plan: wrap each CSV read in a try block, log and continue on failure.
- Small-runs problem with `min_rows = 0`: quiet days still produce valid 0-row Parquet. `validate_output.py` must handle empty files.

---

### 4.3 nasa-eonet

**Source:** `https://eonet.gsfc.nasa.gov/api/v3/events`

**Gate 1 evidence (2026-04-19):**
- API live, JSON response with nested `geometry` time-series per event
- Rate limit: 60 req/min (`x-ratelimit-limit: 60`)
- No auth
- Sample event verified (Hayli Gubbi volcano, Ethiopia)
- Categories endpoint returns canonical list (drought, dustHaze, earthquakes, floods, volcanoes, wildfires, etc.)
- License: no explicit restrictions, treat as public domain → CC0-1.0

**Gate 2 decisions:**
- Schedule: `0 */6 * * *` (every 6h; events are low-frequency)
- Mode: `append` (snapshot each run, dedup on `(event_id, snapshot_time)`)
- Scope: ME bbox `25-65°E, 5-40°N` (wider than SA to capture Horn of Africa context)
- Backend: `github` / `ubuntu-latest`
- H3 strategy: `h3_r5`, `h3_r8`, `h3_r10` on the latest geometry point per event
- Storage: default

**Tables:**
- `events` — one row per event's latest geometry point

**Contract:**
```toml
[tool.registry]
description = "NASA EONET natural events (wildfires, volcanoes, storms, etc.) for the ME region, open + last 30d closed"
schedule = "0 */6 * * *"
timeout = 10
tags = ["natural-events", "nasa", "eonet", "wildfires", "volcanoes", "middle-east"]
schema = "nasa_eonet"
table = "events"
mode = "append"

[tool.registry.license]
code = "Apache-2.0"
data = "CC0-1.0"
data_source = "NASA EONET"
mixed = false

[tool.registry.checks]
min_rows = 0
max_null_pct = 40
geometry = true
unique_cols = ["event_id", "snapshot_time"]
```

(High `max_null_pct` because EONET `description`, `magnitude_value`, and `magnitude_unit` are null on most event types.)

**Extract strategy:** Python + urllib (two calls: open + closed with `days=30`), flatten to rows keeping latest geometry, write via DuckDB with H3 + Hilbert sort.

---

### 4.4 gdelt-events (rename from gdelt-sa)

**Source:** `s3://us-west-2.opendata.source.coop/tabaqat/gdelt-sa/country=SA/year=YYYY/*.parquet` (Source Cooperative, public bucket, hive-partitioned)

**Gate 1 evidence (2026-04-19):**
- S3 read succeeds with `s3_region='us-west-2'; s3_url_style='path'`
- 1,678 rows for country=SA in `SQLDATE >= 20260401` (9 days)
- Rich schema: GDELT event codes + Actor1/Actor2 geo + NearestCity + ArticleContent + ArticleTitle + ArticleScrapeMethod
- Hive-partitioned on `country=` and `year=` → DuckDB reads this natively
- License: GDELT 1.0 events are "unlimited and unrestricted use for any academic, commercial, or governmental use" (GDELT official terms) → map to CC0-1.0. Article content is dropped from the output to avoid third-party copyright concerns. URLs only.

**Gate 2 decisions:**
- Schedule: `0 4 * * *` (daily, 04:00 UTC — after GDELT's daily refresh cycle completes around 03:00 UTC)
- Mode: `append`. Each run captures the last 7 days; dedup on `(GLOBALEVENTID, SQLDATE)`.
- Scope: `country=SA` partition only for now. Consider extending to `country IN (SA, AE, IR, IQ, EG, YE, JO)` in a later PR; adds partitions, not columns.
- Article content: **dropped**. Output keeps `SOURCEURL` but not `ArticleContent`/`ArticleTitle` to stay within unrestricted-use scope and avoid third-party publisher copyright.
- Backend: `github` / `ubuntu-latest`
- H3 strategy: `h3_r5`, `h3_r8`, `h3_r10` on `ActionGeo_Lat/Long`
- Storage: default

**Tables:**
- `events` — one row per GDELT event in SA with geometry + H3 + event metadata + `SOURCEURL`

**Contract:**
```toml
[tool.registry]
description = "GDELT events for Saudi Arabia (event metadata + source URLs), last 7 days rolling"
schedule = "0 4 * * *"
timeout = 20
tags = ["gdelt", "events", "news", "saudi-arabia"]
schema = "gdelt_events"
table = "events"
mode = "append"

[tool.registry.license]
code = "Apache-2.0"
data = "CC0-1.0"
data_source = "GDELT Project (via Source Cooperative tabaqat/gdelt-sa)"
mixed = false

[tool.registry.checks]
min_rows = 100
max_null_pct = 50
geometry = true
unique_cols = ["GLOBALEVENTID", "SQLDATE"]
```

**Extract strategy:** Pure DuckDB SQL. Read from Source Coop with hive partitioning, filter by `SQLDATE >= current_date - 7`, compute H3, write flat Parquet.

**Risks:**
- Source Coop bucket availability out of our control. Fallback: bail with clear error, let scheduler retry next day.
- `NearestCity`, `CityPopulation`, etc. are GDELT-CNG derived fields that only exist in the tabaqat fork. Pin schema to this source explicitly.

---

### 4.5 celestrak-satellites

**Source:** `https://celestrak.org/NORAD/elements/gp.php?GROUP=<group>&FORMAT=json`

**Gate 1 evidence (2026-04-19):**
- `active` group returns 15,141 satellites as JSON
- `stations` group verified (ISS, CSS, various modules)
- No auth, no rate limit headers
- Fields per row: OBJECT_NAME, NORAD_CAT_ID, EPOCH, MEAN_MOTION, ECCENTRICITY, INCLINATION, RA_OF_ASC_NODE, ARG_OF_PERICENTER, MEAN_ANOMALY, BSTAR, CLASSIFICATION_TYPE
- License: CelesTrak data is US Space Force public domain → CC0-1.0
- Sub-satellite point must be **computed client-side** (TLE propagation). Old code uses a simplified mean-motion calculation.

**Gate 2 decisions:**
- Schedule: `0 */6 * * *` (every 6h; orbital elements drift, sub-satellite points need regular refresh)
- Mode: `append`, dedup on `(norad_cat_id, snapshot_utc)`
- Scope: global catalog, tag `over_region` flag on each row (no filter — totals are small)
- Backend: `github` / `ubuntu-latest`
- H3 strategy: `h3_r5` only (sub-satellite points are approximate; fine-grained H3 is misleading)
- Storage: default

**Tables:**
- `satellites` — one row per satellite per snapshot with orbital elements + computed sub-satellite position

**Contract:**
```toml
[tool.registry]
description = "Satellite orbital elements and sub-satellite positions from CelesTrak, 7 priority groups"
schedule = "0 */6 * * *"
timeout = 15
tags = ["satellites", "celestrak", "orbital", "space", "global"]
schema = "celestrak"
table = "satellites"
mode = "append"

[tool.registry.license]
code = "Apache-2.0"
data = "CC0-1.0"
data_source = "CelesTrak / US Space Force"
mixed = false

[tool.registry.checks]
min_rows = 10000
max_null_pct = 20
geometry = true
unique_cols = ["norad_cat_id", "snapshot_utc"]
```

**Extract strategy:** Python + urllib (7 groups fetched in sequence), compute sub-satellite lat/lon/alt, write via DuckDB. Keep the simplified mean-motion propagation from the legacy code; an accurate SGP4 implementation is a future improvement.

**Risks:**
- The old simplified formula diverges from truth for high-eccentricity orbits and over long time-since-epoch windows. Accept this for a watch-center "approximate ground track" use case; flag as approximate in field naming (`latitude_approx`, `longitude_approx`).

---

### 4.6 ioda-signals

**Source:** `https://api.ioda.inetintel.cc.gatech.edu/v2/signals/raw/country/<code>?from=<epoch>&until=<epoch>`

**Gate 1 evidence (2026-04-19):**
- API returns JSON with `data[]` → nested list of time-series per datasource (gtr, gtr-sarima, gtr-norm, merit-nt, bgp, ping-slash24, ping-slash24-loss, ping-slash24-latency, upstream-delay-*, mozilla)
- 7-day window for SA: ~409 KB response across 11 datasources × ~336 points each
- `step: 1800` → 30-minute samples
- No auth, rate-limit not documented but polite use works
- License: IODA / Georgia Tech ACIS — "permission to copy, modify, and distribute this software and its documentation for academic research and education purposes, without fee" (CAIDA + Georgia Tech license). Non-commercial academic research only. Map to CC-BY-NC-4.0 as the closest SPDX identifier. (`validate_manifest.py` RESTRICTIVE_DATA_LICENSES warns but accepts.)

**Gate 2 decisions:**
- Schedule: `30 */4 * * *` (every 4 hours, offset 30 min to not collide with FIRMS)
- Mode: `append`, window = last 48h per run, dedup on `(country_code, datasource, subtype, timestamp)`
- Scope: 15 ME/Gulf country codes (same as legacy)
- Backend: `github` / `ubuntu-latest`
- H3 strategy: none (country-keyed, not coordinate-keyed)
- Storage: default

**Tables:**
- `signals` — raw time-series points, one row per (country, datasource, subtype, timestamp)

**Contract:**
```toml
[tool.registry]
description = "IODA internet-outage signals for 15 ME/Gulf countries (BGP, active probing, Google Transparency, MERIT-NT), last 48h rolling"
schedule = "30 */4 * * *"
timeout = 15
tags = ["internet-outages", "ioda", "connectivity", "middle-east", "bgp", "network"]
schema = "ioda"
table = "signals"
mode = "append"

[tool.registry.license]
code = "Apache-2.0"
data = "CC-BY-NC-4.0"
data_source = "IODA (Georgia Tech ACIS / CAIDA)"
mixed = false
```

Note: IODA's actual CAIDA + Georgia Tech license restricts use to "academic research and education purposes." CC-BY-NC-4.0 is the closest recognized SPDX identifier. The manifest validator flags restrictive licenses with a warning for reviewer attention but accepts them. If the project later obtains a commercial-use license from Georgia Tech OTL, upgrade to CC-BY-4.0.

```toml
[tool.registry.checks]
min_rows = 1000
max_null_pct = 40
unique_cols = ["country_code", "datasource", "subtype", "timestamp"]
```

**Extract strategy:** Python + urllib per country (15 requests), flatten nested time-series structure, write via DuckDB.

**Risks:**
- IODA's response structure is quirky (nested list-of-lists-or-dicts). Old code handles this in `flatten_signals()`; port the flatten logic verbatim and add a DRY_RUN path that exercises both shapes.
- Anomaly detection (z-score computation) was in legacy `to_parquet.sql`. **Decision: drop for MVP.** Consumers can compute z-scores downstream at query time against the raw `signals` table (DuckDB window functions make this trivial). Keeps the extract focused on capture, not derivation.

---

### 4.7 infra-osm

**Source:** OpenStreetMap via Geofabrik PBF extracts

**Gate 1 evidence (2026-04-19):**
- `https://download.geofabrik.de/asia/gcc-states-latest.osm.pbf` → HTTP 302 to timestamped file (`gcc-states-260418.osm.pbf`)
- Direct `saudi-arabia-latest.osm.pbf` and `arabian-peninsula-latest.osm.pbf` paths currently 302 to root (those sub-extracts are not published by Geofabrik under those exact names). QuackOSM abstracts this: pass a bbox, it picks the best available parent extract and downloads.
- License: ODbL-1.0 (OSM)
- PBF sizes: Geofabrik GCC states is ~250-400 MB per region; full ME is larger

**Gate 2 decisions:**
- Schedule: `0 3 1 1 *` (annual, 1 Jan 03:00 UTC — core infrastructure (power lines, pipelines, petroleum sites) changes on the year scale; annual keeps CI and S3 costs in check. Upgrade to quarterly if SA's Vision 2030 buildout produces visible drift)
- Mode: `replace` (weekly full refresh, not incremental — OSM edits are everywhere)
- Scope: SA + Gulf bbox `25-62°E, 12-38°N`
- Backend: `hetzner` / `cax21` (4 cores, 8 GB — PBF parsing is memory-heavy)
- H3 strategy: `h3_r8` + `h3_r10` on representative point of each feature (centroid for polygons, first vertex for lines)
- Storage: default

**Tables:** one per infrastructure category (from legacy):
- `power_lines`, `power_plants`, `power_generators`, `substations`, `power_towers`, `switchgear`
- `pipelines`, `petroleum_sites`, `petroleum_wells`
- `telecoms`
- `water_infra`

11 tables declared in `tables = [...]`. Per-table `min_rows` varies widely (power_lines: 10,000; petroleum_wells: 100).

**Contract skeleton:**
```toml
[tool.registry]
description = "OSM infrastructure for SA + Gulf region: power, petroleum, pipelines, telecoms, water"
schedule = "0 3 * * 0"
timeout = 90
tags = ["osm", "infrastructure", "power", "petroleum", "pipelines", "telecoms", "water", "middle-east"]
schema = "infra_osm"
tables = ["power_lines", "power_plants", "power_generators", "substations", "power_towers", "switchgear", "pipelines", "petroleum_sites", "petroleum_wells", "telecoms", "water_infra"]
mode = "replace"

[tool.registry.runner]
backend = "hetzner"
flavor = "cax21"

[tool.registry.license]
code = "Apache-2.0"
data = "ODbL-1.0"
data_source = "OpenStreetMap via Geofabrik"
mixed = false

[tool.registry.checks]
schema_match = true
geometry = true
```

Per-table checks set min_rows based on expected feature density.

**Extract strategy:** Python + QuackOSM (`convert_geometry_to_geodataframe`), then pass through DuckDB for H3 addition and Hilbert sort. Keep the legacy FORCE-skip-if-exists pattern, but inverted: always re-extract in CI (CI starts from empty state).

**Risks:**
- QuackOSM has a known DuckDB `GEOMETRY('OGC:CRS84')` cast bug for multi-extract regions. Legacy code uses the GeoDataFrame path to avoid this. Preserve that workaround.
- 90-minute timeout: Hetzner cax21 takes ~60-75 min to process GCC states for all 11 categories. Leave margin.
- QuackOSM caches PBFs under `/tmp` in CI; cache does not persist across runs, so every run re-downloads. Trade-off: predictable, no staleness.

---

### 4.8 walkthru-indices

**Source:** `https://data.source.coop/walkthru-earth/...` (public Cloudflare-backed Parquet files)

**Gate 1 evidence (2026-04-19):**
- Population r5 SSP2: 25.4 MB, last-modified 2026-03-05
- Population r6 SSP2: 138.8 MB, last-modified 2026-03-05
- Buildings r5: 13.2 MB, last-modified 2026-03-05
- Terrain r5: 10.2 MB, last-modified 2026-03-05
- All respond HTTP 200 with full `accept-ranges: bytes` support
- License: walkthru-earth indices — `data = "CC-BY-4.0"` (confirmed on source.coop dataset page for v2 family)
- Upstream refreshes are rare (monthly to quarterly); cron weekly is conservative

**Gate 2 decisions:**
- Schedule: `0 4 * * 1` (weekly, Monday 04:00 UTC)
- Mode: `replace` (always full snapshot of current state)
- Scope: SA+Gulf bbox filter applied to global parent files
- Backend: `github` / `ubuntu-latest`
- H3 strategy: **keep h3_r5 as primary key** (source data is already indexed at r5). Drop geometry column; consumers reconstruct from cell ID.
- Storage: default

**Tables:**
- `population_h3r5` — 16 pop columns (pop_2025 through pop_2100 by 5-year steps)
- `buildings_h3r5` — building_count, density, footprint, heights, volumes
- `terrain_h3r5` — elev, slope, aspect, tri, tpi

Open: should we also export unified-joined `unified_h3r5`? Legacy code produces it. Decision: **no**, consumers can join the three tables in DuckLake. Skip unified to avoid duplication.

**Contract:**
```toml
[tool.registry]
description = "Pre-aggregated H3 res-5 indices for SA + Gulf: population (2025-2100, SSP2), buildings, terrain"
schedule = "0 4 * * 1"
timeout = 15
tags = ["h3", "population", "buildings", "terrain", "walkthru-earth", "middle-east"]
schema = "walkthru_indices"
tables = ["population_h3r5", "buildings_h3r5", "terrain_h3r5"]
mode = "replace"

[tool.registry.license]
code = "Apache-2.0"
data = "CC-BY-4.0"
data_source = "Walkthru.Earth Indices v2"
mixed = false

[tool.registry.checks]
schema_match = true

[tool.registry.checks.population_h3r5]
min_rows = 5000
unique_cols = ["h3_index"]

[tool.registry.checks.buildings_h3r5]
min_rows = 5000
unique_cols = ["h3_index"]

[tool.registry.checks.terrain_h3r5]
min_rows = 10000
unique_cols = ["h3_index"]
```

**Extract strategy:** Pure DuckDB SQL. Three `COPY` statements reading from remote Parquet with `h3_cell_to_lat/lng` bbox filter, writing flat Parquet per table. No geometry column in output.

**Risks:**
- `h3_cell_to_lat(h3_index) BETWEEN ...` applied to a 500M+ row file is a full scan. Expected ~2-5 min per table on github runner. Acceptable.

**Future:** Consider adding r6/r8 variants if source publishes them at those resolutions. r6 population is 138 MB vs r5 at 25 MB; feasible.

---

### 4.9 sar-damage

**Source:** STAC (`https://earth-search.aws.element84.com/v1`, `sentinel-1-grd`) + Sentinel-1 GRD COGs on AWS

**Gate 1 evidence (2026-04-19):**
- STAC API live, `sentinel-1-grd` collection valid
- Global temporal coverage since 2014-10-10, ongoing
- `storage:requester_pays: true` on the AWS source → **S3 egress is charged to the reader** when accessing items from `eu-central-1` outside AWS. Mitigations:
  - Run on HuggingFace Jobs backend (container can co-locate)
  - Or use Element84's redirect to actual COG URLs which are public
- License: Copernicus Sentinel data terms (ESA free + open for commercial/non-commercial use)

**Gate 2 decisions:**
- Schedule: **none** (event-driven, not cron). Use `workflow_dispatch` only for MVP.
- Mode: `append` (each run is one AOI + one event date)
- Scope: parameterized AOI from preset list (Iran cities, Gaza, Yemen, Syria, Lebanon, Iraq)
- Backend: `huggingface` / `t4-small` (GPU speeds up convolution + t-test, but not strictly needed). Start `cpu-upgrade` to keep cost down.
- H3 strategy: `h3_r7` + `h3_r9` aggregation (source pixel res is 20 m; r9 edge is 175 m)
- Storage: default

**Tables:**
- `damage_cells` — one row per H3 cell with pixel counts, t-statistics, damage ratio, optional population/buildings join

**Contract:**
```toml
[tool.registry]
description = "SAR damage detection (Pixel-Wise T-Test on Sentinel-1 GRD) for conflict AOIs in the ME region"
# Event-driven, not cron-scheduled
schedule = ""
timeout = 120
tags = ["sar", "damage-detection", "sentinel-1", "conflict", "middle-east"]
schema = "sar_damage"
table = "damage_cells"
mode = "append"

[tool.registry.runner]
backend = "huggingface"
flavor = "cpu-upgrade"
image = "ghcr.io/<owner>/<repo>/sar-damage:latest"

[tool.registry.license]
code = "Apache-2.0"
data = "CC-BY-4.0"
data_source = "Copernicus Sentinel-1 (ESA)"
mixed = false

[tool.registry.checks]
min_rows = 0
max_null_pct = 20
geometry = true
unique_cols = ["h3_hex", "aoi", "event_date"]
```

**Extract strategy:** Python + pystac-client + odc-stac + scipy. Input params via env: `SAR_AOI`, `SAR_EVENT_DATE`, `SAR_PRE_MONTHS`, `SAR_POST_DAYS`, `SAR_T_THRESHOLD`, `SAR_H3_RES`. Writes a single Parquet keyed on `(aoi, event_date)`.

**Risks:**
- `schedule = ""` is accepted by `validate_manifest.py` (empty schedule is valid; it skips the cron parser). The scheduler simply won't auto-dispatch this workspace, which is exactly what we want for event-driven work. No contract change needed.
- HuggingFace backend writes directly to S3 (break in write-isolation, accepted per MAINTAINING.md). Fine.
- Cold start for odc-stac/rioxarray inside a container adds ~30-60s overhead; keep timeout at 120 min.
- `requester_pays` for eu-central-1 COGs: element84 search returns items whose `href` points to the `s3://sentinel-s1-l1c/` bucket. Verify via a dry-run search whether odc-stac honors redirect to the free HTTPS mirror. If not, switch catalog to ESA EOPF or Microsoft Planetary Computer.

**Open question for planning:** Confirm `workflow_dispatch`-only is acceptable (MVP has no scheduled SAR runs). This is a user-of-the-workspace UX question, not a contract question.

---

## 5. What's out of scope

- Already-migrated workspaces: `open-meteo`, `opensky`, `weather`. A separate diff task will compare the legacy versions against current `ai-data-registry` versions and file any missing coverage as follow-up issues.
- New workspaces not in the legacy repo.
- Cross-workspace joins that legacy code did (e.g., fires ⋈ walkthru population). In the new world, these happen at query time against the DuckLake catalog, not at extract time. Each workspace publishes its own table independently.

## 6. Success criteria

- 9 PRs merged, one per workspace, each passing all 4 CI validation layers.
- For each merged workspace, at least one successful scheduled run produces data visible in the global DuckLake catalog.
- `pixi run gpio check all` passes on every produced GeoParquet file.
- No workspace shares a `schema.table` with another.
- Extract times stay under declared `timeout` on 3 consecutive scheduled runs.

## 7. Resolved questions

All questions from draft review have been resolved. See the updated §4 cards for the final decisions.

1. **IODA license:** Resolved to `CC-BY-NC-4.0`. The CAIDA + Georgia Tech license restricts use to academic research and education; CC-BY-NC-4.0 is the closest recognized SPDX identifier and is accepted (with a warning) by `validate_manifest.py`.
2. **GDELT `mixed = true`:** Resolved. `ArticleContent` and `ArticleTitle` are dropped from the output. Only `SOURCEURL` is kept. License is `CC0-1.0` (GDELT events are unrestricted), `mixed = false`.
3. **Unified walkthru table:** Resolved. Not produced. Consumers join the three component tables at query time in DuckLake. Avoids storage duplication.
4. **Migration ordering:** Resolved. Proceed in dependency-free order (opensanctions → firms → eonet → gdelt → celestrak → ioda → infra-osm → walkthru → sar-damage). Business priority can be re-ordered later without changing the per-workspace design.
5. **IODA z-score anomalies:** Resolved. Dropped from the extract. Raw `signals` rows + DuckDB window functions let consumers compute anomalies at query time.

## 8. Next step

After maintainer approval of this spec, hand off to the `superpowers:writing-plans` skill to produce a per-workspace implementation plan with step-by-step tasks. Each workspace becomes its own plan file and its own PR.
