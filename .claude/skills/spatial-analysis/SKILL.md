---
name: spatial-analysis
description: >
  Geospatial analysis with DuckDB spatial (155+ ST_* functions) and unified GDAL CLI.
  Use when the user asks about spatial queries, geometry ops, coordinate transforms,
  distance/area calculations, spatial joins, or any map data processing.
allowed-tools: Read, Write, Edit, Glob, Grep, Bash
---

All tools via pixi: `pixi run duckdb`, `pixi run gdal`.

## Step 0 — Discover available ST_* functions (MUST run first)

Before writing any spatial query, run this to get the latest function signatures directly from the engine:

```sql
LOAD spatial;
SELECT function_name, function_type,
       STRING_AGG(DISTINCT return_type, ', ') AS return_types,
       COUNT(*) AS overloads,
       FIRST(description) AS description
FROM duckdb_functions()
WHERE function_name LIKE 'ST_%'
GROUP BY function_name, function_type
ORDER BY function_name;
```

This is the **authoritative source** — it reflects the exact version installed, including any new functions added in updates. Use it to verify parameter names, return types, and overloads before composing queries. To look up a specific function, add `AND function_name = 'ST_Transform'`.

## ST_* Quick Reference (113 functions)

| Category | Functions |
|----------|-----------|
| **Constructors** | `ST_Point`, `ST_MakeLine`, `ST_MakePolygon`, `ST_MakeEnvelope`, `ST_Collect`, `ST_Multi` |
| **Serialization** | `ST_GeomFromText`, `ST_GeomFromGeoJSON`, `ST_AsGeoJSON`, `ST_AsHEXWKB`, `ST_AsSVG` |
| **Measurement** | `ST_Area`, `ST_Length`, `ST_Distance`, `ST_Perimeter` + `_Spheroid`/`_Sphere` variants |
| **Predicates** | `ST_Contains`, `ST_Intersects`, `ST_Within`, `ST_Crosses`, `ST_Touches`, `ST_DWithin`, `ST_Overlaps`, `ST_Equals` |
| **Operations** | `ST_Buffer`, `ST_Union`, `ST_Intersection`, `ST_Difference`, `ST_Simplify`, `ST_ConvexHull`, `ST_ConcaveHull`, `ST_BuildArea` |
| **Coordinates** | `ST_X`, `ST_Y`, `ST_Z`, `ST_M`, `ST_XMin/Max`, `ST_YMin/Max`, `ST_ZMin/Max` |
| **Transform** | `ST_Transform`, `ST_FlipCoordinates`, `ST_Force2D/3DZ/3DM/4D`, `ST_Rotate`, `ST_Scale`, `ST_Translate` |
| **Line ops** | `ST_LineInterpolatePoint`, `ST_LineLocatePoint`, `ST_LineSubstring`, `ST_LineMerge`, `ST_ShortestLine` |
| **Indexing** | `ST_Hilbert`, `ST_QuadKey`, `ST_TileEnvelope` |
| **Coverage** | `ST_CoverageUnion`, `ST_CoverageSimplify`, `ST_CoverageInvalidEdges` + `_Agg` variants |
| **I/O** | `ST_Read`, `ST_ReadOSM`, `ST_ReadSHP`, `ST_Read_Meta`, `ST_Drivers` |
| **MVT** | `ST_AsMVT`, `ST_AsMVTGeom` |
| **Aggregates** | `ST_Union_Agg`, `ST_Extent_Agg`, `ST_Intersection_Agg`, `ST_MemUnion_Agg`, `ST_Collect` |
| **Validation** | `ST_IsValid`, `ST_IsSimple`, `ST_IsRing`, `ST_IsClosed`, `ST_IsEmpty`, `ST_MakeValid` |

## DuckDB Spatial (`pixi run duckdb`)
- Load: `INSTALL spatial; LOAD spatial;`
- Read: `SELECT * FROM ST_Read('file.parquet')`
- CRS: `ST_Transform(geom, 'EPSG:4326', 'EPSG:3857')`
- Joins: ST_Contains, ST_Within, ST_Intersects
- Aggregations: ST_Union_Agg, ST_Collect with GROUP BY
- Indexing: H3/S2 for large-scale point data

## GDAL (`pixi run gdal`) — unified CLI v3.12+
- Info: `gdal info input.gpkg`
- Convert: `gdal vector convert input.shp output.parquet`
- Reproject: `gdal vector reproject input.gpkg output.gpkg -d EPSG:4326`
- Pipeline: `gdal vector pipeline read in.gpkg ! reproject --dst-crs EPSG:4326 ! write out.parquet`
- Terrain: `gdal raster hillshade dem.tif hillshade.tif`

## GeoParquet — preferred interchange format
- Columnar, compressed, embedded CRS metadata
- Read: DuckDB `read_parquet`/`ST_Read`, GDAL `gdal info`
- Write from DuckDB: `COPY (...) TO 'out.parquet' (FORMAT PARQUET)`
- Write from GDAL: `gdal vector convert in.gpkg out.parquet`

## Analysis patterns
- Distance: use projected CRS (not EPSG:4326) for metric accuracy
- Large-scale points: H3 or S2 indexing
- Raster+vector: `gdal vector rasterize` / `gdal raster polygonize`
- Zonal stats: `gdal raster zonal-stats <raster> <zones> <out>`

## ArcGIS FeatureServer via DuckDB

Reusable macros for querying ArcGIS REST services directly from DuckDB. Load once per session:

```bash
pixi run duckdb -init ".duckdb-skills/arcgis.sql"
```

Or inside a running session: `.read .duckdb-skills/arcgis.sql`

### Macro Quick Reference

| Level | Macro | Returns |
|-------|-------|---------|
| **L0** | `arcgis_type_map(esri_type)` | DuckDB type name |
| **L0** | `arcgis_geom_map(esri_geom)` | WKT geometry type |
| **L0** | `arcgis_query_url(base, layer, ...)` | Full query URL (with token if set) |
| **L1** | `arcgis_services(catalog_url)` | TABLE: service_name, service_type |
| **L1** | `arcgis_layer_meta(layer_url)` | TABLE: meta as VARIANT (dot notation) |
| **L1** | `arcgis_meta(layer_url)` | TABLE: one-row summary |
| **L1** | `arcgis_count(query_url)` | TABLE: total |
| **L1** | `arcgis_fields(layer_url)` | TABLE: field_name, esri_type, duckdb_type, domain |
| **L1** | `arcgis_domains(layer_url)` | TABLE: field_name, code, label |
| **L1** | `arcgis_subtypes(layer_url)` | TABLE: type_field, subtype_id, subtype_name |
| **L1** | `arcgis_relationships(layer_url)` | TABLE: rel_id, rel_name, cardinality, key_field |
| **L2** | `arcgis_query(url)` | TABLE: properties + geometry (no CRS) |
| **L2** | `arcgis_read(url, crs)` | TABLE: properties + geometry WITH CRS |

### Common Workflows

```sql
-- Inspect a layer
SELECT * FROM arcgis_meta('https://.../FeatureServer/0?f=json');
SELECT * FROM arcgis_fields('https://.../FeatureServer/0?f=json');

-- Download features with CRS
SELECT * FROM arcgis_read('https://.../FeatureServer/0/query?where=1%3D1&outFields=%2A&outSR=4326&returnGeometry=true&f=geojson');

-- Paginated download (> maxRecordCount)
SELECT * FROM arcgis_read([
    'https://.../FeatureServer/0/query?where=1%3D1&outFields=%2A&outSR=4326'
    '&returnGeometry=true&resultOffset=' || x || '&resultRecordCount=2000&f=geojson'
    FOR x IN generate_series(0, 12000, 2000)
]);

-- Export to GeoParquet with VARIANT metadata
COPY (
    WITH lm AS (SELECT meta FROM arcgis_layer_meta('https://.../FeatureServer/0?f=json'))
    SELECT f.*, (SELECT lm.meta.drawingInfo FROM lm)::VARIANT AS drawing_info
    FROM arcgis_read('https://.../query?where=1%3D1&outFields=%2A&outSR=4326&returnGeometry=true&f=geojson') f
) TO 'output.parquet' (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 15);

-- Domain resolution (3 steps)
SET VARIABLE arcgis_layer = 'https://.../FeatureServer/16?f=json';
CREATE OR REPLACE TEMP TABLE _domains AS
WITH dl AS (SELECT * FROM arcgis_domains(getvariable('arcgis_layer')))
SELECT MAP(list(field_name), list(lookup)) AS all_domains
FROM (SELECT field_name, MAP(list(code), list(label)) AS lookup FROM dl GROUP BY field_name);
CREATE OR REPLACE MACRO resolve_domain(field_val, field_name) AS
    COALESCE(
        (SELECT all_domains[field_name] FROM _domains)[field_val::VARCHAR],
        (SELECT all_domains[field_name] FROM _domains)[TRY_CAST(field_val AS INTEGER)::VARCHAR]
    );
```

### Authentication

```sql
-- Token in URL (auto-appended by arcgis_query_url)
SET VARIABLE arcgis_token = 'YOUR_TOKEN';

-- HTTP headers (more secure)
SET http_extra_headers = MAP {'X-Esri-Authorization': 'Bearer YOUR_TOKEN'};
```

Full reference: `.duckdb-skills/arcgis.sql`

## Cross-references
- **geoparquet** skill — gpio adds Hilbert sorting, bbox covering, validation
- **gdal** skill — complete unified GDAL CLI reference (including Esri references in `references/esri-*.md`)
- **duckdb-query** skill — interactive DuckDB SQL queries
- **duckdb-state** skill — manages `.duckdb-skills/state.sql` (extensions, credentials, macros)
- **duckdb-read-file** skill — explore any data file before analysis
- **data-explorer** agent — proactive dataset profiling (DuckDB + GDAL + gpio)
- **data-quality** agent — deep validation (nulls, geometry, CRS consistency)
- **pipeline-orchestrator** agent — multi-step workflow generation with pixi tasks
