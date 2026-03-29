-- =============================================================================
-- ArcGIS FeatureServer Macros for DuckDB (v1.5.1+)
-- =============================================================================
-- Reusable table and scalar macros for querying ArcGIS REST services.
-- Organized in levels: L0 (primitives) -> L1 (inspection) -> L2 (data) -> L3 (analysis)
--
-- Usage:
--   pixi run duckdb -init ".duckdb-skills/arcgis.sql"
--   .read .duckdb-skills/arcgis.sql
--
-- Authentication (run before queries):
--   SET VARIABLE arcgis_token = 'YOUR_TOKEN';
--   SET VARIABLE arcgis_headers = 'X-Esri-Authorization: Bearer YOUR_TOKEN';
--
-- =============================================================================

INSTALL httpfs; LOAD httpfs;
INSTALL spatial; LOAD spatial;
SET geometry_always_xy = true;

-- =============================================================================
-- L0: PRIMITIVES (type mapping, URL builders, auth)
-- =============================================================================

-- Map Esri field types to DuckDB types.
-- Use: SELECT arcgis_type_map('esriFieldTypeDouble') => 'DOUBLE'
CREATE OR REPLACE MACRO arcgis_type_map(esri_type) AS
    CASE esri_type
        WHEN 'esriFieldTypeOID'          THEN 'INTEGER'
        WHEN 'esriFieldTypeSmallInteger' THEN 'SMALLINT'
        WHEN 'esriFieldTypeInteger'      THEN 'INTEGER'
        WHEN 'esriFieldTypeBigInteger'   THEN 'BIGINT'
        WHEN 'esriFieldTypeSingle'       THEN 'FLOAT'
        WHEN 'esriFieldTypeDouble'       THEN 'DOUBLE'
        WHEN 'esriFieldTypeString'       THEN 'VARCHAR'
        WHEN 'esriFieldTypeDate'         THEN 'TIMESTAMP'
        WHEN 'esriFieldTypeDateOnly'     THEN 'DATE'
        WHEN 'esriFieldTypeTimeOnly'     THEN 'TIME'
        WHEN 'esriFieldTypeTimestampOffset' THEN 'TIMESTAMPTZ'
        WHEN 'esriFieldTypeGUID'         THEN 'UUID'
        WHEN 'esriFieldTypeGlobalID'     THEN 'UUID'
        WHEN 'esriFieldTypeXML'          THEN 'VARCHAR'
        WHEN 'esriFieldTypeBlob'         THEN 'BLOB'
        WHEN 'esriFieldTypeGeometry'     THEN 'GEOMETRY'
        ELSE 'VARCHAR'
    END;

-- Map Esri geometry types to DuckDB/WKT geometry types.
CREATE OR REPLACE MACRO arcgis_geom_map(esri_geom) AS
    CASE esri_geom
        WHEN 'esriGeometryPoint'      THEN 'POINT'
        WHEN 'esriGeometryMultipoint' THEN 'MULTIPOINT'
        WHEN 'esriGeometryPolyline'   THEN 'LINESTRING'
        WHEN 'esriGeometryPolygon'    THEN 'POLYGON'
        WHEN 'esriGeometryEnvelope'   THEN 'POLYGON'
        ELSE 'GEOMETRY'
    END;

-- Build a GeoJSON query URL with pagination and auth support.
-- Token is appended from the arcgis_token VARIABLE if set.
CREATE OR REPLACE MACRO arcgis_query_url(
    base_url,
    layer_id,
    where_clause := '1%3D1',
    out_sr := '4326',
    page_size := NULL,
    result_offset := NULL
) AS
    base_url || '/' || layer_id || '/query?where=' || where_clause
    || '&outFields=%2A&outSR=' || out_sr || '&returnGeometry=true'
    || CASE WHEN page_size IS NOT NULL THEN '&resultRecordCount=' || page_size ELSE '' END
    || CASE WHEN result_offset IS NOT NULL THEN '&resultOffset=' || result_offset ELSE '' END
    || '&f=geojson'
    || CASE WHEN getvariable('arcgis_token') IS NOT NULL AND getvariable('arcgis_token') != ''
            THEN '&token=' || getvariable('arcgis_token') ELSE '' END;

-- Build a layer metadata URL.
CREATE OR REPLACE MACRO arcgis_meta_url(base_url, layer_id) AS
    base_url || '/' || layer_id || '?f=json'
    || CASE WHEN getvariable('arcgis_token') IS NOT NULL AND getvariable('arcgis_token') != ''
            THEN '&token=' || getvariable('arcgis_token') ELSE '' END;

-- Initialize default variables (safe to call multiple times)
SET VARIABLE arcgis_token = '';
SET VARIABLE arcgis_crs = 'EPSG:4326';

-- =============================================================================
-- L1: INSPECTION (service discovery, metadata, schema)
-- =============================================================================

-- List all services AND folders in an ArcGIS REST catalog.
-- Input: catalog URL (e.g., https://server/arcgis/rest/services/?f=json)
-- Returns: item_type (folder/service), name, service_type
CREATE OR REPLACE MACRO arcgis_catalog(catalog_url) AS TABLE
    WITH raw AS (
        SELECT content::JSON AS j FROM read_text(catalog_url)
    ),
    folders AS (
        SELECT 'folder' AS item_type,
               json_extract_string(j, '$.folders[' || i || ']') AS name,
               NULL AS service_type
        FROM raw, generate_series(0, GREATEST(json_array_length(j->'folders')::BIGINT - 1, -1)) AS t(i)
        WHERE json_array_length(j->'folders') > 0
    ),
    services AS (
        SELECT 'service' AS item_type,
               json_extract_string(j, '$.services[' || i || '].name') AS name,
               json_extract_string(j, '$.services[' || i || '].type') AS service_type
        FROM raw, generate_series(0, GREATEST(json_array_length(j->'services')::BIGINT - 1, -1)) AS t(i)
        WHERE json_array_length(j->'services') > 0
    )
    SELECT * FROM (SELECT * FROM folders UNION ALL SELECT * FROM services);

-- Convenience: list only FeatureServer/MapServer services (no folders).
CREATE OR REPLACE MACRO arcgis_services(catalog_url) AS TABLE
    SELECT name AS service_name, service_type
    FROM arcgis_catalog(catalog_url)
    WHERE service_type IN ('FeatureServer', 'MapServer');

-- List all layers and tables in a FeatureServer/MapServer service.
-- Input: service URL (e.g., https://server/.../FeatureServer?f=json)
-- Returns: layer_id, layer_name, geometry_type, item_type (layer/table)
CREATE OR REPLACE MACRO arcgis_layers(service_url) AS TABLE
    WITH raw AS (
        SELECT content::JSON AS j FROM read_text(service_url)
    ),
    layers AS (
        SELECT json_extract_string(j, '$.layers[' || i || '].id') AS layer_id,
               json_extract_string(j, '$.layers[' || i || '].name') AS layer_name,
               json_extract_string(j, '$.layers[' || i || '].geometryType') AS geometry_type,
               'layer' AS item_type
        FROM raw, generate_series(0, GREATEST(json_array_length(j->'layers')::BIGINT - 1, -1)) AS t(i)
        WHERE json_array_length(j->'layers') > 0
    ),
    tbls AS (
        SELECT json_extract_string(j, '$.tables[' || i || '].id') AS layer_id,
               json_extract_string(j, '$.tables[' || i || '].name') AS layer_name,
               NULL AS geometry_type,
               'table' AS item_type
        FROM raw, generate_series(0, GREATEST(json_array_length(j->'tables')::BIGINT - 1, -1)) AS t(i)
        WHERE json_array_length(j->'tables') > 0
    )
    SELECT * FROM (SELECT * FROM layers UNION ALL SELECT * FROM tbls);

-- Fetch full layer metadata as a single VARIANT for dot-notation access.
-- Example: SELECT meta.name, meta.geometryType, meta.maxRecordCount,
--          meta.extent.spatialReference.latestWkid,
--          meta.advancedQueryCapabilities.supportsPagination
--          FROM arcgis_layer_meta('https://.../FeatureServer/0?f=json');
CREATE OR REPLACE MACRO arcgis_layer_meta(layer_url) AS TABLE
    SELECT (content::JSON)::VARIANT AS meta
    FROM read_text(layer_url);

-- Structured one-row metadata summary.
CREATE OR REPLACE MACRO arcgis_meta(layer_url) AS TABLE
    SELECT
        content->>'name' AS name,
        content->>'geometryType' AS geometry_type,
        arcgis_geom_map(content->>'geometryType') AS duckdb_geom_type,
        CAST(content->>'maxRecordCount' AS INTEGER) AS max_records,
        COALESCE(
            content->'extent'->'spatialReference'->>'latestWkid',
            content->'extent'->'spatialReference'->>'wkid'
        ) AS wkid,
        content->'advancedQueryCapabilities'->>'supportsPagination' AS pagination,
        content->'advancedQueryCapabilities'->>'supportsStatistics' AS statistics,
        content->'advancedQueryCapabilities'->>'supportsOrderBy' AS order_by,
        content->'advancedQueryCapabilities'->>'supportsDistinct' AS distinct_vals,
        json_array_length(content->'fields')::INTEGER AS field_count,
        COALESCE(json_array_length(content->'relationships'), 0)::INTEGER AS rel_count,
        COALESCE(json_array_length(content->'types'), 0)::INTEGER AS subtype_count
    FROM (SELECT content::JSON AS content FROM read_text(layer_url));

-- Get total feature count.
-- Input: base query URL without returnCountOnly/f params.
CREATE OR REPLACE MACRO arcgis_count(query_url) AS TABLE
    SELECT CAST(content->>'count' AS INTEGER) AS total
    FROM (SELECT content::JSON AS content
          FROM read_text(query_url || '&returnCountOnly=true&f=json'));

-- List all fields with Esri type, DuckDB type mapping, alias, and domain info.
CREATE OR REPLACE MACRO arcgis_fields(layer_url) AS TABLE
    WITH raw AS (
        SELECT content::JSON AS j FROM read_text(layer_url)
    )
    SELECT json_extract_string(j, '$.fields[' || i || '].name') AS field_name,
           json_extract_string(j, '$.fields[' || i || '].alias') AS field_alias,
           json_extract_string(j, '$.fields[' || i || '].type') AS esri_type,
           arcgis_type_map(json_extract_string(j, '$.fields[' || i || '].type')) AS duckdb_type,
           json_extract_string(j, '$.fields[' || i || '].domain.type') AS domain_type,
           json_extract_string(j, '$.fields[' || i || '].domain.name') AS domain_name,
           CAST(json_extract_string(j, '$.fields[' || i || '].length') AS INTEGER) AS field_length,
           json_extract_string(j, '$.fields[' || i || '].nullable') AS nullable
    FROM raw, generate_series(0, (SELECT json_array_length(j->'fields')::BIGINT - 1 FROM raw)) AS t(i);

-- Extract coded value domain lookups.
-- Returns: field_name, code, label (one row per coded value).
CREATE OR REPLACE MACRO arcgis_domains(layer_url) AS TABLE
    WITH raw AS (
        SELECT content::JSON AS j FROM read_text(layer_url)
    ),
    fields_list AS (
        SELECT json_extract_string(j, '$.fields[' || i || '].name') AS field_name,
               json_extract_string(j, '$.fields[' || i || '].domain.type') AS dom_type,
               json_extract(j, '$.fields[' || i || '].domain.codedValues') AS cvs
        FROM raw, generate_series(0, (SELECT json_array_length(j->'fields')::BIGINT - 1 FROM raw)) AS t(i)
    ),
    domain_fields AS (
        SELECT field_name, cvs FROM fields_list WHERE dom_type = 'codedValue'
    )
    SELECT field_name,
           json_extract_string(cvs, '$[' || k || '].code') AS code,
           json_extract_string(cvs, '$[' || k || '].name') AS label
    FROM domain_fields, generate_series(0, (SELECT json_array_length(cvs)::BIGINT - 1)) AS t(k);

-- Extract subtypes.
-- Returns: type_field, subtype_id, subtype_name.
CREATE OR REPLACE MACRO arcgis_subtypes(layer_url) AS TABLE
    WITH raw AS (
        SELECT content::JSON AS j FROM read_text(layer_url)
    )
    SELECT json_extract_string(j, '$.typeIdField') AS type_field,
           json_extract_string(j, '$.types[' || i || '].id') AS subtype_id,
           json_extract_string(j, '$.types[' || i || '].name') AS subtype_name
    FROM raw, generate_series(0, GREATEST(json_array_length(j->'types')::BIGINT - 1, -1)) AS t(i)
    WHERE json_array_length(j->'types') > 0;

-- Extract relationship classes.
CREATE OR REPLACE MACRO arcgis_relationships(layer_url) AS TABLE
    WITH raw AS (
        SELECT content::JSON AS j FROM read_text(layer_url)
    )
    SELECT json_extract_string(j, '$.relationships[' || i || '].id') AS rel_id,
           json_extract_string(j, '$.relationships[' || i || '].name') AS rel_name,
           json_extract_string(j, '$.relationships[' || i || '].relatedTableId') AS related_table_id,
           json_extract_string(j, '$.relationships[' || i || '].cardinality') AS cardinality,
           json_extract_string(j, '$.relationships[' || i || '].role') AS role,
           json_extract_string(j, '$.relationships[' || i || '].keyField') AS key_field,
           json_extract_string(j, '$.relationships[' || i || '].composite') AS composite
    FROM raw, generate_series(0, GREATEST(json_array_length(j->'relationships')::BIGINT - 1, -1)) AS t(i)
    WHERE json_array_length(j->'relationships') > 0;

-- =============================================================================
-- L2: DATA ACCESS (feature queries, CRS detection, pagination)
-- =============================================================================

-- Query features as geometry + flattened properties.
-- Geometry has NO CRS tag. Use arcgis_read() for CRS-tagged output.
CREATE OR REPLACE MACRO arcgis_query(query_url) AS TABLE
    SELECT unnest(feature.properties),
           ST_GeomFromGeoJSON(feature.geometry) AS geometry
    FROM (
        SELECT unnest(features) AS feature
        FROM read_json_auto(query_url)
    );

-- Query features WITH CRS tagging (recommended for GeoParquet export).
-- Default CRS: EPSG:4326 (override with crs parameter or arcgis_crs variable).
CREATE OR REPLACE MACRO arcgis_read(query_url, crs := NULL) AS TABLE
    SELECT unnest(feature.properties),
           ST_SetCRS(
               ST_GeomFromGeoJSON(feature.geometry),
               COALESCE(crs, getvariable('arcgis_crs'), 'EPSG:4326')
           ) AS geometry
    FROM (
        SELECT unnest(features) AS feature
        FROM read_json_auto(query_url)
    );

-- Auto-detect CRS from a GeoJSON response and store in arcgis_crs variable.
-- Run once before querying. Sets VARIABLE arcgis_crs.
-- Usage:
--   .read .duckdb-skills/arcgis.sql
--   SET VARIABLE arcgis_crs = (
--       SELECT COALESCE(j->'crs'->'properties'->>'name', 'EPSG:4326')
--       FROM (SELECT content::JSON AS j
--             FROM read_text('https://.../FeatureServer/0/query?where=1%3D1&outSR=4326&returnGeometry=true&resultRecordCount=1&f=geojson'))
--   );

-- =============================================================================
-- L3: ANALYSIS (domain resolution, metadata export, spatial filters)
-- =============================================================================

-- Domain Resolution Pattern (3 steps, run after loading macros):
--
-- Step 1: Set your layer URL
--   SET VARIABLE arcgis_layer = 'https://.../FeatureServer/16?f=json';
--
-- Step 2: Build domain resolver
--   CREATE OR REPLACE TEMP TABLE _domains AS
--   WITH dl AS (SELECT * FROM arcgis_domains(getvariable('arcgis_layer')))
--   SELECT MAP(list(field_name), list(lookup)) AS all_domains
--   FROM (SELECT field_name, MAP(list(code), list(label)) AS lookup
--         FROM dl GROUP BY field_name);
--
--   CREATE OR REPLACE MACRO resolve_domain(field_val, field_name) AS
--       COALESCE(
--           (SELECT all_domains[field_name] FROM _domains)[field_val::VARCHAR],
--           (SELECT all_domains[field_name] FROM _domains)[TRY_CAST(field_val AS INTEGER)::VARCHAR]
--       );
--
-- Step 3: Use on any query
--   SELECT objectid, material, resolve_domain(material, 'material') AS material_label
--   FROM arcgis_read('https://.../FeatureServer/16/query?where=1%3D1&outFields=%2A&outSR=4326&returnGeometry=true&f=geojson');

-- Pagination Pattern (for datasets > maxRecordCount):
--
-- Step 1: Get total and max_records
--   SELECT * FROM arcgis_count('https://.../FeatureServer/0/query?where=1%3D1');
--   SELECT * FROM arcgis_meta('https://.../FeatureServer/0?f=json');
--
-- Step 2: Paginated query with list comprehension
--   SELECT * FROM arcgis_read([
--       'https://.../FeatureServer/0/query?where=1%3D1&outFields=%2A&outSR=4326'
--       '&returnGeometry=true&resultOffset=' || x || '&resultRecordCount=2000&f=geojson'
--       FOR x IN generate_series(0, 12000, 2000)
--   ]);
--
-- Formula: generate_series(0, total - 1, max_records)

-- GeoParquet Export with VARIANT Metadata Pattern:
--
--   COPY (
--       WITH lm AS (
--           SELECT meta FROM arcgis_layer_meta('https://.../FeatureServer/0?f=json')
--       )
--       SELECT f.*,
--              (SELECT meta.drawingInfo FROM lm)::VARIANT AS drawing_info,
--              (SELECT meta.fields FROM lm)::VARIANT AS fields_schema,
--              (SELECT meta.relationships FROM lm)::VARIANT AS relationships,
--              (SELECT meta.types FROM lm)::VARIANT AS subtypes
--       FROM arcgis_read(
--           'https://.../FeatureServer/0/query?where=1%3D1&outFields=%2A&outSR=4326&returnGeometry=true&f=geojson'
--       ) f
--   ) TO 'output.parquet' (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 15, ROW_GROUP_SIZE 100000);

-- Spatial Filter Pattern (bbox):
--
--   SELECT * FROM arcgis_read(
--       'https://.../FeatureServer/0/query?where=1%3D1&outFields=%2A&outSR=4326'
--       '&returnGeometry=true'
--       '&geometry=-122.5,37.5,-122.0,38.0'
--       '&geometryType=esriGeometryEnvelope'
--       '&inSR=4326&spatialRel=esriSpatialRelIntersects'
--       '&f=geojson'
--   );

-- Point + Distance Buffer Pattern:
--
--   SELECT * FROM arcgis_read(
--       'https://.../FeatureServer/0/query?where=1%3D1&outFields=%2A&outSR=4326'
--       '&returnGeometry=true'
--       '&geometry=-122.42,37.78'
--       '&geometryType=esriGeometryPoint&inSR=4326'
--       '&spatialRel=esriSpatialRelIntersects'
--       '&distance=500&units=esriSRUnit_Meter'
--       '&f=geojson'
--   );

-- Authentication (3 methods, pick one):
--
-- Method 1: Token variable (auto-appended to URLs built with arcgis_query_url)
--   SET VARIABLE arcgis_token = 'YOUR_TOKEN';
--
-- Method 2: HTTP Secret (recommended, applies to all HTTP requests in session)
--   CREATE SECRET arcgis_auth (
--       TYPE HTTP,
--       EXTRA_HTTP_HEADERS MAP {'X-Esri-Authorization': 'Bearer YOUR_TOKEN'}
--   );
--
-- Method 3: Bearer token secret (standard Authorization header)
--   CREATE SECRET arcgis_auth (
--       TYPE HTTP,
--       BEARER_TOKEN 'YOUR_TOKEN'
--   );

-- =============================================================================
-- QUICK REFERENCE
-- =============================================================================
--
-- L0 Primitives:
--   arcgis_type_map(esri_type)         -> DuckDB type name (VARCHAR)
--   arcgis_geom_map(esri_geom)         -> WKT geometry type (VARCHAR)
--   arcgis_query_url(base, layer, ...) -> full query URL (VARCHAR)
--   arcgis_meta_url(base, layer)       -> metadata URL (VARCHAR)
--
-- L1 Inspection:
--   arcgis_catalog(catalog_url)        -> TABLE (item_type, name, service_type) -- folders + services
--   arcgis_services(catalog_url)       -> TABLE (service_name, service_type)    -- FeatureServer/MapServer only
--   arcgis_layers(service_url)         -> TABLE (layer_id, layer_name, geometry_type, item_type)
--   arcgis_layer_meta(layer_url)       -> TABLE (meta VARIANT)
--   arcgis_meta(layer_url)             -> TABLE (one-row summary)
--   arcgis_count(query_url)            -> TABLE (total INTEGER)
--   arcgis_fields(layer_url)           -> TABLE (field_name, esri_type, duckdb_type, ...)
--   arcgis_domains(layer_url)          -> TABLE (field_name, code, label)
--   arcgis_subtypes(layer_url)         -> TABLE (type_field, subtype_id, subtype_name)
--   arcgis_relationships(layer_url)    -> TABLE (rel_id, rel_name, cardinality, ...)
--
-- L2 Data:
--   arcgis_query(query_url)            -> TABLE (properties..., geometry)
--   arcgis_read(query_url, crs)        -> TABLE (properties..., geometry WITH CRS)
--
-- L3 Patterns (documented above, copy-paste):
--   resolve_domain(field_val, field_name) -> resolved label
--   Pagination via generate_series + list comprehension
--   GeoParquet export with VARIANT metadata
--   Spatial filters (bbox, point+distance)
--   HTTP header authentication
