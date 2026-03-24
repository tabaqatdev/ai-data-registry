# GeoParquet Tool Comparison Matrix

Feature comparison between gpio, GDAL, DuckDB, and GeoPandas for GeoParquet workflows.

## Format Support

| Capability | gpio | GDAL | DuckDB | GeoPandas |
|---|---|---|---|---|
| Read GeoParquet | Yes | Yes | Yes | Yes |
| Write GeoParquet | Yes | Yes | Partial* | Yes |
| GeoParquet geo metadata | Full | Full | No* | Full |
| Bbox covering metadata | Read/Write | Read only | No | No |
| Read GeoJSON | Yes | Yes | Yes | Yes |
| Read Shapefile | No | Yes | Yes | Yes |
| Read FlatGeobuf | Yes | Yes | Yes | Yes |
| Read CSV/TSV | Yes | Yes | Yes | Yes |
| Read remote (S3/GCS/HTTP) | Yes | Yes | Yes | Limited |

*DuckDB writes standard Parquet with geometry as WKB but does not embed GeoParquet `geo` metadata in the file footer. Post-process with gpio to add it.

## Spatial Operations

| Capability | gpio | GDAL | DuckDB | GeoPandas |
|---|---|---|---|---|
| Hilbert sorting | Yes (native) | No | No | No |
| S2/H3/geohash sorting | Yes | No | No | No |
| Bbox covering enrichment | Yes (native) | No | No | No |
| Spatial index enrichment (H3, S2, A5, quadkey, KD-tree) | Yes | No | Partial (H3 via extension) | Via libraries |
| Spatial partitioning (kdtree, admin, h3, s2, quadkey) | Yes (native) | Attribute only | Hive partitioning only | No |
| Reprojection | No | Yes | Yes (spatial ext) | Yes |
| Spatial joins | No | Yes (SQL) | Yes (spatial ext) | Yes |
| Buffer/clip/overlay | No | Yes | Yes (spatial ext) | Yes |
| Geometry validation | No | Yes | Yes (spatial ext) | Yes |
| Geometry repair | No | Yes | No | Yes |

## Compression and Optimization

| Capability | gpio | GDAL | DuckDB | GeoPandas |
|---|---|---|---|---|
| zstd compression | Yes | Yes | Yes | Yes (via pyarrow) |
| Compression level control | Yes (1-22) | Yes | Yes | Yes |
| Row group size control | Yes | Yes | Yes | Yes (via pyarrow) |
| Row group statistics | Inspect/validate | Read only | Read only | No |
| Compression analysis | Yes (`check compression`) | No | No | No |
| File size benchmarking | Yes (`benchmark`) | No | No | No |

## Validation and Quality

| Capability | gpio | GDAL | DuckDB | GeoPandas |
|---|---|---|---|---|
| GeoParquet spec validation | Yes (`check spec`) | No | No | No |
| Bbox covering validation | Yes (`check bbox`) | No | No | No |
| Spatial ordering quality check | Yes (`check spatial-order`) | No | No | No |
| Row group size analysis | Yes (`check row-groups`) | No | No | No |
| Full validation suite | Yes (`check all`) | No | No | No |

## Data Access and Extraction

| Capability | gpio | GDAL | DuckDB | GeoPandas |
|---|---|---|---|---|
| ArcGIS Feature Service extraction | Yes | Yes | No | No |
| WFS extraction | Yes | Yes | No | No |
| BigQuery extraction | Yes | No | Yes (via extension) | Yes (via bigquery client) |
| SQL-based transformations | No | Limited (per-layer SQL) | Yes (full SQL) | No (pandas API) |
| Streaming/chunked reads | Yes | Yes | Yes | Yes |

## Publishing and Distribution

| Capability | gpio | GDAL | DuckDB | GeoPandas |
|---|---|---|---|---|
| STAC metadata generation | Yes | No | No | No |
| S3 publishing | Yes | Yes (vsi) | Yes | No |
| GCS publishing | Yes | Yes (vsi) | Yes | No |
| Azure publishing | Yes | Yes (vsi) | Yes | No |

## Performance Characteristics

| Aspect | gpio | GDAL | DuckDB | GeoPandas |
|---|---|---|---|---|
| Memory model | Streaming | Streaming | Columnar/streaming | In-memory |
| Large file handling | Excellent | Excellent | Excellent | Poor (RAM-bound) |
| Parallel processing | Yes | Limited | Yes (auto) | No |
| Spatial query speed (sorted data) | Benchmark tool | N/A | Fast with pushdown | Slow (full scan) |

## Recommended Roles in a Workflow

| Stage | Recommended Tool | Why |
|---|---|---|
| **Extraction** from services | gpio extract, GDAL | Native service connectors |
| **Format conversion** (non-Parquet to Parquet) | GDAL, gpio convert | Broad format support |
| **SQL transformations** | DuckDB | Full SQL engine, aggregations, joins |
| **Spatial analysis** (joins, buffers, overlays) | DuckDB spatial, GeoPandas | Rich spatial operations |
| **GeoParquet optimization** (sort, bbox, compress) | gpio | Purpose-built for this |
| **Validation** | gpio check | Only tool with full validation |
| **Spatial indexing** (H3, S2, etc.) | gpio add index | Native multi-index support |
| **Partitioning** for distribution | gpio partition | Spatial-aware strategies |
| **Publishing** with STAC | gpio publish | Integrated STAC + cloud upload |
| **Quick exploration** | DuckDB | SQL queries, instant schema inspection |

## Typical Combined Workflow

```
GDAL / gpio extract  →  DuckDB (SQL transforms)  →  gpio (optimize)  →  gpio (validate + publish)
       ↑ ingest              ↑ transform               ↑ optimize            ↑ distribute
```

1. **Ingest**: GDAL converts from Shapefile/GDB/GeoJSON, or gpio extracts from services
2. **Transform**: DuckDB runs SQL joins, filters, aggregations
3. **Optimize**: gpio sorts (Hilbert), adds bbox covering, sets compression
4. **Distribute**: gpio validates (`check all`), generates STAC, publishes to cloud
