---
name: duckdb-docs
description: >
  Search DuckDB and DuckLake documentation via full-text search. Use when encountering
  DuckDB errors, looking up function syntax, or when other skills hit unknown DuckDB behavior.
  Also use proactively when unsure about DuckDB features.
argument-hint: <question or keyword>
allowed-tools: Bash
---

Query: `$@`

## Indexes

| Index | URL | Cache file | Versions |
|-------|-----|------------|----------|
| DuckDB | `https://duckdb.org/data/docs-search.duckdb` | `duckdb-docs.duckdb` | stable, current, blog |
| DuckLake | `https://ducklake.select/data/docs-search.duckdb` | `ducklake-docs.duckdb` | stable, preview |

Schema: `chunk_id` (PK), `page_title`, `section`, `breadcrumb`, `url`, `version`, `text`

Default: search DuckDB `stable`. Use `current` for nightly, `blog` for background, DuckLake index for DuckLake questions.

## Extract search terms

Natural language → extract key technical terms (drop stop words). Technical terms → use as-is.

## Ensure cache (~/.duckdb/docs/)

```bash
pixi run python -c "import pathlib; pathlib.Path.home().joinpath('.duckdb','docs').mkdir(parents=True, exist_ok=True)"
```

Check freshness (≤2 days). If stale/missing:
```bash
pixi run duckdb -c "
LOAD httpfs; LOAD fts;
ATTACH 'REMOTE_URL' AS remote (READ_ONLY);
ATTACH '$HOME/.duckdb/docs/CACHE_FILE.tmp' AS tmp;
COPY FROM DATABASE remote TO tmp;
" && mv "$HOME/.duckdb/docs/CACHE_FILE.tmp" "$HOME/.duckdb/docs/CACHE_FILE"
```

## Search

```bash
pixi run duckdb "$HOME/.duckdb/docs/CACHE_FILE" -readonly -json -c "
LOAD fts;
SELECT chunk_id, page_title, section, url, version, text,
  fts_main_docs_chunks.match_bm25(chunk_id, 'SEARCH_QUERY') AS score
FROM docs_chunks
WHERE score IS NOT NULL AND version = 'VERSION'
ORDER BY score DESC LIMIT 8;
"
```

No results → broaden query, drop least specific term. Still nothing → suggest docs website.

## Present

Show results ordered by score, then synthesize a concise answer to `$@`.
