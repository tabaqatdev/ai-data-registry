---
name: env-check
description: >
  Validates project environment health — checks that pixi, DuckDB, GDAL, gpio,
  required extensions, and versions are correctly installed and compatible.
allowed-tools:
  - Bash
  - Read
  - Glob
---

# Environment Validation Skill

This skill checks that all tools and dependencies required by the
**ai-data-registry** project are present, correctly versioned, and compatible.

Run this skill before executing pipelines or when troubleshooting failures.

---

## Checks

### 1. pixi Installation & Lock File

```bash
echo "=== pixi ==="
if command -v pixi &>/dev/null; then
  echo "  pixi version: $(pixi --version)"
  echo "  pixi installed: YES"
else
  echo "  pixi installed: NO"
  echo "  Recommendation: Install pixi — https://pixi.sh"
fi

# Lock file freshness
if [ -f pixi.lock ]; then
  LOCK_MOD=$(stat -f %m pixi.lock 2>/dev/null || stat -c %Y pixi.lock 2>/dev/null)
  TOML_MOD=$(stat -f %m pixi.toml 2>/dev/null || stat -c %Y pixi.toml 2>/dev/null)
  if [ "$TOML_MOD" -gt "$LOCK_MOD" ]; then
    echo "  pixi.lock: STALE (pixi.toml is newer)"
    echo "  Recommendation: Run 'pixi install' to update the lock file"
  else
    echo "  pixi.lock: up to date"
  fi
else
  echo "  pixi.lock: MISSING"
  echo "  Recommendation: Run 'pixi install' to generate the lock file"
fi
```

### 2. DuckDB

```bash
echo ""
echo "=== DuckDB ==="
if pixi run duckdb --version 2>/dev/null; then
  echo "  DuckDB available: YES"
else
  echo "  DuckDB available: NO"
  echo "  Recommendation: Ensure duckdb is listed in pixi.toml [dependencies]"
fi
```

### 3. GDAL

```bash
echo ""
echo "=== GDAL ==="
if pixi run gdal --version 2>/dev/null || pixi run gdalinfo --version 2>/dev/null; then
  echo "  GDAL available: YES"
else
  echo "  GDAL available: NO"
  echo "  Recommendation: Ensure gdal is listed in pixi.toml [dependencies]"
fi
```

### 4. gpio (GeoParquet I/O)

```bash
echo ""
echo "=== gpio ==="
if pixi run gpio --version 2>/dev/null; then
  echo "  gpio available: YES"
else
  echo "  gpio available: NO"
  echo "  Recommendation: gpio may need manual installation."
  echo "  Check the project README or install via: pip install geoparquet-io"
fi
```

### 5. GDAL Arrow/Parquet Driver Compatibility

```bash
echo ""
echo "=== GDAL Arrow/Parquet driver ==="
# Check that GDAL was built with Arrow/Parquet support
if pixi run ogrinfo --formats 2>/dev/null | grep -qi parquet; then
  echo "  GDAL Parquet driver: AVAILABLE"
else
  echo "  GDAL Parquet driver: NOT FOUND"
  echo "  Recommendation: Install libgdal-arrow-parquet or rebuild GDAL with Arrow support"
fi

# Check version compatibility
GDAL_VER=$(pixi run gdalinfo --version 2>/dev/null | head -1)
echo "  GDAL version string: $GDAL_VER"
echo "  Ensure libgdal-arrow-parquet matches the installed GDAL major version."
```

### 6. DuckDB Extensions

```bash
echo ""
echo "=== DuckDB Extensions ==="
for ext in spatial httpfs fts; do
  if pixi run duckdb -c "INSTALL $ext; LOAD $ext; SELECT '${ext}_ok';" 2>/dev/null | grep -q "${ext}_ok"; then
    echo "  $ext: OK"
  else
    echo "  $ext: FAILED"
    echo "  Recommendation: Run 'pixi run duckdb -c \"INSTALL $ext;\"' manually"
  fi
done
```

### 7. state.sql Validation

```bash
echo ""
echo "=== state.sql ==="
STATE_FILE=".duckdb-skills/state.sql"
if [ -f "$STATE_FILE" ]; then
  echo "  state.sql: EXISTS"
  if pixi run duckdb -init "$STATE_FILE" -c "SELECT 'state_ok';" 2>&1 | grep -q "state_ok"; then
    echo "  state.sql: VALID"
  else
    echo "  state.sql: INVALID — errors during execution"
    echo "  Recommendation: Re-initialize via duckdb-state skill or delete and recreate"
  fi
else
  echo "  state.sql: NOT FOUND (will be created on first use by duckdb-state skill)"
fi
```

---

## Full Check Script

Run all checks together and produce a summary checklist:

```bash
#!/usr/bin/env bash
set -euo pipefail

echo "============================================"
echo "  ai-data-registry — Environment Check"
echo "============================================"
echo ""

PASS=0
FAIL=0

check() {
  local name="$1"
  local cmd="$2"
  if eval "$cmd" &>/dev/null; then
    echo "  OK   $name"
    ((PASS++))
  else
    echo "  FAIL $name"
    ((FAIL++))
  fi
}

check "pixi installed"            "command -v pixi"
check "pixi.lock exists"          "test -f pixi.lock"
check "DuckDB available"          "pixi run duckdb --version"
check "GDAL available"            "pixi run gdalinfo --version"
check "gpio available"            "pixi run gpio --version"
check "GDAL Parquet driver"       "pixi run ogrinfo --formats 2>/dev/null | grep -qi parquet"
check "DuckDB spatial extension"  "pixi run duckdb -c 'INSTALL spatial; LOAD spatial; SELECT 1;'"
check "DuckDB httpfs extension"   "pixi run duckdb -c 'INSTALL httpfs; LOAD httpfs; SELECT 1;'"
check "DuckDB fts extension"      "pixi run duckdb -c 'INSTALL fts; LOAD fts; SELECT 1;'"
check "state.sql valid"           "test -f .duckdb-skills/state.sql && pixi run duckdb -init .duckdb-skills/state.sql -c 'SELECT 1;'"

echo ""
echo "--------------------------------------------"
echo "  Results: $PASS passed, $FAIL failed"
echo "============================================"
```

---

## Cross-references

- **duckdb-install** — for installing/updating DuckDB and extensions
- **duckdb-state** — for initializing or repairing state.sql
