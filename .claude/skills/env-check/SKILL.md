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
pixi --version && echo "  pixi installed: YES" || echo "  pixi installed: NO — install from https://pixi.sh"

# Lock file freshness (cross-platform via Python)
if [ -f pixi.lock ]; then
  pixi run python -c "
import os, pathlib
lock_mtime = pathlib.Path('pixi.lock').stat().st_mtime
toml_mtime = pathlib.Path('pixi.toml').stat().st_mtime
if toml_mtime > lock_mtime:
    print('  pixi.lock: STALE (pixi.toml is newer)')
    print('  Recommendation: Run pixi install to update the lock file')
else:
    print('  pixi.lock: up to date')
"
else
  echo "  pixi.lock: MISSING"
  echo "  Recommendation: Run 'pixi install' to generate the lock file"
fi
```

### 2. DuckDB

```bash
echo ""
echo "=== DuckDB ==="
pixi run duckdb --version 2>/dev/null && echo "  DuckDB available: YES" || echo "  DuckDB available: NO — ensure duckdb is in pixi.toml [dependencies]"
```

### 3. GDAL

```bash
echo ""
echo "=== GDAL ==="
pixi run gdal --version 2>/dev/null && echo "  GDAL available: YES" || echo "  GDAL available: NO — ensure gdal is in pixi.toml [dependencies]"
```

### 4. gpio (GeoParquet I/O)

```bash
echo ""
echo "=== gpio ==="
pixi run gpio --version 2>/dev/null && echo "  gpio available: YES" || echo "  gpio available: NO — check README for installation"
```

### 5. GDAL Arrow/Parquet Driver Compatibility

```bash
echo ""
echo "=== GDAL Arrow/Parquet driver ==="
# Use the new unified GDAL CLI to check driver support
pixi run gdal vector info --formats 2>/dev/null | grep -qi parquet && echo "  GDAL Parquet driver: AVAILABLE" || echo "  GDAL Parquet driver: NOT FOUND — install libgdal-arrow-parquet"

# Show GDAL version
pixi run gdal --version 2>/dev/null
echo "  Ensure libgdal-arrow-parquet matches the installed GDAL major version."
```

### 6. DuckDB Extensions

```bash
echo ""
echo "=== DuckDB Extensions ==="
for ext in spatial httpfs fts; do
  pixi run duckdb -c "INSTALL $ext; LOAD $ext; SELECT '${ext}_ok';" 2>/dev/null | grep -q "${ext}_ok" && echo "  $ext: OK" || echo "  $ext: FAILED — run 'pixi run duckdb -c \"INSTALL $ext;\"'"
done
```

### 7. state.sql Validation

```bash
echo ""
echo "=== state.sql ==="
STATE_FILE=".duckdb-skills/state.sql"
if [ -f "$STATE_FILE" ]; then
  echo "  state.sql: EXISTS"
  pixi run duckdb -init "$STATE_FILE" -c "SELECT 'state_ok';" 2>&1 | grep -q "state_ok" && echo "  state.sql: VALID" || echo "  state.sql: INVALID — re-initialize via duckdb-state skill"
else
  echo "  state.sql: NOT FOUND (will be created on first use by duckdb-state skill)"
fi
```

---

## Full Check Script

Run all checks together via `pixi run python` for cross-platform compatibility:

```bash
pixi run python -c "
import subprocess, sys, shutil

def run(cmd):
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30)
        return result.returncode == 0
    except Exception:
        return False

def check(name, cmd):
    ok = run(cmd)
    status = 'OK' if ok else 'FAIL'
    print(f'  {status:4s}  {name}')
    return ok

print('=' * 48)
print('  ai-data-registry — Environment Check')
print('=' * 48)
print()

passed = failed = 0
checks = [
    ('pixi installed',           'pixi --version'),
    ('pixi.lock exists',         'pixi run python -c \"import pathlib; exit(0 if pathlib.Path(\\\"pixi.lock\\\").exists() else 1)\"'),
    ('DuckDB available',         'pixi run duckdb --version'),
    ('GDAL available',           'pixi run gdal --version'),
    ('gpio available',           'pixi run gpio --version'),
    ('DuckDB spatial extension', 'pixi run duckdb -c \"INSTALL spatial; LOAD spatial; SELECT 1;\"'),
    ('DuckDB httpfs extension',  'pixi run duckdb -c \"INSTALL httpfs; LOAD httpfs; SELECT 1;\"'),
    ('DuckDB fts extension',     'pixi run duckdb -c \"INSTALL fts; LOAD fts; SELECT 1;\"'),
]

for name, cmd in checks:
    if check(name, cmd):
        passed += 1
    else:
        failed += 1

print()
print('-' * 48)
print(f'  Results: {passed} passed, {failed} failed')
print('=' * 48)
"
```

---

## Cross-references

- **duckdb-install** — for installing/updating DuckDB and extensions
- **duckdb-state** — for initializing or repairing state.sql
