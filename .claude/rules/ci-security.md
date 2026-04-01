---
paths:
  - ".github/scripts/**/*.py"
  - ".github/workflows/**/*.yml"
---
# CI Security Rules

These rules prevent secret exfiltration, command injection, and SQL injection in CI pipelines. All rules are enforced and must not be weakened.

## Workflow Rules

### Never use `${{ }}` expressions directly in `run:` blocks

GitHub Actions expressions (`${{ }}`) are interpolated into shell scripts at parse time, creating injection vectors. Always assign them to `env:` variables first, then reference the env var in shell.

```yaml
# WRONG - shell injection via crafted comment body
run: |
  echo "${{ github.event.comment.body }}"

# CORRECT - env var indirection
env:
  COMMENT_BODY: ${{ github.event.comment.body }}
run: |
  echo "$COMMENT_BODY"
```

Expressions in `name:`, `if:`, `with:` (action inputs), `concurrency:`, `strategy:`, and `run-name:` are safe. Only `run:` blocks need indirection.

### Validate all untrusted inputs early

Workflow inputs (`inputs.*`), comment-derived values, and PR metadata must be validated before use:

- **Workspace names**: must match `^[a-z][a-z0-9-]*$`
- **PR numbers**: must match `^[0-9]+$`
- **Branch names**: must not contain `..`

Validation must happen in the first step that parses the input, before any downstream use.

### Never pass write credentials to untrusted code

PR validation workflows (`pull_request` trigger) run contributor code. Never expose `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, or other write credentials to steps that execute workspace code. The `check_catalog.py` script gracefully skips when credentials are absent.

### Restrict pixi cache writes to main branch

All `setup-pixi` steps must include `cache-write` to prevent PR cache poisoning:

```yaml
- uses: prefix-dev/setup-pixi@v0.9.4
  with:
    cache: true
    cache-write: ${{ github.event_name == 'push' && github.ref_name == 'main' }}
```

### Scope Docker build caches by ref

Docker build caches must include `scope=${{ github.ref }}` to prevent cross-branch cache poisoning:

```yaml
cache-from: type=gha,scope=${{ github.ref }}
cache-to: type=gha,scope=${{ github.ref }},mode=max
```

## Python Script Rules

### Always escape DuckDB SQL values and identifiers

DuckDB DDL, table functions (`glob`, `ducklake_list_files`, `ducklake_add_data_files`, `read_parquet`), and `CREATE SECRET` do NOT support parameterized queries. Use the escaping helpers from `registry_config.py`:

```python
from scripts.registry_config import quote_ident, quote_literal

# Identifiers (table names, schema names, column names)
con.execute(f'SELECT * FROM ws.{quote_ident(schema)}.{quote_ident(table)}')

# String literals (paths, credentials, function arguments)
con.execute(f"SELECT file FROM glob({quote_literal(path)})")
```

For `information_schema` WHERE clauses, use parameterized queries instead (they work in value context):

```python
con.execute("""
    SELECT column_name FROM information_schema.columns
    WHERE table_schema = ? AND table_name = ?
""", [schema, table])
```

### Validate workspace names at script entry points

Every script `main()` that accepts `--workspace` must validate the name:

```python
if not WORKSPACE_NAME_RE.match(args.workspace):
    print(f"ERROR: Invalid workspace name: {args.workspace}")
    sys.exit(1)
```

### Path builders validate inputs

`build_repo_prefix()`, `build_branch_prefix()`, and `build_staging_path()` in `registry_config.py` validate their inputs. Do not bypass these functions with manual path construction.

## Known Trade-offs

- **HuggingFace backend** passes S3 write credentials to external containers (documented, accepted)
- **Layer 3 catalog check** is skipped on all PRs (no write credentials available). Full testing happens via `/run-extract`
- **`template-setup.yml`** uses `${{ steps.*.outputs }}` in run blocks (safe, self-deleting workflow)
