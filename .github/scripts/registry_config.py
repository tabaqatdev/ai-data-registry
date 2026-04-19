"""Shared configuration module for the data registry platform.

Reads .github/registry.config.toml and provides helpers for parsing
workspace manifests, discovering workspaces, and resolving backend configs.
"""

from __future__ import annotations

import os
import re
import subprocess
import tomllib
from glob import glob
from pathlib import Path

# Repo root: two levels up from .github/scripts/
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
CONFIG_PATH = REPO_ROOT / ".github" / "registry.config.toml"
WORKSPACES_DIR = REPO_ROOT / "workspaces"

# Valid SPDX license identifiers for code (OSI-approved)
VALID_CODE_LICENSES = {
    "Apache-2.0", "MIT", "BSD-2-Clause", "BSD-3-Clause", "ISC",
    "MPL-2.0", "LGPL-2.1-only", "LGPL-3.0-only", "GPL-2.0-only",
    "GPL-3.0-only", "AGPL-3.0-only", "Unlicense", "0BSD",
}

# Valid SPDX license identifiers for data
VALID_DATA_LICENSES = {
    "CC-BY-4.0", "CC-BY-SA-4.0", "CC0-1.0", "ODbL-1.0", "PDDL-1.0",
    "CC-BY-3.0", "CC-BY-2.0", "CC-BY-NC-4.0", "CC-BY-NC-SA-4.0",
    "public-domain", "CDLA-Permissive-2.0", "CDLA-Sharing-1.0",
}

# Restrictive data licenses that trigger a warning (not a block)
RESTRICTIVE_DATA_LICENSES = {"CC-BY-NC-4.0", "CC-BY-NC-SA-4.0"}

# Valid workspace name pattern
WORKSPACE_NAME_RE = re.compile(r"^[a-z][a-z0-9-]*$")

# Required fields in [tool.registry]
# Note: "table" or "tables" is validated separately (either one must be present)
REQUIRED_REGISTRY_FIELDS = {"description", "schedule", "timeout", "tags", "schema", "mode"}

# Valid modes
VALID_MODES = {"append", "replace", "upsert"}

# Table name pattern (lowercase, starts with letter, underscores and digits allowed)
TABLE_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")

# Required tasks every workspace must define
REQUIRED_TASKS = {"pipeline", "extract", "validate", "dry-run"}


def quote_ident(name: str) -> str:
    """Escape a DuckDB identifier. Wrap in double quotes, double any internal quotes."""
    return '"' + name.replace('"', '""') + '"'


def quote_literal(val: str) -> str:
    """Escape a DuckDB string literal. Wrap in single quotes, double any internal quotes."""
    return "'" + val.replace("'", "''") + "'"


def load_config() -> dict:
    """Load the registry config from .github/registry.config.toml."""
    with open(CONFIG_PATH, "rb") as f:
        return tomllib.load(f)


def load_storage_configs() -> dict[str, dict]:
    """Return all named storage sections as {name: config}.

    Supports two formats in registry.config.toml:

    New (named):
        [storage.eu-hetzner]
        provider = "hetzner"
        endpoint_url_secret = "S3_ENDPOINT_URL"
        ...

    Legacy (flat, auto-wrapped as "default"):
        [storage]
        endpoint_url_secret = "S3_ENDPOINT_URL"
        ...

    Order is preserved. The first entry is the default storage.
    """
    config = load_config()
    storage_section = config.get("storage", {})

    # Detect legacy flat format: has endpoint_url_secret directly
    if "endpoint_url_secret" in storage_section:
        return {"default": storage_section}

    # Named format: each value should be a dict with storage config
    storages = {}
    for name, section in storage_section.items():
        if isinstance(section, dict) and "endpoint_url_secret" in section:
            storages[name] = section
    return storages


def get_default_storage_name() -> str:
    """Return the name of the first defined storage (the default)."""
    storages = load_storage_configs()
    if not storages:
        raise ValueError("No storage targets defined in registry.config.toml")
    return next(iter(storages))


def get_storage_config() -> dict:
    """Return the default storage config.

    Deprecated: prefer load_storage_configs() for multi-storage support.
    """
    return load_storage_configs()[get_default_storage_name()]


def get_workspace_storages(registry: dict) -> list[str]:
    """Get the list of storage target names for a workspace.

    Reads the optional ``storage`` field from ``[tool.registry]``.
    If absent, returns a list containing just the default storage.
    Accepts a string or list of strings.
    """
    available = load_storage_configs()
    raw = registry.get("storage")
    if not raw:
        return [get_default_storage_name()]
    names = [raw] if isinstance(raw, str) else list(raw)
    for name in names:
        if name not in available:
            raise ValueError(
                f"Unknown storage target '{name}'. "
                f"Available: {', '.join(available)}"
            )
    return names


def resolve_storage_env(storage_name: str) -> dict:
    """Resolve a storage target's secret names to env var values.

    Returns a dict with keys: endpoint_url, bucket, region, access_key, secret_key.
    Values are None if the env var is not set.
    """
    storages = load_storage_configs()
    if storage_name not in storages:
        raise ValueError(f"Unknown storage target: {storage_name}")
    cfg = storages[storage_name]
    return {
        "endpoint_url": os.environ.get(cfg["endpoint_url_secret"]),
        "bucket": os.environ.get(cfg["bucket_secret"]),
        "region": os.environ.get(cfg.get("region_secret", ""), ""),
        "access_key": os.environ.get(cfg["write_key_id_secret"]),
        "secret_key": os.environ.get(cfg["write_secret_key_secret"]),
    }


_REPO_RE = re.compile(r"^[a-zA-Z0-9._-]+/[a-zA-Z0-9._-]+$")

def build_repo_prefix() -> str:
    """Build the {owner}/{repo} prefix from GitHub environment variables.

    Uses GITHUB_REPOSITORY (format: owner/repo). Returns empty string
    when not running in CI (local development preserves flat layout).
    """
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    if not repo:
        return ""
    if not _REPO_RE.match(repo):
        raise ValueError(f"Invalid GITHUB_REPOSITORY format: {repo}")
    return repo


def build_branch_prefix() -> str:
    """Return the current branch name from GITHUB_REF_NAME.

    Returns empty string when not running in CI.
    """
    branch = os.environ.get("GITHUB_REF_NAME", "")
    if branch and ".." in branch:
        raise ValueError(f"Invalid GITHUB_REF_NAME (contains ..): {branch}")
    return branch


def build_s3_root(storage_name: str) -> str:
    """Build the full S3 root path for a storage target.

    Returns: s3://{bucket}/{owner}/{repo}/{branch}/
    In local dev (no GitHub env vars): s3://{bucket}/
    """
    creds = resolve_storage_env(storage_name)
    bucket = creds["bucket"] or ""
    repo_prefix = build_repo_prefix()
    branch = build_branch_prefix()

    parts = [f"s3://{bucket}"]
    if repo_prefix:
        parts.append(repo_prefix)
    if branch:
        parts.append(branch)
    return "/".join(parts) + "/"


def build_global_catalog_path(storage_name: str) -> str:
    """Build the S3 path for the global catalog.

    Returns: s3://{bucket}/{owner}/{repo}/{branch}/catalog.duckdb
    """
    storages = load_storage_configs()
    cfg = storages[storage_name]
    root = build_s3_root(storage_name)
    return f"{root}{cfg['global_catalog']}"


def build_staging_path(
    storage_name: str, pr_number: str | int, workspace: str
) -> str:
    """Build the S3 path for PR staging data.

    Returns: s3://{bucket}/{owner}/{repo}/pr/{pr_number}/{workspace}/
    PR staging uses owner/repo but NOT branch (keyed on pr_number for cleanup).
    """
    if not str(pr_number).isdigit():
        raise ValueError(f"PR number must be numeric, got: {pr_number}")
    creds = resolve_storage_env(storage_name)
    bucket = creds["bucket"] or ""
    storages = load_storage_configs()
    cfg = storages[storage_name]
    repo_prefix = build_repo_prefix()

    parts = [f"s3://{bucket}"]
    if repo_prefix:
        parts.append(repo_prefix)
    parts.append(f"{cfg['staging_prefix']}/{pr_number}/{workspace}")
    return "/".join(parts) + "/"


def get_backends() -> dict[str, dict]:
    """Return backends as {name: {workflow, flavors}}."""
    config = load_config()
    backends = {}
    for key, value in config.items():
        if key.startswith("backends."):
            name = key.split(".", 1)[1]
            backends[name] = value
        elif key != "storage" and isinstance(value, dict) and "flavors" in value:
            backends[key] = value
    # Also check for [backends] as a table with sub-tables
    if "backends" in config and isinstance(config["backends"], dict):
        for name, section in config["backends"].items():
            if isinstance(section, dict) and "flavors" in section:
                backends[name] = section
    return backends


# Eagerly load for convenience
SUPPORTED_BACKENDS = get_backends()


def parse_workspace_manifest(path: str | Path) -> dict:
    """Parse a workspace pixi.toml and return its full content."""
    with open(path, "rb") as f:
        return tomllib.load(f)


def parse_workspace_registry(path: str | Path) -> dict | None:
    """Parse [tool.registry] from a workspace pixi.toml. Returns None if missing."""
    manifest = parse_workspace_manifest(path)
    tool = manifest.get("tool", {})
    return tool.get("registry")


def get_workspace_name(path: str | Path) -> str:
    """Extract workspace name from its pixi.toml path."""
    return Path(path).parent.name


def discover_workspaces(workspaces_dir: str | Path | None = None) -> list[dict]:
    """Find all workspaces and return their parsed registry configs.

    Returns a list of dicts with keys: name, path, registry, manifest.
    """
    ws_dir = Path(workspaces_dir) if workspaces_dir else WORKSPACES_DIR
    results = []
    for pixi_path in sorted(ws_dir.glob("*/pixi.toml")):
        manifest = parse_workspace_manifest(pixi_path)
        registry = manifest.get("tool", {}).get("registry")
        results.append({
            "name": pixi_path.parent.name,
            "path": str(pixi_path),
            "registry": registry,
            "manifest": manifest,
        })
    return results


def get_tables(registry: dict) -> list[str]:
    """Normalize table/tables field to a list of table names.

    Accepts either form in pixi.toml:
        table = "name"            -> ["name"]
        tables = ["a", "b"]      -> ["a", "b"]
    """
    tables = registry.get("tables")
    if tables:
        return [tables] if isinstance(tables, str) else list(tables)
    table = registry.get("table")
    if table:
        return [table] if isinstance(table, list) is False else list(table)
    return []


def get_table_checks(registry: dict, table_name: str) -> dict:
    """Get quality checks for a specific table.

    Per-table overrides via [tool.registry.checks.<table_name>] merge on top
    of global [tool.registry.checks] defaults.  Keys that are dicts (i.e. other
    table subsections) are filtered out of the base.
    """
    checks = registry.get("checks", {})
    base = {k: v for k, v in checks.items() if not isinstance(v, dict)}
    table_section = checks.get(table_name)
    if isinstance(table_section, dict):
        base.update(table_section)
    return base


def s5cmd_for_storage(storage_name: str, *args: str) -> subprocess.CompletedProcess:
    """Run s5cmd with credentials for the given storage target."""
    creds = resolve_storage_env(storage_name)
    endpoint = creds["endpoint_url"] or ""
    env = os.environ.copy()
    env["AWS_ACCESS_KEY_ID"] = creds["access_key"] or ""
    env["AWS_SECRET_ACCESS_KEY"] = creds["secret_key"] or ""
    cmd = ["pixi", "run", "s5cmd", "--endpoint-url", endpoint, *args]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=120, env=env)


def resolve_secret_env(secret_name: str) -> str | None:
    """Resolve a secret name to its environment variable value (for CI scripts)."""
    return os.environ.get(secret_name)
