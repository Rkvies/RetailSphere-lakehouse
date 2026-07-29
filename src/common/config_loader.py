"""
src/common/config_loader.py
Environment-aware, layered YAML config loader. ENV=dev (default),
ENV=databricks, or ENV=prod select which override file is merged on
top of base_config.yaml.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional

import yaml

CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"


class ConfigError(Exception):
    pass


def _load_yaml_file(filename: str) -> dict[str, Any]:
    file_path = CONFIG_DIR / filename
    if not file_path.exists():
        raise ConfigError(f"Config file not found: {file_path}")
    try:
        with open(file_path, "r") as f:
            content = yaml.safe_load(f) or {}
    except yaml.YAMLError as e:
        raise ConfigError(f"Invalid YAML in {file_path}: {e}") from e
    return content


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def get_environment() -> str:
    if "DATABRICKS_RUNTIME_VERSION" in os.environ and "ENV" not in os.environ:
        return "databricks"
    return os.environ.get("ENV", "dev").lower()


def load_app_config(env: Optional[str] = None) -> dict[str, Any]:
    resolved_env = env or get_environment()
    base = _load_yaml_file("base_config.yaml")
    try:
        env_overrides = _load_yaml_file(f"{resolved_env}_config.yaml")
    except ConfigError:
        env_overrides = {}
    return _deep_merge(base, env_overrides)


def load_table_config(table_name: str) -> dict[str, Any]:
    all_tables = _load_yaml_file("table_config.yaml").get("tables", {})
    if table_name not in all_tables:
        raise ConfigError(
            f"No configuration found for table '{table_name}'. "
            f"Available: {list(all_tables.keys())}"
        )
    return all_tables[table_name]


def list_configured_tables() -> list[str]:
    all_tables = _load_yaml_file("table_config.yaml").get("tables", {})
    return list(all_tables.keys())


def resolve_table_ref(layer: str, table_name: str) -> str:
    """
    Returns the reference every read/write should target for this
    layer/table: a filesystem PATH for "landing" (always - raw files
    belong in a Volume/local folder, never a catalog table), or for
    bronze/silver/gold either:
      - a managed Unity Catalog table name ("catalog.schema.layer_table")
        when unity_catalog.enabled=true, or
      - a filesystem path (same as resolve_layer_path) otherwise (local dev)

    Why NOT a path-based external table under a Volume for bronze/silver/
    gold: Unity Catalog tables require LOCATION to point at a registered
    "External Location" backed by cloud storage credentials - a Volume
    path doesn't have that, and CREATE TABLE ... LOCATION '/Volumes/...'
    fails with "Missing cloud file system scheme". Managed tables
    (saveAsTable, no LOCATION clause) sidestep this entirely - Unity
    Catalog handles the underlying storage itself. This is also the
    more idiomatic UC pattern, not just a workaround.

    delta_utils.py functions (merge_upsert, scd2_merge, _table_exists)
    detect whether a given ref is a path or a catalog table name by
    checking for "/" - every path in this project starts with "/" or
    contains one; catalog names never do (three dot-separated
    identifiers, e.g. "retail_lakehouse.lakehouse.bronze_sales").
    """
    if layer == "landing":
        return resolve_layer_path(layer, table_name)

    app_config = load_app_config()
    uc_config = app_config.get("unity_catalog", {})

    if uc_config.get("enabled", False):
        catalog = uc_config["catalog"]
        schema = uc_config["schema"]
        return f"{catalog}.{schema}.{layer}_{table_name}"

    return resolve_layer_path(layer, table_name)


def resolve_layer_path(layer: str, table_name: str) -> str:
    """
    Joins app_config's paths.<layer> with the table name (or, for
    landing, the table's configured source_path). Used directly for
    landing zone paths always, and as the fallback for bronze/silver/
    gold when Unity Catalog isn't enabled (local dev).
    """
    app_config = load_app_config()
    path_key = "landing_zone" if layer == "landing" else layer
    base_path = app_config["paths"][path_key].rstrip("/")

    if layer == "landing":
        table_conf = load_table_config(table_name)
        relative = table_conf["source_path"].strip("/")
        return f"{base_path}/{relative}"

    return f"{base_path}/{table_name}"
