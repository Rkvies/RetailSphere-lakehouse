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
    Returns the reference every read/write should target: a filesystem
    PATH for "landing" (always - raw files belong in a Volume/local
    folder, never a catalog table), or for bronze/silver/gold/
    quarantine either:
      - a managed Unity Catalog table name
        ("catalog.<layer_schema>.table_name") when unity_catalog.enabled
        is true - e.g. "retail_lakehouse.silver.dim_customer" - or
      - a filesystem path (same as resolve_layer_path) otherwise
        (local dev, no metastore)

    Schema-PER-LAYER (bronze/silver/gold/quarantine each their own
    schema), not a single shared schema with layer-prefixed table
    names - this is the idiomatic Unity Catalog pattern: Catalog
    Explorer browses by schema, so "gold.fact_sales" reads naturally
    and groups with every other Gold table, rather than everything
    flattened into one schema disambiguated only by a name prefix.

    table_name does NOT need to match a table_config.yaml key for the
    "gold" layer specifically - Gold is a derived/aggregated layer
    (e.g. gold_builder.py writes "dim_customer", "fact_sales",
    "fact_returns" etc., which are business-facing names, not
    necessarily 1:1 with Silver's per-domain table_config entries).
    """
    if layer == "landing":
        return resolve_layer_path(layer, table_name)

    app_config = load_app_config()
    uc_config = app_config.get("unity_catalog", {})

    if uc_config.get("enabled", False):
        catalog = uc_config["catalog"]
        schema = uc_config["schemas"][layer]
        return f"{catalog}.{schema}.{table_name}"

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
