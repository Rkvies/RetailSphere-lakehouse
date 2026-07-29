import pytest

from src.common.config_loader import (
    _deep_merge,
    ConfigError,
    load_table_config,
    resolve_layer_path,
    resolve_table_ref,
)


def test_deep_merge_preserves_untouched_nested_keys():
    base = {
        "spark": {"shuffle_partitions": 8, "app_name": "retail"},
        "paths": {"bronze": "data/bronze"},
    }
    override = {"spark": {"shuffle_partitions": 4}}
    result = _deep_merge(base, override)
    assert result["spark"]["shuffle_partitions"] == 4
    assert result["spark"]["app_name"] == "retail"
    assert result["paths"]["bronze"] == "data/bronze"


def test_deep_merge_adds_new_top_level_keys():
    base = {"spark": {"app_name": "retail"}}
    override = {"logging": {"level": "DEBUG"}}
    result = _deep_merge(base, override)
    assert result["logging"]["level"] == "DEBUG"
    assert result["spark"]["app_name"] == "retail"


def test_load_table_config_returns_expected_keys(tmp_path, monkeypatch):
    import src.common.config_loader as config_loader_module

    (tmp_path / "table_config.yaml").write_text(
        "tables:\n  sales:\n    business_key: ['invoice_id']\n    scd_type: null\n"
    )
    monkeypatch.setattr(config_loader_module, "CONFIG_DIR", tmp_path)

    result = load_table_config("sales")
    assert result["business_key"] == ["invoice_id"]
    assert result["scd_type"] is None


def test_load_table_config_raises_for_unknown_table(tmp_path, monkeypatch):
    import src.common.config_loader as config_loader_module

    (tmp_path / "table_config.yaml").write_text("tables:\n  sales:\n    business_key: ['invoice_id']\n")
    monkeypatch.setattr(config_loader_module, "CONFIG_DIR", tmp_path)

    with pytest.raises(ConfigError, match="No configuration found for table 'unknown_table'"):
        load_table_config("unknown_table")


def test_resolve_layer_path_joins_base_and_table_name(tmp_path, monkeypatch):
    import src.common.config_loader as config_loader_module

    (tmp_path / "base_config.yaml").write_text(
        "paths:\n  landing_zone: 'data/landing'\n  bronze: 'data/bronze'\n"
    )
    (tmp_path / "table_config.yaml").write_text(
        "tables:\n  sales:\n    source_path: 'sales/'\n"
    )
    monkeypatch.setattr(config_loader_module, "CONFIG_DIR", tmp_path)
    monkeypatch.delenv("ENV", raising=False)
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)

    assert resolve_layer_path("bronze", "sales") == "data/bronze/sales"
    assert resolve_layer_path("landing", "sales") == "data/landing/sales"


def test_resolve_layer_path_switches_environment_via_env_var(tmp_path, monkeypatch):
    import src.common.config_loader as config_loader_module

    (tmp_path / "base_config.yaml").write_text(
        "paths:\n  landing_zone: 'data/landing'\n  bronze: 'data/bronze'\n"
    )
    (tmp_path / "databricks_config.yaml").write_text(
        "paths:\n  bronze: '/Volumes/cat/schema/vol/bronze'\n"
    )
    (tmp_path / "table_config.yaml").write_text(
        "tables:\n  sales:\n    source_path: 'sales/'\n"
    )
    monkeypatch.setattr(config_loader_module, "CONFIG_DIR", tmp_path)
    monkeypatch.setenv("ENV", "databricks")

    assert resolve_layer_path("bronze", "sales") == "/Volumes/cat/schema/vol/bronze/sales"


def test_resolve_table_ref_returns_managed_catalog_name_when_uc_enabled(tmp_path, monkeypatch):
    import src.common.config_loader as config_loader_module

    (tmp_path / "base_config.yaml").write_text(
        "paths:\n  landing_zone: 'data/landing'\n  bronze: 'data/bronze'\n"
        "unity_catalog:\n  enabled: false\n"
    )
    (tmp_path / "databricks_config.yaml").write_text(
        "unity_catalog:\n  enabled: true\n  catalog: 'retail_lakehouse'\n  schema: 'lakehouse'\n"
    )
    (tmp_path / "table_config.yaml").write_text("tables:\n  sales:\n    source_path: 'sales/'\n")
    monkeypatch.setattr(config_loader_module, "CONFIG_DIR", tmp_path)
    monkeypatch.setenv("ENV", "databricks")

    assert resolve_table_ref("bronze", "sales") == "retail_lakehouse.lakehouse.bronze_sales"
    # landing is ALWAYS a path, even with unity_catalog enabled - raw
    # files belong in a Volume, not a managed table
    assert resolve_table_ref("landing", "sales") == "data/landing/sales"


def test_resolve_table_ref_falls_back_to_path_when_uc_disabled(tmp_path, monkeypatch):
    import src.common.config_loader as config_loader_module

    (tmp_path / "base_config.yaml").write_text(
        "paths:\n  landing_zone: 'data/landing'\n  bronze: 'data/bronze'\n"
        "unity_catalog:\n  enabled: false\n"
    )
    (tmp_path / "table_config.yaml").write_text("tables:\n  sales:\n    source_path: 'sales/'\n")
    monkeypatch.setattr(config_loader_module, "CONFIG_DIR", tmp_path)
    monkeypatch.delenv("ENV", raising=False)
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)

    assert resolve_table_ref("bronze", "sales") == "data/bronze/sales"
