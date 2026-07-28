"""
tests/unit/test_config_loader.py
"""
import pytest

from src.common.config_loader import (
    _deep_merge,
    ConfigError,
    load_table_config,
)


def test_deep_merge_preserves_untouched_nested_keys():
    base = {
        "spark": {"shuffle_partitions": 8, "app_name": "retail"},
        "paths": {"bronze": "data/bronze"},
    }
    override = {"spark": {"shuffle_partitions": 4}}

    result = _deep_merge(base, override)

    # Overridden key changed...
    assert result["spark"]["shuffle_partitions"] == 4
    # ...but sibling nested key survived (this is the bug a shallow merge causes)
    assert result["spark"]["app_name"] == "retail"
    # ...and untouched top-level key survived
    assert result["paths"]["bronze"] == "data/bronze"


def test_deep_merge_adds_new_top_level_keys():
    base = {"spark": {"app_name": "retail"}}
    override = {"logging": {"level": "DEBUG"}}
    result = _deep_merge(base, override)
    assert result["logging"]["level"] == "DEBUG"
    assert result["spark"]["app_name"] == "retail"


def test_load_table_config_returns_expected_keys(tmp_path, monkeypatch):
    # Point CONFIG_DIR at a temp fixture directory so this test doesn't
    # depend on the real project's config/ files - keeps unit tests
    # isolated and fast, per NFR-08 (testability).
    import src.common.config_loader as config_loader_module

    fixture_config = tmp_path / "table_config.yaml"
    fixture_config.write_text(
        "tables:\n"
        "  sales:\n"
        "    business_key: ['invoice_id']\n"
        "    scd_type: null\n"
    )
    monkeypatch.setattr(config_loader_module, "CONFIG_DIR", tmp_path)

    result = load_table_config("sales")
    assert result["business_key"] == ["invoice_id"]
    assert result["scd_type"] is None


def test_load_table_config_raises_for_unknown_table(tmp_path, monkeypatch):
    import src.common.config_loader as config_loader_module

    fixture_config = tmp_path / "table_config.yaml"
    fixture_config.write_text("tables:\n  sales:\n    business_key: ['invoice_id']\n")
    monkeypatch.setattr(config_loader_module, "CONFIG_DIR", tmp_path)

    with pytest.raises(ConfigError, match="No configuration found for table 'unknown_table'"):
        load_table_config("unknown_table")