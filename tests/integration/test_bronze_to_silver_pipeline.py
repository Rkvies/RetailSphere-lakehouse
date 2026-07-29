"""
tests/integration/test_bronze_to_silver_pipeline.py
Proves the ACTUAL run_bronze_ingestion() -> process_fact_table() chain
works end-to-end, using a temp, isolated config directory (not the
real project config/) so this test never touches real data/ paths and
is safe to re-run repeatedly.
"""
import os

import pytest

import src.common.config_loader as config_loader_module


@pytest.fixture
def isolated_env(tmp_path, monkeypatch):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    landing_dir = tmp_path / "data" / "landing" / "sales"
    landing_dir.mkdir(parents=True)

    (config_dir / "base_config.yaml").write_text(f"""
paths:
  landing_zone: "{tmp_path}/data/landing"
  bronze: "{tmp_path}/data/bronze"
  silver: "{tmp_path}/data/silver"
  gold: "{tmp_path}/data/gold"
  quarantine: "{tmp_path}/data/quarantine"
""")
    (config_dir / "table_config.yaml").write_text("""
tables:
  sales:
    source_path: "sales/"
    schema:
      - {name: invoice_id, type: string}
      - {name: stock_code, type: string}
      - {name: quantity, type: integer}
    business_key: ["invoice_id", "stock_code"]
    scd_type: null
    dq_rules:
      - {type: not_null, column: stock_code}
""")

    (landing_dir / "sales_sample.csv").write_text(
        "invoice_id,stock_code,quantity\n"
        "INV001,A100,5\n"
        "INV002,,3\n"
    )

    monkeypatch.setattr(config_loader_module, "CONFIG_DIR", config_dir)
    monkeypatch.delenv("ENV", raising=False)
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    yield tmp_path


def test_bronze_to_silver_end_to_end_for_sales(isolated_env, spark):
    from src.ingestion.bronze_loader import run_bronze_ingestion
    from src.transformation.silver_processor import process_fact_table

    bronze_summary = run_bronze_ingestion("sales")
    assert bronze_summary["valid_count"] == 1
    assert bronze_summary["invalid_count"] == 1

    silver_summary = process_fact_table("sales")
    assert silver_summary["valid_count"] == 1
