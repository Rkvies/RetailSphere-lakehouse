"""
tests/integration/test_bronze_to_silver_pipeline.py

Proves the ACTUAL pipeline functions (not just the underlying framework)
correctly chain: run_bronze_ingestion() -> process_fact_table(), using
a real (small) source file through real Bronze/Silver Delta writes.
"""
import pytest
import os
from src.common.spark_session import get_spark_session, stop_spark_session
from src.ingestion.bronze_loader import run_bronze_ingestion
from src.transformation.silver_processor import process_fact_table


@pytest.fixture
def isolated_data_dirs(tmp_path, monkeypatch):
    """
    Redirects Bronze/Silver/quarantine writes to a temp directory for
    this test, so integration tests never touch real project data/ -
    keeping integration tests as isolated and re-runnable as unit tests,
    just slower due to real Spark I/O.
    """
    monkeypatch.chdir(tmp_path)
    os.makedirs("data/landing/sales", exist_ok=True)
    with open("data/landing/sales/sales_20260727.csv", "w") as f:
        f.write("invoice_id,stock_code,quantity,unit_price,customer_id,country,invoice_date\n")
        f.write("INV001,A100,5,9.99,C001,United Kingdom,2026-07-27T10:00:00\n")
        f.write("INV002,,3,5.00,C002,France,2026-07-27T11:00:00\n")  # null stock_code - should quarantine
    yield tmp_path


def test_bronze_to_silver_end_to_end_for_sales(isolated_data_dirs):
    bronze_summary = run_bronze_ingestion("sales")
    assert bronze_summary["valid_count"] == 1
    assert bronze_summary["invalid_count"] == 1

    silver_summary = process_fact_table("sales")
    assert silver_summary["valid_count"] == 1  # only the clean row reaches Silver

    stop_spark_session()