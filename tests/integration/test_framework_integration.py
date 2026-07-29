"""
tests/integration/test_framework_integration.py
Chains config_loader -> data_quality -> delta_utils across a simulated
two-day customer dimension load, proving the framework modules produce
compatible outputs when combined (not just correct in isolation).
"""
import shutil
from pathlib import Path

import pytest

from src.common.data_quality import validate
from src.common.delta_utils import scd2_merge

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "sample_data"

CUSTOMER_DQ_RULES = [
    {"type": "not_null", "column": "customer_id"},
    {"type": "not_null", "column": "country"},
]
CUSTOMER_BUSINESS_KEY = ["customer_id"]
CUSTOMER_TRACKED_COLUMNS = ["country", "segment"]


@pytest.fixture
def silver_target_path(tmp_path):
    path = tmp_path / "silver" / "dim_customer"
    yield str(path)
    if path.exists():
        shutil.rmtree(path)


def test_day1_ingestion_quarantines_null_country_row(spark):
    raw_df = spark.read.csv(str(FIXTURE_DIR / "customer_sample_day1.csv"), header=True)
    result = validate(raw_df, dq_rules=CUSTOMER_DQ_RULES, table_name="customer")

    assert result.valid_count == 2
    assert result.invalid_count == 1
    assert result.invalid_df.collect()[0]["customer_id"] == "C003"


def test_day1_valid_rows_load_into_scd2_dimension_as_current(spark, silver_target_path):
    raw_df = spark.read.csv(str(FIXTURE_DIR / "customer_sample_day1.csv"), header=True)
    result = validate(raw_df, dq_rules=CUSTOMER_DQ_RULES, table_name="customer")

    scd2_merge(
        spark, result.valid_df, silver_target_path,
        business_key=CUSTOMER_BUSINESS_KEY, tracked_columns=CUSTOMER_TRACKED_COLUMNS,
        surrogate_key_col="customer_sk",
    )

    silver_df = spark.read.format("delta").load(silver_target_path)
    assert silver_df.count() == 2
    assert silver_df.filter("is_current = true").count() == 2


def test_day2_change_is_historized_correctly_end_to_end(spark, silver_target_path):
    day1_raw = spark.read.csv(str(FIXTURE_DIR / "customer_sample_day1.csv"), header=True)
    day1_result = validate(day1_raw, dq_rules=CUSTOMER_DQ_RULES, table_name="customer")
    scd2_merge(
        spark, day1_result.valid_df, silver_target_path,
        business_key=CUSTOMER_BUSINESS_KEY, tracked_columns=CUSTOMER_TRACKED_COLUMNS,
        surrogate_key_col="customer_sk",
    )

    day2_raw = spark.read.csv(str(FIXTURE_DIR / "customer_sample_day2.csv"), header=True)
    day2_result = validate(day2_raw, dq_rules=CUSTOMER_DQ_RULES, table_name="customer")
    assert day2_result.invalid_count == 0

    scd2_merge(
        spark, day2_result.valid_df, silver_target_path,
        business_key=CUSTOMER_BUSINESS_KEY, tracked_columns=CUSTOMER_TRACKED_COLUMNS,
        surrogate_key_col="customer_sk",
    )

    silver_df = spark.read.format("delta").load(silver_target_path)

    c001_rows = silver_df.filter("customer_id = 'C001'").collect()
    assert len(c001_rows) == 2
    c001_current = [r for r in c001_rows if r["is_current"]][0]
    c001_closed = [r for r in c001_rows if not r["is_current"]][0]
    assert c001_current["country"] == "Germany"
    assert c001_closed["country"] == "United Kingdom"
    assert c001_closed["effective_end_date"] is not None

    c002_rows = silver_df.filter("customer_id = 'C002'").collect()
    assert len(c002_rows) == 1
    assert c002_rows[0]["is_current"] is True

    assert silver_df.count() == 3
