"""
tests/integration/test_framework_integration.py

Integration test chaining: config_loader -> data_quality -> delta_utils
(with logger and exception_handler operating implicitly throughout, since
every module under test imports and uses them).

This test simulates a miniature two-day Bronze -> Silver flow for the
'customer' dimension, proving:
1. Config drives table-specific behavior (business key, DQ rules, SCD type)
   without any hardcoded table logic in this test itself.
2. The DQ engine correctly quarantines the row with a null country.
3. scd2_merge correctly historizes C001's change and leaves C002 untouched
   across the two simulated daily batches.

This is deliberately NOT a test of bronze_loader.py / silver_processor.py
(those don't exist yet - built in the next modules) - it proves the
FRAMEWORK itself is soundly wired before we build pipelines on top of it.
"""

import shutil
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from src.common.data_quality import validate
from src.common.delta_utils import scd2_merge

FIXTURE_DIR = Path("/Users/raghupathyjothibalaji@gmail.com/RetailSphere-lakehouse/tests/fixtures/sample_data")


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.appName("test_framework_integration")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def silver_target_path(tmp_path):
    path = tmp_path / "silver" / "dim_customer"
    yield str(path)
    if path.exists():
        shutil.rmtree(path)


# Config normally comes from table_config.yaml via load_table_config("customer") -
# reproduced inline here so this test doesn't depend on the real project config
# file's exact contents changing over time, keeping the test isolated (NFR-08).
CUSTOMER_DQ_RULES = [
    {"type": "not_null", "column": "customer_id"},
    {"type": "not_null", "column": "country"},
]
CUSTOMER_BUSINESS_KEY = ["customer_id"]
CUSTOMER_TRACKED_COLUMNS = ["country", "segment"]


def test_day1_ingestion_quarantines_null_country_row(spark):
    """
    Chains: read raw CSV -> validate() (data_quality.py).
    Asserts the DQ engine correctly isolates C003 (null country) while
    letting C001/C002 (clean rows) pass through.
    """
    raw_df = spark.read.csv(str(FIXTURE_DIR / "customer_sample_day1.csv"), header=True)

    result = validate(raw_df, dq_rules=CUSTOMER_DQ_RULES, table_name="customer")

    assert result.valid_count == 2
    assert result.invalid_count == 1
    assert result.invalid_df.collect()[0]["customer_id"] == "C003"
    assert any(v.column == "country" for v in result.violations)


def test_day1_valid_rows_load_into_scd2_dimension_as_current(spark, silver_target_path):
    """
    Chains: validate() -> scd2_merge() (delta_utils.py).
    Only the DQ-valid rows from day 1 should reach the Silver dimension,
    both marked as the current version.
    """
    raw_df = spark.read.csv(str(FIXTURE_DIR / "customer_sample_day1.csv"), header=True)
    result = validate(raw_df, dq_rules=CUSTOMER_DQ_RULES, table_name="customer")

    scd2_merge(
        spark, result.valid_df, silver_target_path,
        business_key=CUSTOMER_BUSINESS_KEY,
        tracked_columns=CUSTOMER_TRACKED_COLUMNS,
        surrogate_key_col="customer_sk",
    )

    silver_df = spark.read.format("delta").load(silver_target_path)
    assert silver_df.count() == 2  # C003 correctly excluded (quarantined upstream)
    assert silver_df.filter("is_current = true").count() == 2


def test_day2_change_is_historized_correctly_end_to_end(spark, silver_target_path):
    """
    Full two-day chain: validates and loads day 1, then validates and
    merges day 2 - asserting the complete framework produces correct
    point-in-time history for a real business scenario (customer C001
    relocating from UK to Germany).
    """
    # --- Day 1 ---
    day1_raw = spark.read.csv(str(FIXTURE_DIR / "customer_sample_day1.csv"), header=True)
    day1_result = validate(day1_raw, dq_rules=CUSTOMER_DQ_RULES, table_name="customer")
    scd2_merge(
        spark, day1_result.valid_df, silver_target_path,
        business_key=CUSTOMER_BUSINESS_KEY,
        tracked_columns=CUSTOMER_TRACKED_COLUMNS,
        surrogate_key_col="customer_sk",
    )

    # --- Day 2 ---
    day2_raw = spark.read.csv(str(FIXTURE_DIR / "customer_sample_day2.csv"), header=True)
    day2_result = validate(day2_raw, dq_rules=CUSTOMER_DQ_RULES, table_name="customer")
    assert day2_result.invalid_count == 0  # day 2 fixture is deliberately all-clean

    scd2_merge(
        spark, day2_result.valid_df, silver_target_path,
        business_key=CUSTOMER_BUSINESS_KEY,
        tracked_columns=CUSTOMER_TRACKED_COLUMNS,
        surrogate_key_col="customer_sk",
    )

    silver_df = spark.read.format("delta").load(silver_target_path)

    # C001 should now have 2 rows (old UK version closed, new Germany version current)
    c001_rows = silver_df.filter("customer_id = 'C001'").collect()
    assert len(c001_rows) == 2

    c001_current = [r for r in c001_rows if r["is_current"]][0]
    c001_closed = [r for r in c001_rows if not r["is_current"]][0]
    assert c001_current["country"] == "Germany"
    assert c001_closed["country"] == "United Kingdom"
    assert c001_closed["effective_end_date"] is not None

    # C002 unchanged between day 1 and day 2 - should still have exactly 1 row,
    # proving scd2_merge does NOT create spurious history for unchanged entities.
    c002_rows = silver_df.filter("customer_id = 'C002'").collect()
    assert len(c002_rows) == 1
    assert c002_rows[0]["is_current"] is True

    # Overall table sanity: C001 (2 versions) + C002 (1 version) = 3 rows total
    # (C003 never entered Silver at all, on either day, per the DQ quarantine)
    assert silver_df.count() == 3