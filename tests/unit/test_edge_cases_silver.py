"""
tests/unit/test_edge_cases_silver.py
"""
import pytest
from pyspark.sql import SparkSession

from src.common.delta_utils import merge_upsert


@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder.appName("test_edge_cases_silver")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def test_business_key_appearing_across_two_separate_incremental_runs(spark, tmp_path):
    """
    Simulates a business-key correction arriving on a LATER day, not
    within the same batch - e.g. INV001's quantity was recorded wrong
    on day 1, corrected on day 2. This must resolve via merge_upsert's
    update-in-place, not create a duplicate row - proving the
    cross-batch (not just within-batch) case is handled correctly.
    """
    target = str(tmp_path / "fact_edge_case")

    day1 = spark.createDataFrame([("INV001", 5)], ["invoice_id", "quantity"])
    merge_upsert(spark, day1, target, business_key=["invoice_id"])

    day2_correction = spark.createDataFrame([("INV001", 8)], ["invoice_id", "quantity"])
    merge_upsert(spark, day2_correction, target, business_key=["invoice_id"])

    result = spark.read.format("delta").load(target)
    assert result.count() == 1  # still exactly one row for INV001
    assert result.collect()[0]["quantity"] == 8  # corrected value won


def test_new_business_key_and_correction_in_same_run(spark, tmp_path):
    """Mixed batch: one brand-new key, one correction to an existing key,
    in the SAME merge_upsert call - proves whenMatched/whenNotMatched
    branches both fire correctly within a single MERGE statement."""
    target = str(tmp_path / "fact_edge_case_2")

    initial = spark.createDataFrame([("INV001", 5)], ["invoice_id", "quantity"])
    merge_upsert(spark, initial, target, business_key=["invoice_id"])

    mixed_batch = spark.createDataFrame(
        [("INV001", 99), ("INV002", 3)], ["invoice_id", "quantity"]
    )
    merge_upsert(spark, mixed_batch, target, business_key=["invoice_id"])

    result = spark.read.format("delta").load(target)
    assert result.count() == 2
    assert result.filter("invoice_id = 'INV001'").collect()[0]["quantity"] == 99
    assert result.filter("invoice_id = 'INV002'").collect()[0]["quantity"] == 3