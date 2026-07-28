"""
tests/unit/test_edge_cases_scd2.py
"""
import pytest
from pyspark.sql import SparkSession

from src.common.delta_utils import scd2_merge


@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder.appName("test_edge_cases_scd2")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def test_scd2_empty_target_with_zero_incoming_rows_is_a_safe_noop(spark, tmp_path):
    """Guards against a crash if scd2_merge is somehow invoked with an
    empty incoming DataFrame against an already-existing target."""
    target = str(tmp_path / "dim_edge_empty")
    initial = spark.createDataFrame([("C001", "UK")], ["customer_id", "country"])
    scd2_merge(spark, initial, target, business_key=["customer_id"], tracked_columns=["country"], surrogate_key_col="customer_sk")

    empty = spark.createDataFrame([], initial.schema)
    # Should not raise, should not change existing state
    scd2_merge(spark, empty, target, business_key=["customer_id"], tracked_columns=["country"], surrogate_key_col="customer_sk")

    result = spark.read.format("delta").load(target)
    assert result.count() == 1
    assert result.filter("is_current = true").count() == 1


def test_scd2_multiple_customers_mixed_new_and_changed_same_run(spark, tmp_path):
    """A single run containing one brand-new customer, one changed
    customer, and one unchanged customer - all three code paths inside
    scd2_merge exercised together, not in isolation."""
    target = str(tmp_path / "dim_edge_mixed")
    initial = spark.createDataFrame(
        [("C001", "UK"), ("C002", "France")], ["customer_id", "country"]
    )
    scd2_merge(spark, initial, target, business_key=["customer_id"], tracked_columns=["country"], surrogate_key_col="customer_sk")

    mixed = spark.createDataFrame(
        [("C001", "Germany"), ("C002", "France"), ("C003", "Spain")],
        ["customer_id", "country"],
    )
    scd2_merge(spark, mixed, target, business_key=["customer_id"], tracked_columns=["country"], surrogate_key_col="customer_sk")

    result = spark.read.format("delta").load(target)
    assert result.count() == 4  # C001 old + C001 new + C002 (unchanged, 1 row) + C003 new
    assert result.filter("customer_id = 'C001'").count() == 2
    assert result.filter("customer_id = 'C002'").count() == 1
    assert result.filter("customer_id = 'C003'").count() == 1
    assert result.filter("customer_id = 'C003' AND is_current = true").collect()[0]["country"] == "Spain"