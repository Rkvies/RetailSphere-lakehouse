"""
tests/unit/test_delta_utils.py
"""
import pytest
from pyspark.sql import SparkSession

from src.common.delta_utils import merge_upsert, scd2_merge


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    return (
        SparkSession.builder.appName("test_delta_utils")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def test_merge_upsert_initial_write_creates_table(spark, tmp_path):
    target = str(tmp_path / "fact_test")
    df = spark.createDataFrame([("INV001", 5), ("INV002", 3)], ["invoice_id", "quantity"])

    merge_upsert(spark, df, target, business_key=["invoice_id"])

    result = spark.read.format("delta").load(target)
    assert result.count() == 2


def test_merge_upsert_updates_existing_and_inserts_new(spark, tmp_path):
    target = str(tmp_path / "fact_test_2")
    initial = spark.createDataFrame([("INV001", 5)], ["invoice_id", "quantity"])
    merge_upsert(spark, initial, target, business_key=["invoice_id"])

    # Re-run with an update to INV001 and a brand new INV002
    incoming = spark.createDataFrame([("INV001", 99), ("INV002", 3)], ["invoice_id", "quantity"])
    merge_upsert(spark, incoming, target, business_key=["invoice_id"])

    result = spark.read.format("delta").load(target)
    assert result.count() == 2  # no duplicate for INV001
    inv001_qty = result.filter("invoice_id = 'INV001'").collect()[0]["quantity"]
    assert inv001_qty == 99  # updated, not duplicated


def test_scd2_initial_load_marks_all_rows_current(spark, tmp_path):
    target = str(tmp_path / "dim_test")
    df = spark.createDataFrame([("C001", "UK"), ("C002", "France")], ["customer_id", "country"])

    scd2_merge(spark, df, target, business_key=["customer_id"], tracked_columns=["country"], surrogate_key_col="customer_sk")

    result = spark.read.format("delta").load(target)
    assert result.count() == 2
    assert result.filter("is_current = true").count() == 2


def test_scd2_change_closes_old_and_inserts_new_version(spark, tmp_path):
    target = str(tmp_path / "dim_test_2")
    initial = spark.createDataFrame([("C001", "UK")], ["customer_id", "country"])
    scd2_merge(spark, initial, target, business_key=["customer_id"], tracked_columns=["country"], surrogate_key_col="customer_sk")

    # Simulate the customer's country changing
    changed = spark.createDataFrame([("C001", "France")], ["customer_id", "country"])
    scd2_merge(spark, changed, target, business_key=["customer_id"], tracked_columns=["country"], surrogate_key_col="customer_sk")

    result = spark.read.format("delta").load(target)
    assert result.count() == 2  # old (closed) + new (current) version

    old_version = result.filter("is_current = false").collect()[0]
    new_version = result.filter("is_current = true").collect()[0]

    assert old_version["country"] == "UK"
    assert old_version["effective_end_date"] is not None
    assert new_version["country"] == "France"
    assert new_version["effective_end_date"] is None


def test_scd2_no_change_does_not_create_new_version(spark, tmp_path):
    target = str(tmp_path / "dim_test_3")
    initial = spark.createDataFrame([("C001", "UK")], ["customer_id", "country"])
    scd2_merge(spark, initial, target, business_key=["customer_id"], tracked_columns=["country"], surrogate_key_col="customer_sk")

    # Re-run with IDENTICAL data - should be a no-op, not a new version
    same = spark.createDataFrame([("C001", "UK")], ["customer_id", "country"])
    scd2_merge(spark, same, target, business_key=["customer_id"], tracked_columns=["country"], surrogate_key_col="customer_sk")

    result = spark.read.format("delta").load(target)
    assert result.count() == 1  # still just one row - no spurious history