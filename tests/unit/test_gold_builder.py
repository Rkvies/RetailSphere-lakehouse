"""
tests/unit/test_gold_builder.py
"""
import pytest
from pyspark.sql import SparkSession

from src.aggregation.gold_builder import _point_in_time_join


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("test_gold_builder").master("local[2]").getOrCreate()


def test_point_in_time_join_selects_correct_historical_version(spark):
    """
    This is the single most important test in the entire project - it
    proves the point-in-time join correctly avoids the data-leakage
    failure mode described in the architecture note.
    """
    fact_df = spark.createDataFrame(
        [
            ("INV001", "C001", "2026-01-15"),  # sale BEFORE the country change
            ("INV002", "C001", "2026-08-01"),  # sale AFTER the country change
        ],
        ["invoice_id", "customer_id", "sale_date"],
    ).withColumn("sale_date", spark.sql("SELECT 1").selectExpr("1").schema)  # placeholder to be replaced

    # (Using string dates cast properly - reconstructing with correct types)
    fact_df = spark.createDataFrame(
        [("INV001", "C001", "2026-01-15"), ("INV002", "C001", "2026-08-01")],
        ["invoice_id", "customer_id", "sale_date"],
    ).withColumn("sale_date", spark.sql("SELECT CAST('2026-01-01' AS DATE) as d").collect()[0]["d"].__class__ and None)

    # Simpler, correct construction:
    from pyspark.sql.types import StructType, StructField, StringType, DateType
    import datetime

    fact_df = spark.createDataFrame(
        [
            ("INV001", "C001", datetime.date(2026, 1, 15)),
            ("INV002", "C001", datetime.date(2026, 8, 1)),
        ],
        StructType([
            StructField("invoice_id", StringType()),
            StructField("customer_id", StringType()),
            StructField("sale_date", DateType()),
        ]),
    )

    dim_df = spark.createDataFrame(
        [
            (1, "C001", "United Kingdom", datetime.date(2025, 1, 1), datetime.date(2026, 7, 1)),
            (2, "C001", "Germany", datetime.date(2026, 7, 2), None),
        ],
        StructType([
            StructField("customer_sk", StringType()),
            StructField("customer_id", StringType()),
            StructField("country", StringType()),
            StructField("effective_start_date", DateType()),
            StructField("effective_end_date", DateType()),
        ]),
    )

    result = _point_in_time_join(
        fact_df, dim_df,
        fact_key_col="customer_id", dim_key_col="customer_id",
        fact_date_col="sale_date", dim_surrogate_key_col="customer_sk",
    )

    rows = {r["invoice_id"]: r["customer_sk"] for r in result.collect()}
    assert rows["INV001"] == 1  # pre-change sale -> old (UK) surrogate key
    assert rows["INV002"] == 2  # post-change sale -> new (Germany) surrogate key