"""
tests/unit/test_scd2_handler.py
"""
import pytest
from pyspark.sql import SparkSession

from src.transformation.scd2_handler import _deduplicate_within_batch


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("test_scd2_handler").master("local[2]").getOrCreate()


def test_deduplicate_within_batch_keeps_most_recent_ingest(spark):
    """
    Simulates the exact failure scenario from the architecture note:
    two rows for C001 in the same incoming batch, with different
    country values due to an early vs. late extract.
    """
    df = spark.createDataFrame(
        [
            ("C001", "United Kingdom", "2026-07-25T06:00:00"),  # early, partial extract
            ("C001", "Germany", "2026-07-25T18:00:00"),          # later, corrected extract
        ],
        ["customer_id", "country", "_ingest_ts"],
    )

    result = _deduplicate_within_batch(df, business_key=["customer_id"])

    assert result.count() == 1  # exactly one row per business key, guaranteed
    assert result.collect()[0]["country"] == "Germany"  # later extract wins


def test_deduplicate_within_batch_preserves_distinct_customers(spark):
    df = spark.createDataFrame(
        [
            ("C001", "United Kingdom", "2026-07-25T06:00:00"),
            ("C002", "France", "2026-07-25T06:00:00"),
        ],
        ["customer_id", "country", "_ingest_ts"],
    )
    result = _deduplicate_within_batch(df, business_key=["customer_id"])
    assert result.count() == 2


def test_deduplicate_within_batch_single_row_unaffected(spark):
    df = spark.createDataFrame(
        [("C001", "United Kingdom", "2026-07-25T06:00:00")],
        ["customer_id", "country", "_ingest_ts"],
    )
    result = _deduplicate_within_batch(df, business_key=["customer_id"])
    assert result.count() == 1
    assert result.collect()[0]["country"] == "United Kingdom"