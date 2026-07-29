from src.transformation.scd2_handler import _deduplicate_within_batch


def test_deduplicate_within_batch_keeps_most_recent_ingest(spark):
    df = spark.createDataFrame(
        [
            ("C001", "United Kingdom", "2026-07-25T06:00:00"),
            ("C001", "Germany", "2026-07-25T18:00:00"),
        ],
        ["customer_id", "country", "_ingest_ts"],
    )
    result = _deduplicate_within_batch(df, business_key=["customer_id"])
    assert result.count() == 1
    assert result.collect()[0]["country"] == "Germany"


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
