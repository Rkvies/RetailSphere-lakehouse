from src.transformation.silver_processor import _deduplicate_on_business_key


def test_deduplicate_keeps_most_recently_ingested_row(spark):
    df = spark.createDataFrame(
        [
            ("INV001", "A100", 5, "2026-07-25T10:00:00"),
            ("INV001", "A100", 7, "2026-07-25T14:00:00"),
        ],
        ["invoice_id", "stock_code", "quantity", "_ingest_ts"],
    )
    result = _deduplicate_on_business_key(df, business_key=["invoice_id", "stock_code"])
    assert result.count() == 1
    assert result.collect()[0]["quantity"] == 7


def test_deduplicate_preserves_distinct_business_keys(spark):
    df = spark.createDataFrame(
        [
            ("INV001", "A100", 5, "2026-07-25T10:00:00"),
            ("INV002", "A101", 3, "2026-07-25T10:00:00"),
        ],
        ["invoice_id", "stock_code", "quantity", "_ingest_ts"],
    )
    result = _deduplicate_on_business_key(df, business_key=["invoice_id", "stock_code"])
    assert result.count() == 2


def test_deduplicate_handles_single_row_per_key_unchanged(spark):
    df = spark.createDataFrame(
        [("INV001", "A100", 5, "2026-07-25T10:00:00")],
        ["invoice_id", "stock_code", "quantity", "_ingest_ts"],
    )
    result = _deduplicate_on_business_key(df, business_key=["invoice_id", "stock_code"])
    assert result.count() == 1
    assert result.collect()[0]["quantity"] == 5
