import datetime

from pyspark.sql.types import StructType, StructField, StringType, DateType

from src.aggregation.gold_builder import _point_in_time_join


def test_point_in_time_join_selects_correct_historical_version(spark):
    """
    The single most important test in the project: proves a sale made
    BEFORE a customer's country change resolves to the OLD dimension
    version, and a sale made AFTER resolves to the NEW version - not
    "whatever the dimension says today." Without this, SCD2 is
    implemented but silently defeated at the point it's actually used.
    """
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
    assert rows["INV001"] == "1"  # pre-change sale -> old (UK) surrogate key
    assert rows["INV002"] == "2"  # post-change sale -> new (Germany) surrogate key


def test_point_in_time_join_handles_open_current_version_null_end_date(spark):
    """
    Guards specifically against the NULL-comparison trap: without
    coalescing effective_end_date to a sentinel date, `sale_date <= NULL`
    evaluates to NULL (not true) in SQL, and the currently-open dimension
    version would never match any fact row.
    """
    fact_df = spark.createDataFrame(
        [("INV003", "C002", datetime.date(2026, 12, 1))],
        StructType([
            StructField("invoice_id", StringType()),
            StructField("customer_id", StringType()),
            StructField("sale_date", DateType()),
        ]),
    )
    dim_df = spark.createDataFrame(
        [(3, "C002", "France", datetime.date(2026, 1, 1), None)],
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

    row = result.collect()[0]
    assert row["customer_sk"] == "3"  # matched the open (NULL end date) version, not left unmatched
