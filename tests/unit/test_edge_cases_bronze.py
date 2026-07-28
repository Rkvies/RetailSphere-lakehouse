"""
tests/unit/test_edge_cases_bronze.py
"""
import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from src.common.data_quality import validate


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    return SparkSession.builder.appName("test_edge_cases").master("local[2]").getOrCreate()


def test_validate_handles_empty_dataframe_without_error(spark):
    """
    An empty (header-only) source file should produce a clean
    "0 valid, 0 invalid, 100% pass rate" result, NOT an exception -
    a crash here would incorrectly halt the entire Bronze pipeline
    over a legitimately empty (not malformed) daily file.
    """
    schema = StructType([
        StructField("invoice_id", StringType()),
        StructField("quantity", IntegerType()),
    ])
    empty_df = spark.createDataFrame([], schema)

    dq_rules = [{"type": "not_null", "column": "invoice_id"}]
    result = validate(empty_df, dq_rules, table_name="test_empty")

    assert result.valid_count == 0
    assert result.invalid_count == 0
    assert result.pass_rate == 100.0  # vacuously true - no rows, no violations