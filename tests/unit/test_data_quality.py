import pytest

from src.common.data_quality import validate, _build_rule_check


def test_validate_splits_valid_and_invalid_rows(spark):
    df = spark.createDataFrame(
        [("INV001", "A100", 5), ("INV002", None, 3), ("INV003", "A102", -1)],
        ["invoice_id", "stock_code", "quantity"],
    )
    dq_rules = [
        {"type": "not_null", "column": "stock_code"},
        {"type": "non_negative", "column": "quantity"},
    ]
    result = validate(df, dq_rules, table_name="test_sales")
    assert result.valid_count == 1
    assert result.invalid_count == 2
    assert result.valid_df.collect()[0]["invoice_id"] == "INV001"


def test_validate_captures_violation_reasons(spark):
    df = spark.createDataFrame([("INV001", None)], ["invoice_id", "stock_code"])
    dq_rules = [{"type": "not_null", "column": "stock_code"}]
    result = validate(df, dq_rules, table_name="test_sales")
    assert len(result.violations) == 1
    assert result.violations[0].rule_name == "not_null"
    assert result.violations[0].column == "stock_code"


def test_validate_with_no_rules_passes_everything(spark):
    df = spark.createDataFrame([("INV001",)], ["invoice_id"])
    result = validate(df, dq_rules=[], table_name="test_sales")
    assert result.valid_count == 1
    assert result.invalid_count == 0


def test_unknown_rule_type_raises_value_error():
    with pytest.raises(ValueError, match="Unknown data quality rule type"):
        _build_rule_check({"type": "not_a_real_rule", "column": "x"})


def test_allowed_values_rule_rejects_out_of_set_values(spark):
    df = spark.createDataFrame(
        [("row1", "United Kingdom"), ("row2", "Atlantis")], ["id", "country"]
    )
    dq_rules = [{"type": "allowed_values", "column": "country", "allowed": ["United Kingdom", "France"]}]
    result = validate(df, dq_rules, table_name="test_sales")
    assert result.valid_count == 1
    assert result.invalid_count == 1


def test_pass_rate_calculation(spark):
    df = spark.createDataFrame([(1,), (2,), (3,), (4,)], ["x"])
    dq_rules = [{"type": "non_negative", "column": "x"}]
    result = validate(df, dq_rules, table_name="test")
    assert result.pass_rate == 100.0


def test_validate_handles_empty_dataframe_without_error(spark):
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    schema = StructType([
        StructField("invoice_id", StringType()),
        StructField("quantity", IntegerType()),
    ])
    empty_df = spark.createDataFrame([], schema)
    dq_rules = [{"type": "not_null", "column": "invoice_id"}]
    result = validate(empty_df, dq_rules, table_name="test_empty")
    assert result.valid_count == 0
    assert result.invalid_count == 0
    assert result.pass_rate == 100.0


def test_validate_flags_entirely_null_column_as_zero_pass_rate(spark):
    df = spark.createDataFrame(
        [("INV001", None), ("INV002", None), ("INV003", None)],
        ["invoice_id", "customer_id"],
    )
    dq_rules = [{"type": "not_null", "column": "customer_id"}]
    result = validate(df, dq_rules, table_name="test_all_null")
    assert result.invalid_count == 3
    assert result.valid_count == 0
    assert result.pass_rate == 0.0
