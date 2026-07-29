import pytest

from src.ingestion.bronze_loader import _build_schema, _tag_metadata
from src.common.exception_handler import SchemaValidationError


def test_build_schema_creates_correct_struct_type():
    schema_config = [
        {"name": "invoice_id", "type": "string"},
        {"name": "quantity", "type": "integer"},
    ]
    schema = _build_schema(schema_config)
    assert schema.fieldNames() == ["invoice_id", "quantity"]
    assert schema["quantity"].dataType.typeName() == "integer"


def test_build_schema_raises_on_unsupported_type():
    with pytest.raises(SchemaValidationError, match="Unsupported schema type"):
        _build_schema([{"name": "bad_col", "type": "not_a_real_type"}])


def test_tag_metadata_adds_all_required_columns(spark):
    df = spark.createDataFrame([("INV001",)], ["invoice_id"])
    tagged = _tag_metadata(df, source_path="data/landing/sales/file.csv", batch_id="batch-123")

    expected_metadata_cols = {"_source_file", "_ingest_ts", "_batch_id", "_ingest_date"}
    assert expected_metadata_cols.issubset(set(tagged.columns))

    row = tagged.collect()[0]
    assert row["_source_file"] == "data/landing/sales/file.csv"
    assert row["_batch_id"] == "batch-123"


def test_tag_metadata_preserves_original_columns_and_data(spark):
    df = spark.createDataFrame([("INV001", 5)], ["invoice_id", "quantity"])
    tagged = _tag_metadata(df, source_path="x", batch_id="y")
    row = tagged.collect()[0]
    assert row["invoice_id"] == "INV001"
    assert row["quantity"] == 5
