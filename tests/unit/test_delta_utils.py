from src.common.delta_utils import merge_upsert, scd2_merge


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

    incoming = spark.createDataFrame([("INV001", 99), ("INV002", 3)], ["invoice_id", "quantity"])
    merge_upsert(spark, incoming, target, business_key=["invoice_id"])

    result = spark.read.format("delta").load(target)
    assert result.count() == 2
    assert result.filter("invoice_id = 'INV001'").collect()[0]["quantity"] == 99


def test_merge_upsert_cross_batch_correction_does_not_duplicate(spark, tmp_path):
    target = str(tmp_path / "fact_edge_case")
    day1 = spark.createDataFrame([("INV001", 5)], ["invoice_id", "quantity"])
    merge_upsert(spark, day1, target, business_key=["invoice_id"])

    day2_correction = spark.createDataFrame([("INV001", 8)], ["invoice_id", "quantity"])
    merge_upsert(spark, day2_correction, target, business_key=["invoice_id"])

    result = spark.read.format("delta").load(target)
    assert result.count() == 1
    assert result.collect()[0]["quantity"] == 8


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

    changed = spark.createDataFrame([("C001", "France")], ["customer_id", "country"])
    scd2_merge(spark, changed, target, business_key=["customer_id"], tracked_columns=["country"], surrogate_key_col="customer_sk")

    result = spark.read.format("delta").load(target)
    assert result.count() == 2

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

    same = spark.createDataFrame([("C001", "UK")], ["customer_id", "country"])
    scd2_merge(spark, same, target, business_key=["customer_id"], tracked_columns=["country"], surrogate_key_col="customer_sk")

    result = spark.read.format("delta").load(target)
    assert result.count() == 1


def test_scd2_empty_incoming_batch_against_existing_target_is_safe_noop(spark, tmp_path):
    target = str(tmp_path / "dim_edge_empty")
    initial = spark.createDataFrame([("C001", "UK")], ["customer_id", "country"])
    scd2_merge(spark, initial, target, business_key=["customer_id"], tracked_columns=["country"], surrogate_key_col="customer_sk")

    empty = spark.createDataFrame([], initial.schema)
    scd2_merge(spark, empty, target, business_key=["customer_id"], tracked_columns=["country"], surrogate_key_col="customer_sk")

    result = spark.read.format("delta").load(target)
    assert result.count() == 1
    assert result.filter("is_current = true").count() == 1


def test_scd2_mixed_new_changed_unchanged_in_same_run(spark, tmp_path):
    target = str(tmp_path / "dim_edge_mixed")
    initial = spark.createDataFrame([("C001", "UK"), ("C002", "France")], ["customer_id", "country"])
    scd2_merge(spark, initial, target, business_key=["customer_id"], tracked_columns=["country"], surrogate_key_col="customer_sk")

    mixed = spark.createDataFrame(
        [("C001", "Germany"), ("C002", "France"), ("C003", "Spain")],
        ["customer_id", "country"],
    )
    scd2_merge(spark, mixed, target, business_key=["customer_id"], tracked_columns=["country"], surrogate_key_col="customer_sk")

    result = spark.read.format("delta").load(target)
    assert result.count() == 4
    assert result.filter("customer_id = 'C001'").count() == 2
    assert result.filter("customer_id = 'C002'").count() == 1
    assert result.filter("customer_id = 'C003'").count() == 1
