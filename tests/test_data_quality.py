from datetime import datetime, timezone

from retailco_lakehouse.data_quality import (
    check_no_duplicates,
    check_not_null,
    check_row_count_reconciliation,
    check_valid_values,
    results_to_dataframe,
)


def test_check_not_null_passes_when_no_nulls(spark):
    df = spark.createDataFrame([(1,), (2,)], schema="id long")

    result = check_not_null(df, "id", layer="silver", table_name="customers")

    assert result.passed
    assert result.row_count == 0


def test_check_not_null_fails_and_counts_nulls(spark):
    df = spark.createDataFrame([(1,), (None,), (None,)], schema="id long")

    result = check_not_null(df, "id", layer="silver", table_name="customers")

    assert not result.passed
    assert result.row_count == 2


def test_check_no_duplicates_detects_repeated_keys(spark):
    df = spark.createDataFrame([(1,), (1,), (2,)], schema="id long")

    result = check_no_duplicates(df, ["id"], layer="silver", table_name="customers")

    assert not result.passed
    assert result.row_count == 1  # one distinct key (id=1) has duplicates


def test_check_row_count_reconciliation_passes_when_silver_shrinks(spark):
    upstream = spark.createDataFrame([(1,), (2,), (3,)], schema="id long")
    downstream = spark.createDataFrame([(1,), (2,)], schema="id long")

    result = check_row_count_reconciliation(upstream, downstream, layer="silver", table_name="customers")

    assert result.passed
    assert result.row_count == 1


def test_check_row_count_reconciliation_fails_when_silver_grows(spark):
    upstream = spark.createDataFrame([(1,)], schema="id long")
    downstream = spark.createDataFrame([(1,), (2,)], schema="id long")

    result = check_row_count_reconciliation(upstream, downstream, layer="silver", table_name="customers")

    assert not result.passed


def test_check_valid_values_flags_unexpected_values(spark):
    df = spark.createDataFrame([("Success",), ("Bogus",), (None,)], schema="status string")

    result = check_valid_values(
        df, "status", ["Success", "Pending", "Cancelled", "Failed"], layer="silver", table_name="payments"
    )

    assert not result.passed
    assert result.row_count == 1  # nulls aren't counted as invalid


def test_results_to_dataframe_has_the_expected_shape(spark):
    result = check_not_null(
        spark.createDataFrame([(1,)], schema="id long"), "id", layer="silver", table_name="customers"
    )

    df = results_to_dataframe(spark, [result], run_timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc))
    row = df.collect()[0]

    assert row["check_name"] == "not_null"
    assert row["layer"] == "silver"
    assert row["table_name"] == "customers"
    assert row["status"] == "PASS"
    assert row["row_count"] == 0
