"""Tests for the generic Delta upsert helper.

Uses the delta_spark fixture (a SparkSession with Delta's SQL extensions and
its own temporary warehouse) since MERGE INTO needs a real Delta table.
"""

import uuid

import pytest

from retailco_lakehouse.delta_merge import merge_into


@pytest.fixture
def target_table_name():
    # Unique per test: delta_spark is session-scoped, so a fixed name would
    # leak state (and tables) between tests.
    return f"default.test_merge_into_{uuid.uuid4().hex}"


def test_merge_into_creates_the_table_on_first_run(delta_spark, target_table_name):
    updates = delta_spark.createDataFrame([(1, "Ada")], schema="customer_id long, customer_name string")

    merge_into(delta_spark, target_table_name, updates, ["customer_id"])

    result = delta_spark.table(target_table_name).collect()
    assert len(result) == 1
    assert result[0]["customer_name"] == "Ada"


def test_merge_into_upserts_matched_and_unmatched_rows(delta_spark, target_table_name):
    initial = delta_spark.createDataFrame(
        [(1, "Ada Old Name"), (2, "Grace")], schema="customer_id long, customer_name string"
    )
    merge_into(delta_spark, target_table_name, initial, ["customer_id"])

    updates = delta_spark.createDataFrame(
        [(1, "Ada Updated"), (3, "Margaret")], schema="customer_id long, customer_name string"
    )
    merge_into(delta_spark, target_table_name, updates, ["customer_id"])

    result = {r["customer_id"]: r["customer_name"] for r in delta_spark.table(target_table_name).collect()}
    assert result == {1: "Ada Updated", 2: "Grace", 3: "Margaret"}


def test_merge_into_is_idempotent(delta_spark, target_table_name):
    updates = delta_spark.createDataFrame([(1, "Ada")], schema="customer_id long, customer_name string")

    merge_into(delta_spark, target_table_name, updates, ["customer_id"])
    merge_into(delta_spark, target_table_name, updates, ["customer_id"])

    assert delta_spark.table(target_table_name).count() == 1


def test_merge_into_requires_at_least_one_merge_key(delta_spark, target_table_name):
    updates = delta_spark.createDataFrame([(1, "Ada")], schema="customer_id long, customer_name string")

    with pytest.raises(ValueError):
        merge_into(delta_spark, target_table_name, updates, [])
