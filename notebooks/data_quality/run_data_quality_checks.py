# Databricks notebook source
# MAGIC %md
# MAGIC # Data quality checks
# MAGIC Runs after Silver/Gold in every pipeline run. Checks null keys, duplicate keys,
# MAGIC Bronze -> Silver row-count reconciliation, and valid-value enumerations, and logs
# MAGIC one row per check to `retailco.audit.dq_results` so a failing run is queryable
# MAGIC instead of just a red job in the Workflow UI.

# COMMAND ----------

from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

dbutils.widgets.text("catalog", "retailco", "Catalog name")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check helpers
# MAGIC Each returns `(status, row_count)` — `row_count` is the number of *offending*
# MAGIC rows for null/duplicate/valid-value checks, and the absolute Bronze/Silver
# MAGIC difference for reconciliation checks.

# COMMAND ----------

def check_not_null(df, column):
    bad = df.filter(F.col(column).isNull()).count()
    return ("PASS" if bad == 0 else "FAIL", bad)


def check_no_duplicates(df, key_columns):
    dup_groups = df.groupBy(*key_columns).count().filter(F.col("count") > 1).count()
    return ("PASS" if dup_groups == 0 else "FAIL", dup_groups)


def check_row_count_reconciliation(upstream_df, downstream_df):
    diff = abs(upstream_df.count() - downstream_df.count())
    # Silver dedups/filters Bronze, so it should never end up with *more* rows.
    status = "PASS" if downstream_df.count() <= upstream_df.count() else "FAIL"
    return (status, diff)


def check_valid_values(df, column, allowed_values):
    bad = (
        df.filter(F.col(column).isNotNull() & ~F.col(column).isin(list(allowed_values)))
        .count()
    )
    return ("PASS" if bad == 0 else "FAIL", bad)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run the checks

# COMMAND ----------

customers = spark.table(f"{catalog}.silver.customers")
payments = spark.table(f"{catalog}.silver.payments")
refunds = spark.table(f"{catalog}.silver.refunds")
addresses = spark.table(f"{catalog}.silver.addresses")
orders = spark.table(f"{catalog}.silver.orders")

bronze_customers = spark.table(f"{catalog}.bronze.v_customers").filter(F.col("customer_id").isNotNull())
bronze_payments = spark.table(f"{catalog}.bronze.payments")
bronze_refunds = spark.table(f"{catalog}.bronze.refunds")

results = []


def record(check_name, layer, table_name, outcome):
    status, row_count = outcome
    results.append((check_name, layer, table_name, status, int(row_count)))


# Null keys
record("null_key", "silver", "customers", check_not_null(customers, "customer_id"))
record("null_key", "silver", "payments", check_not_null(payments, "payment_id"))
record("null_key", "silver", "refunds", check_not_null(refunds, "refund_id"))
record("null_key", "silver", "addresses", check_not_null(addresses, "customer_id"))
record("null_key", "silver", "orders", check_not_null(orders, "order_id"))

# Duplicate keys
record("no_duplicates", "silver", "customers", check_no_duplicates(customers, ["customer_id"]))
record("no_duplicates", "silver", "payments", check_no_duplicates(payments, ["payment_id"]))
record("no_duplicates", "silver", "refunds", check_no_duplicates(refunds, ["refund_id"]))
record("no_duplicates", "silver", "addresses", check_no_duplicates(addresses, ["customer_id"]))
record("no_duplicates", "silver", "orders", check_no_duplicates(orders, ["order_id", "item_id"]))

# Bronze -> Silver row-count reconciliation
record(
    "row_count_reconciliation", "silver", "customers",
    check_row_count_reconciliation(bronze_customers, customers),
)
record(
    "row_count_reconciliation", "silver", "payments",
    check_row_count_reconciliation(bronze_payments, payments),
)
record(
    "row_count_reconciliation", "silver", "refunds",
    check_row_count_reconciliation(bronze_refunds, refunds),
)

# Valid values
record(
    "valid_values", "silver", "payments",
    check_valid_values(payments, "payment_status", ["Success", "Pending", "Cancelled", "Failed"]),
)
record("not_null", "silver", "orders", check_not_null(orders, "order_status"))
record("not_null", "silver", "refunds", check_not_null(refunds, "refund_source"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write results to the audit table

# COMMAND ----------

run_timestamp = datetime.now(timezone.utc)

schema = StructType(
    [
        StructField("check_name", StringType(), False),
        StructField("layer", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("status", StringType(), False),
        StructField("row_count", LongType(), False),
        StructField("run_timestamp", TimestampType(), False),
    ]
)

results_df = spark.createDataFrame(
    [(*row, run_timestamp) for row in results], schema=schema
)

(
    results_df.write.format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(f"{catalog}.audit.dq_results")
)

display(results_df)

# COMMAND ----------

failed = [r for r in results if r[3] == "FAIL"]
if failed:
    raise AssertionError(f"{len(failed)} data quality check(s) failed: {failed}")
