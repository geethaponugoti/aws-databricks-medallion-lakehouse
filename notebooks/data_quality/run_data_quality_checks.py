# Databricks notebook source
# MAGIC %md
# MAGIC # Data quality checks
# MAGIC Thin runner — logic lives in `src/retailco_lakehouse/data_quality.py`. Runs
# MAGIC after Silver/Gold in every pipeline run: null-key, duplicate-key, Bronze ->
# MAGIC Silver row-count reconciliation, and valid-value checks, logged to
# MAGIC `retailco.audit.dq_results`. Fails the task (and therefore the Workflow run)
# MAGIC if anything fails, so a bad run doesn't propagate silently.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../../src"))

from pyspark.sql import functions as F  # noqa: E402

from retailco_lakehouse.config import load_config  # noqa: E402
from retailco_lakehouse.data_quality import (  # noqa: E402
    check_no_duplicates,
    check_not_null,
    check_row_count_reconciliation,
    check_valid_values,
    write_results,
)

# COMMAND ----------

dbutils.widgets.text("catalog", "retailco", "Catalog name")
config = load_config(dbutils)

customers = spark.table(config.table("silver", "customers"))
payments = spark.table(config.table("silver", "payments"))
refunds = spark.table(config.table("silver", "refunds"))
addresses = spark.table(config.table("silver", "addresses"))
orders = spark.table(config.table("silver", "orders"))

bronze_customers = spark.table(config.table("bronze", "customers")).filter(F.col("customer_id").isNotNull())
bronze_payments = spark.table(config.table("bronze", "payments"))
bronze_refunds = spark.table(config.table("bronze", "refunds"))

# COMMAND ----------

results = [
    check_not_null(customers, "customer_id", layer="silver", table_name="customers"),
    check_not_null(payments, "payment_id", layer="silver", table_name="payments"),
    check_not_null(refunds, "refund_id", layer="silver", table_name="refunds"),
    check_not_null(addresses, "customer_id", layer="silver", table_name="addresses"),
    check_not_null(orders, "order_id", layer="silver", table_name="orders"),
    check_no_duplicates(customers, ["customer_id"], layer="silver", table_name="customers"),
    check_no_duplicates(payments, ["payment_id"], layer="silver", table_name="payments"),
    check_no_duplicates(refunds, ["refund_id"], layer="silver", table_name="refunds"),
    check_no_duplicates(addresses, ["customer_id"], layer="silver", table_name="addresses"),
    check_no_duplicates(orders, ["order_id", "item_id"], layer="silver", table_name="orders"),
    check_row_count_reconciliation(bronze_customers, customers, layer="silver", table_name="customers"),
    check_row_count_reconciliation(bronze_payments, payments, layer="silver", table_name="payments"),
    check_row_count_reconciliation(bronze_refunds, refunds, layer="silver", table_name="refunds"),
    check_valid_values(
        payments, "payment_status", ["Success", "Pending", "Cancelled", "Failed"],
        layer="silver", table_name="payments",
    ),
    check_not_null(orders, "order_status", layer="silver", table_name="orders"),
    check_not_null(refunds, "refund_source", layer="silver", table_name="refunds"),
]

# COMMAND ----------

write_results(spark, results, config.table("audit", "dq_results"))
display(spark.table(config.table("audit", "dq_results")))

# COMMAND ----------

failed = [r for r in results if not r.passed]
if failed:
    raise AssertionError(f"{len(failed)} data quality check(s) failed: {failed}")
