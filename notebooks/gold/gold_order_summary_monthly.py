# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: order_summary_monthly
# MAGIC Thin runner — logic lives in `src/retailco_lakehouse`. Total orders, items, and
# MAGIC revenue per customer per month, excluding cancelled/pending orders.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../../src"))

from retailco_lakehouse.config import load_config  # noqa: E402
from retailco_lakehouse.transform.gold import build_monthly_order_summary  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("catalog", "retailco", "Catalog name")
config = load_config(dbutils)

orders = spark.table(config.table("silver", "orders"))
result = build_monthly_order_summary(orders)

# COMMAND ----------

# Gold is a cheap, fully-derived recompute — no incremental state to merge.
result.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(
    config.table("gold", "order_summary_monthly")
)

# COMMAND ----------

display(spark.table(config.table("gold", "order_summary_monthly")))
