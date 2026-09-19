# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: customer_address
# MAGIC Thin runner — logic lives in `src/retailco_lakehouse`. Customer profile joined
# MAGIC with shipping/billing address — a single customer-360 table for BI.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../../src"))

from retailco_lakehouse.config import load_config  # noqa: E402
from retailco_lakehouse.transform.gold import build_customer_address  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("catalog", "retailco", "Catalog name")
config = load_config(dbutils)

customers = spark.table(config.table("silver", "customers"))
addresses = spark.table(config.table("silver", "addresses"))
result = build_customer_address(customers, addresses)

# COMMAND ----------

# Gold is a cheap, fully-derived recompute — no incremental state to merge.
result.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(
    config.table("gold", "customer_address")
)

# COMMAND ----------

display(spark.table(config.table("gold", "customer_address")))
