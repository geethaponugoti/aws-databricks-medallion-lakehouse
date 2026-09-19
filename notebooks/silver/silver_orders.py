# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: orders
# MAGIC Thin runner — logic lives in `src/retailco_lakehouse`. Parses the raw order
# MAGIC JSON text, explodes line items, and upserts into `retailco.silver.orders`
# MAGIC (grain: one row per order line item, keyed on `order_id` + `item_id`).

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../../src"))

from retailco_lakehouse.config import load_config  # noqa: E402
from retailco_lakehouse.delta_merge import merge_into  # noqa: E402
from retailco_lakehouse.transform.orders import MERGE_KEYS, clean_orders  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("catalog", "retailco", "Catalog name")
config = load_config(dbutils)

bronze = spark.table(config.table("bronze", "orders"))
updates = clean_orders(bronze)

# COMMAND ----------

merge_into(spark, config.table("silver", "orders"), updates, MERGE_KEYS)

# COMMAND ----------

display(spark.table(config.table("silver", "orders")))
