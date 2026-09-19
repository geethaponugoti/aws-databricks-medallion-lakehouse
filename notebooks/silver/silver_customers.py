# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: customers
# MAGIC Thin runner — logic lives in `src/retailco_lakehouse`. Dedupes, types, and
# MAGIC upserts Bronze customers into `retailco.silver.customers`.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../../src"))

from retailco_lakehouse.config import load_config  # noqa: E402
from retailco_lakehouse.delta_merge import merge_into  # noqa: E402
from retailco_lakehouse.transform.customers import MERGE_KEYS, clean_customers  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("catalog", "retailco", "Catalog name")
config = load_config(dbutils)

bronze = spark.table(config.table("bronze", "customers"))
updates = clean_customers(bronze)

# COMMAND ----------

merge_into(spark, config.table("silver", "customers"), updates, MERGE_KEYS)

# COMMAND ----------

display(spark.table(config.table("silver", "customers")))
