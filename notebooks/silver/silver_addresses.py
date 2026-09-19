# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: addresses
# MAGIC Thin runner — logic lives in `src/retailco_lakehouse`. Pivots shipping/billing
# MAGIC rows to one row per customer and upserts into `retailco.silver.addresses`.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../../src"))

from retailco_lakehouse.config import load_config  # noqa: E402
from retailco_lakehouse.delta_merge import merge_into  # noqa: E402
from retailco_lakehouse.transform.addresses import MERGE_KEYS, clean_addresses  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("catalog", "retailco", "Catalog name")
config = load_config(dbutils)

bronze = spark.table(config.table("bronze", "addresses"))
updates = clean_addresses(bronze)

# COMMAND ----------

merge_into(spark, config.table("silver", "addresses"), updates, MERGE_KEYS)

# COMMAND ----------

display(spark.table(config.table("silver", "addresses")))
