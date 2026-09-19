# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: refunds
# MAGIC Thin runner — logic lives in `src/retailco_lakehouse`. Splits `refund_timestamp`
# MAGIC and `refund_reason`, and upserts into `retailco.silver.refunds`.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../../src"))

from retailco_lakehouse.config import load_config  # noqa: E402
from retailco_lakehouse.delta_merge import merge_into  # noqa: E402
from retailco_lakehouse.transform.refunds import MERGE_KEYS, clean_refunds  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("catalog", "retailco", "Catalog name")
config = load_config(dbutils)

bronze = spark.table(config.table("bronze", "refunds"))
updates = clean_refunds(bronze)

# COMMAND ----------

merge_into(spark, config.table("silver", "refunds"), updates, MERGE_KEYS)

# COMMAND ----------

display(spark.table(config.table("silver", "refunds")))
