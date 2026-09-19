# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: payments
# MAGIC Thin runner — logic lives in `src/retailco_lakehouse`. Splits `payment_date`,
# MAGIC decodes `payment_status`, and upserts into `retailco.silver.payments`.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../../src"))

from retailco_lakehouse.config import load_config  # noqa: E402
from retailco_lakehouse.delta_merge import merge_into  # noqa: E402
from retailco_lakehouse.transform.payments import MERGE_KEYS, clean_payments  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("catalog", "retailco", "Catalog name")
config = load_config(dbutils)

bronze = spark.table(config.table("bronze", "payments"))
updates = clean_payments(bronze)

# COMMAND ----------

merge_into(spark, config.table("silver", "payments"), updates, MERGE_KEYS)

# COMMAND ----------

display(spark.table(config.table("silver", "payments")))
