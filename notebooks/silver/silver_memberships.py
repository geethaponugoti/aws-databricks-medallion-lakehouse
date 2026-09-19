# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: memberships
# MAGIC Thin runner — logic lives in `src/retailco_lakehouse`. Extracts `customer_id`
# MAGIC from the membership card filename and upserts into
# MAGIC `retailco.silver.memberships`.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../../src"))

from retailco_lakehouse.config import load_config  # noqa: E402
from retailco_lakehouse.delta_merge import merge_into  # noqa: E402
from retailco_lakehouse.transform.memberships import MERGE_KEYS, clean_memberships  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("catalog", "retailco", "Catalog name")
config = load_config(dbutils)

bronze = spark.table(config.table("bronze", "memberships"))
updates = clean_memberships(bronze)

# COMMAND ----------

merge_into(spark, config.table("silver", "memberships"), updates, MERGE_KEYS)

# COMMAND ----------

display(spark.table(config.table("silver", "memberships")))
