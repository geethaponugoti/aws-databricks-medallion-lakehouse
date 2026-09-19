# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: refunds
# MAGIC Thin runner — logic lives in `src/retailco_lakehouse`. Loads new files from the
# MAGIC external refunds CSV extract into `retailco.bronze.refunds` via `COPY INTO`.
# MAGIC For a local/demo run without a real export, run
# MAGIC `notebooks/setup/seed_sample_refunds_data.py` first.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../../src"))

from retailco_lakehouse.config import load_config  # noqa: E402
from retailco_lakehouse.copy_into import copy_into  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("catalog", "retailco", "Catalog name")
dbutils.widgets.text("bucket_name", "", "S3 bucket name")
config = load_config(dbutils)
assert config.bucket_name, "Set the bucket_name widget/parameter before running this notebook."

target_table = config.table("bronze", "refunds")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {target_table} (
    refund_id INT,
    payment_id INT,
    refund_timestamp TIMESTAMP,
    refund_amount DECIMAL(10, 2),
    refund_reason STRING
)
USING DELTA
""")

# COMMAND ----------

copy_into(spark, target_table, config.external_path("refunds"))

# COMMAND ----------

display(spark.table(target_table))
