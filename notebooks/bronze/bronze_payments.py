# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: payments
# MAGIC Thin runner — logic lives in `src/retailco_lakehouse`. Loads new files from the
# MAGIC external payments CSV extract into `retailco.bronze.payments` via `COPY INTO`.

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

target_table = config.table("bronze", "payments")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {target_table} (
    payment_id INT,
    customer_id INT,
    payment_date TIMESTAMP,
    payment_status STRING,
    payment_method STRING
)
USING DELTA
""")

# COMMAND ----------

copy_into(spark, target_table, config.external_path("payments"))

# COMMAND ----------

display(spark.table(target_table))
