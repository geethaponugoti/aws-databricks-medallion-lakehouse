# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: customers
# MAGIC Thin runner — logic lives in `src/retailco_lakehouse`. Incrementally ingests
# MAGIC customer JSON files from the landing volume into `retailco.bronze.customers`
# MAGIC via Auto Loader; already-processed files are never reprocessed.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../../src"))

from pyspark.sql import functions as F  # noqa: E402

from retailco_lakehouse.autoloader import run_autoloader_to_table  # noqa: E402
from retailco_lakehouse.config import load_config  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("catalog", "retailco", "Catalog name")
dbutils.widgets.text("bucket_name", "", "S3 bucket name")
config = load_config(dbutils)
assert config.bucket_name, "Set the bucket_name widget/parameter before running this notebook."

# COMMAND ----------

run_autoloader_to_table(
    spark,
    source_path=config.landing_path("customers"),
    file_format="json",
    target_table=config.table("bronze", "customers"),
    checkpoint_path=config.checkpoint_path("bronze", "customers"),
    schema_tracking_path=config.schema_tracking_path("bronze", "customers"),
    extra_columns={
        "file_path": F.col("_metadata.file_path"),
        "ingested_at": F.current_timestamp(),
    },
)

# COMMAND ----------

display(spark.table(config.table("bronze", "customers")))
