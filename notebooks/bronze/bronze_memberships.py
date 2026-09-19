# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: memberships
# MAGIC Thin runner — logic lives in `src/retailco_lakehouse`. Ingests membership card
# MAGIC PNGs via Auto Loader's `binaryFile` format into `retailco.bronze.memberships`.

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
    source_path=f"{config.landing_path('memberships')}/*/*.png",
    file_format="binaryFile",
    target_table=config.table("bronze", "memberships"),
    checkpoint_path=config.checkpoint_path("bronze", "memberships"),
    schema_tracking_path=config.schema_tracking_path("bronze", "memberships"),
    extra_columns={"ingested_at": F.current_timestamp()},
)

# COMMAND ----------

display(spark.table(config.table("bronze", "memberships")))
