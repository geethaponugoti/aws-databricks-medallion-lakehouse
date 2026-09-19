# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: memberships
# MAGIC Extracts `customer_id` from the membership card filename (`.../<customer_id>.png`)
# MAGIC and upserts into `retailco.silver.memberships` with `MERGE INTO` keyed on
# MAGIC `customer_id`.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "retailco", "Catalog name")
catalog = dbutils.widgets.get("catalog")
target_table = f"{catalog}.silver.memberships"

# COMMAND ----------

bronze = spark.table(f"{catalog}.bronze.memberships")

updates = bronze.select(
    F.regexp_extract("path", r".*/([0-9]+)\.png$", 1).alias("customer_id"),
    F.col("content").alias("membership_card"),
)

# COMMAND ----------

if spark.catalog.tableExists(target_table):
    (
        DeltaTable.forName(spark, target_table)
        .alias("target")
        .merge(updates.alias("updates"), "target.customer_id = updates.customer_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    updates.write.format("delta").option("mergeSchema", "true").saveAsTable(target_table)

# COMMAND ----------

display(spark.table(target_table))
