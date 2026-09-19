# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: refunds
# MAGIC Splits `refund_timestamp` into date/time columns and `refund_reason`
# MAGIC (`"<reason>:<source>"`) into `refund_reason` / `refund_source`, then upserts
# MAGIC into `retailco.silver.refunds` with `MERGE INTO` keyed on `refund_id`.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "retailco", "Catalog name")
catalog = dbutils.widgets.get("catalog")
target_table = f"{catalog}.silver.refunds"

# COMMAND ----------

bronze = spark.table(f"{catalog}.bronze.refunds")

updates = bronze.select(
    "refund_id",
    "payment_id",
    F.to_date("refund_timestamp").alias("refund_date"),
    F.date_format("refund_timestamp", "HH:mm:ss").alias("refund_time"),
    "refund_amount",
    F.split(F.col("refund_reason"), ":").getItem(0).alias("refund_reason"),
    F.split(F.col("refund_reason"), ":").getItem(1).alias("refund_source"),
)

# COMMAND ----------

if spark.catalog.tableExists(target_table):
    (
        DeltaTable.forName(spark, target_table)
        .alias("target")
        .merge(updates.alias("updates"), "target.refund_id = updates.refund_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    updates.write.format("delta").option("mergeSchema", "true").saveAsTable(target_table)

# COMMAND ----------

display(spark.table(target_table))
