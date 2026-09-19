# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: payments
# MAGIC Splits `payment_date` into a date and a time-of-day column, decodes the
# MAGIC numeric `payment_status` code into a descriptive value, and upserts into
# MAGIC `retailco.silver.payments` with `MERGE INTO` keyed on `payment_id`.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "retailco", "Catalog name")
catalog = dbutils.widgets.get("catalog")
target_table = f"{catalog}.silver.payments"

PAYMENT_STATUS_MAP = {"1": "Success", "2": "Pending", "3": "Cancelled", "4": "Failed"}

# COMMAND ----------

bronze = spark.table(f"{catalog}.bronze.payments")
status_map = F.create_map([F.lit(x) for pair in PAYMENT_STATUS_MAP.items() for x in pair])

updates = bronze.select(
    "payment_id",
    "customer_id",
    F.to_date("payment_date").alias("payment_date"),
    F.date_format("payment_date", "HH:mm:ss").alias("payment_time"),
    status_map[F.col("payment_status").cast("string")].alias("payment_status"),
    "payment_method",
)

# COMMAND ----------

if spark.catalog.tableExists(target_table):
    (
        DeltaTable.forName(spark, target_table)
        .alias("target")
        .merge(updates.alias("updates"), "target.payment_id = updates.payment_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    updates.write.format("delta").option("mergeSchema", "true").saveAsTable(target_table)

# COMMAND ----------

display(spark.table(target_table))
