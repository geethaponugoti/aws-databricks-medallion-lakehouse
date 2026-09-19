# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: addresses
# MAGIC Bronze has one row per (customer, address type). Pivoted here to one row per
# MAGIC customer with separate shipping/billing columns, then upserted into
# MAGIC `retailco.silver.addresses` with `MERGE INTO` keyed on `customer_id`.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "retailco", "Catalog name")
catalog = dbutils.widgets.get("catalog")
target_table = f"{catalog}.silver.addresses"

# COMMAND ----------

bronze = spark.table(f"{catalog}.bronze.addresses")

updates = (
    bronze.groupBy("customer_id")
    .pivot("address_type", ["shipping", "billing"])
    .agg(
        F.max("address_line_1").alias("address_line_1"),
        F.max("city").alias("city"),
        F.max("state").alias("state"),
        F.max("postcode").alias("postcode"),
    )
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
