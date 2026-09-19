# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: customers
# MAGIC Drops rows with a null `customer_id`, deduplicates to the latest
# MAGIC `created_timestamp` per customer, casts columns to their proper types, and
# MAGIC upserts into `retailco.silver.customers` with `MERGE INTO`. Replaces the
# MAGIC original `CREATE TABLE ... AS SELECT` full rebuild — safe to re-run against an
# MAGIC incrementally-loaded Bronze table without reprocessing history as duplicates.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "retailco", "Catalog name")
catalog = dbutils.widgets.get("catalog")
target_table = f"{catalog}.silver.customers"

# COMMAND ----------

bronze = spark.table(f"{catalog}.bronze.customers").filter(F.col("customer_id").isNotNull())

latest_per_customer = Window.partitionBy("customer_id").orderBy(F.col("created_timestamp").desc())

updates = (
    bronze.withColumn("_rn", F.row_number().over(latest_per_customer))
    .filter(F.col("_rn") == 1)
    .select(
        F.col("customer_id").cast("long").alias("customer_id"),
        F.col("customer_name"),
        F.col("date_of_birth").cast("date").alias("date_of_birth"),
        F.col("email"),
        F.col("member_since").cast("date").alias("member_since"),
        F.col("telephone"),
        F.col("created_timestamp").cast("timestamp").alias("created_timestamp"),
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
