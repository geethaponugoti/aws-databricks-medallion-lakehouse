# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: addresses
# MAGIC Tab-delimited CSV export with one row per (customer, address type), ingested
# MAGIC incrementally via Auto Loader.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "retailco", "Catalog name")
dbutils.widgets.text("bucket_name", "", "S3 bucket name")
catalog = dbutils.widgets.get("catalog")
bucket_name = dbutils.widgets.get("bucket_name")
assert bucket_name, "Set the bucket_name widget/parameter before running this notebook."

source_path = f"/Volumes/{catalog}/landing/operational_data/addresses"
checkpoint_path = f"s3://{bucket_name}/_checkpoints/bronze/addresses"
schema_tracking_path = f"s3://{bucket_name}/_schemas/bronze/addresses"
target_table = f"{catalog}.bronze.addresses"

# COMMAND ----------

raw_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", schema_tracking_path)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("header", "true")
    .option("delimiter", "\t")
    .load(source_path)
    .withColumn("file_path", F.col("_metadata.file_path"))
    .withColumn("ingested_at", F.current_timestamp())
)

query = (
    raw_stream.writeStream.format("delta")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(target_table)
)
query.awaitTermination()

# COMMAND ----------

display(spark.table(target_table))
