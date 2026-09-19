# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: customers
# MAGIC Incrementally ingests customer JSON files from the landing volume into
# MAGIC `retailco.bronze.customers` using Auto Loader. Auto Loader's checkpoint tracks
# MAGIC which files have already been processed, so re-running this notebook never
# MAGIC reprocesses or duplicates a file — a `CREATE OR REPLACE VIEW` over the raw path
# MAGIC (the original approach) re-reads everything, every time. New source columns are
# MAGIC picked up automatically (`cloudFiles.schemaEvolutionMode = addNewColumns`)
# MAGIC instead of silently breaking the read.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "retailco", "Catalog name")
dbutils.widgets.text("bucket_name", "", "S3 bucket name")
catalog = dbutils.widgets.get("catalog")
bucket_name = dbutils.widgets.get("bucket_name")
assert bucket_name, "Set the bucket_name widget/parameter before running this notebook."

source_path = f"/Volumes/{catalog}/landing/operational_data/customers"
checkpoint_path = f"s3://{bucket_name}/_checkpoints/bronze/customers"
schema_tracking_path = f"s3://{bucket_name}/_schemas/bronze/customers"
target_table = f"{catalog}.bronze.customers"

# COMMAND ----------

raw_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", schema_tracking_path)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
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
