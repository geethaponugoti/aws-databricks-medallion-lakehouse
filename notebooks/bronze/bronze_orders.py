# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: orders
# MAGIC The raw orders export is newline-delimited JSON with one malformed field
# MAGIC (`order_date` isn't quoted), which breaks strict JSON parsing — so Bronze reads
# MAGIC it as `text` via Auto Loader and the fix-up/parsing happens in Silver. Auto
# MAGIC Loader's checkpoint makes re-running this notebook safe: already-ingested files
# MAGIC are never reprocessed.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "retailco", "Catalog name")
dbutils.widgets.text("bucket_name", "", "S3 bucket name")
catalog = dbutils.widgets.get("catalog")
bucket_name = dbutils.widgets.get("bucket_name")
assert bucket_name, "Set the bucket_name widget/parameter before running this notebook."

source_path = f"/Volumes/{catalog}/landing/operational_data/orders"
checkpoint_path = f"s3://{bucket_name}/_checkpoints/bronze/orders"
schema_tracking_path = f"s3://{bucket_name}/_schemas/bronze/orders"
target_table = f"{catalog}.bronze.orders"

# COMMAND ----------

raw_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "text")
    .option("cloudFiles.schemaLocation", schema_tracking_path)
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
