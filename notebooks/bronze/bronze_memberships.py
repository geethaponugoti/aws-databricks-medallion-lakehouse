# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: memberships
# MAGIC Membership card images, one PNG per customer, ingested via Auto Loader's
# MAGIC `binaryFile` format. `customer_id` is embedded in the filename and extracted
# MAGIC in Silver.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "retailco", "Catalog name")
dbutils.widgets.text("bucket_name", "", "S3 bucket name")
catalog = dbutils.widgets.get("catalog")
bucket_name = dbutils.widgets.get("bucket_name")
assert bucket_name, "Set the bucket_name widget/parameter before running this notebook."

source_path = f"/Volumes/{catalog}/landing/operational_data/memberships/*/*.png"
checkpoint_path = f"s3://{bucket_name}/_checkpoints/bronze/memberships"
schema_tracking_path = f"s3://{bucket_name}/_schemas/bronze/memberships"
target_table = f"{catalog}.bronze.memberships"

# COMMAND ----------

raw_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "binaryFile")
    .option("cloudFiles.schemaLocation", schema_tracking_path)
    .load(source_path)
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
