# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: refunds
# MAGIC `refund_reason` packs two values as `"<reason>:<source>"` (e.g.
# MAGIC `"Order Cancelled:Customer"`) — split out in Silver.
# MAGIC
# MAGIC The original project seeded this table with `INSERT` statements, since the
# MAGIC upstream billing export didn't include a refunds file yet. Converted here to
# MAGIC `COPY INTO` from `external_data/refunds/`, matching `bronze_payments` — the
# MAGIC same incremental, no-duplicate-loads pattern. For a local/demo run without a
# MAGIC real export, `notebooks/setup/seed_sample_refunds_data.py` writes the original
# MAGIC ten sample rows as a CSV to that path.

# COMMAND ----------

dbutils.widgets.text("catalog", "retailco", "Catalog name")
dbutils.widgets.text("bucket_name", "", "S3 bucket name")
catalog = dbutils.widgets.get("catalog")
bucket_name = dbutils.widgets.get("bucket_name")
assert bucket_name, "Set the bucket_name widget/parameter before running this notebook."

target_table = f"{catalog}.bronze.refunds"
source_path = f"s3://{bucket_name}/external_data/refunds/"

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {target_table} (
    refund_id INT,
    payment_id INT,
    refund_timestamp TIMESTAMP,
    refund_amount DECIMAL(10, 2),
    refund_reason STRING
)
USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
COPY INTO {target_table}
FROM '{source_path}'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'delimiter' = ',', 'inferSchema' = 'false')
COPY_OPTIONS ('mergeSchema' = 'true')
""")

# COMMAND ----------

display(spark.table(target_table))
