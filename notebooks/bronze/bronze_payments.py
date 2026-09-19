# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: payments
# MAGIC Payments are extracted from the billing system as CSV and dropped in
# MAGIC `external_data/payments/`. `COPY INTO` loads only files it hasn't seen before
# MAGIC into a managed Delta table, replacing the original approach of declaring an
# MAGIC external table straight over the S3 location (which had to `REFRESH` its
# MAGIC metadata and re-scanned every file on every query).
# MAGIC
# MAGIC The original version of this table had `delimeter=','` in its options — not a
# MAGIC recognized Spark CSV option, so it silently fell back to the default comma
# MAGIC delimiter. Fixed here to the correct `delimiter` option.

# COMMAND ----------

dbutils.widgets.text("catalog", "retailco", "Catalog name")
dbutils.widgets.text("bucket_name", "", "S3 bucket name")
catalog = dbutils.widgets.get("catalog")
bucket_name = dbutils.widgets.get("bucket_name")
assert bucket_name, "Set the bucket_name widget/parameter before running this notebook."

target_table = f"{catalog}.bronze.payments"
source_path = f"s3://{bucket_name}/external_data/payments/"

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {target_table} (
    payment_id INT,
    customer_id INT,
    payment_date TIMESTAMP,
    payment_status STRING,
    payment_method STRING
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
