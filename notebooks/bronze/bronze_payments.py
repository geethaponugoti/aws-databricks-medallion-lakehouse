# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: payments
# MAGIC Payments are extracted from the billing system as CSV and dropped in
# MAGIC `external_data/payments/`, outside the landing volume. Declared as a typed
# MAGIC external table directly over that S3 location.
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

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.payments
(
    payment_id INT,
    customer_id INT,
    payment_date TIMESTAMP,
    payment_status STRING,
    payment_method STRING
)
USING CSV
OPTIONS (
    header = 'true',
    delimiter = ','
)
LOCATION 's3://{bucket_name}/external_data/payments/'
""")

# metadata refresh — picks up new files dropped at the external location
spark.sql(f"REFRESH TABLE {catalog}.bronze.payments")

display(spark.table(f"{catalog}.bronze.payments"))
