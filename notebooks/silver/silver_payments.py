# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: payments
# MAGIC Splits `payment_date` into a date and a time-of-day column, and decodes the
# MAGIC numeric `payment_status` code into a descriptive value.

# COMMAND ----------

dbutils.widgets.text("catalog", "retailco", "Catalog name")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.silver.payments
AS
SELECT
  payment_id,
  customer_id,
  CAST(date_format(payment_date, 'yyyy-MM-dd') AS DATE) AS payment_date,
  date_format(payment_date, 'HH:mm:ss') AS payment_time,
  CASE payment_status
    WHEN '1' THEN 'Success'
    WHEN '2' THEN 'Pending'
    WHEN '3' THEN 'Cancelled'
    WHEN '4' THEN 'Failed'
  END AS payment_status,
  payment_method
FROM {catalog}.bronze.payments
""")

display(spark.table(f"{catalog}.silver.payments"))
