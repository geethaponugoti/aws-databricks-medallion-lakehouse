# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: refunds
# MAGIC Splits `refund_timestamp` into date/time columns and `refund_reason`
# MAGIC (`"<reason>:<source>"`) into `refund_reason` / `refund_source`.

# COMMAND ----------

dbutils.widgets.text("catalog", "retailco", "Catalog name")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.silver.refunds
AS
SELECT
  refund_id,
  payment_id,
  CAST(date_format(refund_timestamp, 'yyyy-MM-dd') AS DATE) AS refund_date,
  date_format(refund_timestamp, 'HH:mm:ss') AS refund_time,
  refund_amount,
  SPLIT(refund_reason, ':')[0] AS refund_reason,
  SPLIT(refund_reason, ':')[1] AS refund_source
FROM {catalog}.bronze.refunds
""")

display(spark.table(f"{catalog}.silver.refunds"))
