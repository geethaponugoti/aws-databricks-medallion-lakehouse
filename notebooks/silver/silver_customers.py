# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: customers
# MAGIC Drops rows with a null `customer_id`, deduplicates to the latest
# MAGIC `created_timestamp` per customer, and casts columns to their proper types.
# MAGIC
# MAGIC Original version used bare `CREATE TABLE ... AS`, which fails on a second run.
# MAGIC Changed to `CREATE OR REPLACE TABLE` here so the notebook is re-runnable.

# COMMAND ----------

dbutils.widgets.text("catalog", "retailco", "Catalog name")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.silver.customers
AS
WITH latest AS (
  SELECT customer_id, MAX(created_timestamp) AS created_timestamp
  FROM {catalog}.bronze.v_customers
  WHERE customer_id IS NOT NULL
  GROUP BY customer_id
)
SELECT DISTINCT
  CAST(b.created_timestamp AS TIMESTAMP) AS created_timestamp,
  b.customer_id,
  b.customer_name,
  CAST(b.date_of_birth AS DATE) AS date_of_birth,
  b.email,
  CAST(b.member_since AS DATE) AS member_since,
  b.telephone
FROM {catalog}.bronze.v_customers b
JOIN latest l
  ON b.customer_id = l.customer_id AND b.created_timestamp = l.created_timestamp
""")

display(spark.table(f"{catalog}.silver.customers"))
