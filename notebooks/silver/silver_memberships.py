# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: memberships
# MAGIC Extracts `customer_id` from the membership card filename (`.../<customer_id>.png`).

# COMMAND ----------

dbutils.widgets.text("catalog", "retailco", "Catalog name")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.silver.memberships
AS
SELECT
  regexp_extract(path, '.*/([0-9]+)\\\\.png$', 1) AS customer_id,
  content AS membership_card
FROM {catalog}.bronze.v_memberships
""")

display(spark.table(f"{catalog}.silver.memberships"))
