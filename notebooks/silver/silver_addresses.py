# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: addresses
# MAGIC Bronze has one row per (customer, address type). Pivoted here to one row per
# MAGIC customer with separate shipping/billing columns.

# COMMAND ----------

dbutils.widgets.text("catalog", "retailco", "Catalog name")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.silver.addresses
AS
SELECT *
FROM (
  SELECT customer_id, address_type, address_line_1, city, state, postcode
  FROM {catalog}.bronze.v_addresses
)
PIVOT (
  MAX(address_line_1) AS address_line_1,
  MAX(city) AS city,
  MAX(state) AS state,
  MAX(postcode) AS postcode
  FOR address_type IN ('shipping', 'billing')
)
""")

display(spark.table(f"{catalog}.silver.addresses"))
