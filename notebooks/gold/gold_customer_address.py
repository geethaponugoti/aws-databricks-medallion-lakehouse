# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: customer_address
# MAGIC Customer profile joined with their shipping/billing address — a single
# MAGIC customer-360 table for BI and downstream consumers.

# COMMAND ----------

dbutils.widgets.text("catalog", "retailco", "Catalog name")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.gold.customer_address
AS
SELECT
  c.customer_id,
  c.customer_name,
  c.date_of_birth,
  c.email,
  c.telephone,
  a.shipping_address_line_1,
  a.shipping_city,
  a.shipping_state,
  a.shipping_postcode,
  a.billing_address_line_1,
  a.billing_city,
  a.billing_state,
  a.billing_postcode
FROM {catalog}.silver.customers c
JOIN {catalog}.silver.addresses a ON c.customer_id = a.customer_id
""")

display(spark.table(f"{catalog}.gold.customer_address"))
