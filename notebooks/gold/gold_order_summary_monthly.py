# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: order_summary_monthly
# MAGIC Total orders, items, and revenue per customer per month. Excludes cancelled
# MAGIC and pending orders, since neither represents realized revenue.
# MAGIC
# MAGIC Fixed a typo carried through the original pipeline: the output column was
# MAGIC named `total_amnount`. Renamed to `total_amount` here.

# COMMAND ----------

dbutils.widgets.text("catalog", "retailco", "Catalog name")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.gold.order_summary_monthly
AS
SELECT
  customer_id,
  date_format(transaction_timestamp, 'yyyy-MM') AS transaction_month,
  COUNT(DISTINCT order_id) AS total_orders,
  SUM(quantity) AS total_items,
  SUM(quantity * price) AS total_amount
FROM {catalog}.silver.orders
WHERE order_status NOT IN ('Cancelled', 'Pending')
GROUP BY customer_id, date_format(transaction_timestamp, 'yyyy-MM')
""")

display(spark.table(f"{catalog}.gold.order_summary_monthly"))
