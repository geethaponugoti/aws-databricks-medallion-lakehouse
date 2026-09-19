# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: orders
# MAGIC Combines what were originally two notebooks ("Transform Orders Data" and
# MAGIC "Transform Orders Data - Explode Arrays") into one Silver flow:
# MAGIC 1. Fix the one malformed field in the raw JSON text (`order_date` isn't quoted).
# MAGIC 2. Parse it into a struct with a known schema.
# MAGIC 3. Explode the `items` array to one row per line item and flatten it.
# MAGIC
# MAGIC The original version persisted step 2's output as its own table
# MAGIC (`silver.orders_json`) before exploding it in a second notebook. That
# MAGIC intermediate table added no value downstream, so it's collapsed into a single
# MAGIC temp view here — one less table to maintain and keep in sync.

# COMMAND ----------

dbutils.widgets.text("catalog", "retailco", "Catalog name")
catalog = dbutils.widgets.get("catalog")

ORDER_SCHEMA = (
    "STRUCT<"
    "customer_id: BIGINT, "
    "items: ARRAY<STRUCT<"
    "  category: STRING, "
    "  details: STRUCT<brand: STRING, color: STRING>, "
    "  item_id: BIGINT, "
    "  name: STRING, "
    "  price: BIGINT, "
    "  quantity: BIGINT"
    ">>, "
    "order_date: STRING, "
    "order_id: BIGINT, "
    "order_status: STRING, "
    "payment_method: STRING, "
    "total_amount: BIGINT, "
    "transaction_timestamp: STRING"
    ">"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fix the malformed date field and parse to JSON

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW tv_orders_parsed AS
SELECT from_json(
  regexp_replace(value, '"order_date": (\\\\d{{4}}-\\\\d{{2}}-\\\\d{{2}})', '"order_date":"$1"'),
  '{ORDER_SCHEMA}'
) AS json_value
FROM {catalog}.bronze.v_orders
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deduplicate and explode the line items, then write to Silver

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.silver.orders
AS
SELECT
  json_value.order_id AS order_id,
  json_value.order_status AS order_status,
  json_value.payment_method AS payment_method,
  json_value.total_amount AS total_amount,
  CAST(json_value.transaction_timestamp AS TIMESTAMP) AS transaction_timestamp,
  json_value.customer_id AS customer_id,
  item.item_id AS item_id,
  item.name AS name,
  item.price AS price,
  item.quantity AS quantity,
  item.category AS category,
  item.details.brand AS brand,
  item.details.color AS color
FROM (
  SELECT json_value, explode(array_distinct(json_value.items)) AS item
  FROM tv_orders_parsed
)
""")

display(spark.table(f"{catalog}.silver.orders"))
