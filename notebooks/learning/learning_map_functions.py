# Databricks notebook source
# MAGIC %md
# MAGIC # Learning: map functions
# MAGIC Reference notebook, not part of the production pipeline. Originally "22 Map".
# MAGIC A map is a collection of key-value pairs, e.g. `{'laptop': 1200, 'phone': 1000}`.
# MAGIC
# MAGIC - `TRANSFORM_KEYS`
# MAGIC - `TRANSFORM_VALUES`
# MAGIC - `MAP_FILTER`
# MAGIC
# MAGIC Syntax: `function_name(map_column, lambda_expression)`

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW order_item_prices AS
SELECT * FROM VALUES
  (1, map('smartphone', 699, 'laptop', 1199, 'monitor', 399)),
  (2, map('tablet', 599, 'headphones', 199, 'smartwatch', 299)),
  (3, map('keyboard', 89, 'mouse', 59))
AS orders(order_id, item_prices)
""")
display(spark.sql("SELECT * FROM order_item_prices"))

# COMMAND ----------

display(spark.sql("""
SELECT order_id, TRANSFORM_KEYS(item_prices, (k, v) -> upper(k)) AS item_prices
FROM order_item_prices
"""))

# COMMAND ----------

display(spark.sql("""
SELECT order_id, TRANSFORM_VALUES(item_prices, (k, v) -> round(v * 1.10, 2)) AS item_prices_with_tax
FROM order_item_prices
"""))

# COMMAND ----------

display(spark.sql("""
SELECT order_id, MAP_FILTER(item_prices, (k, v) -> v > 500) AS premium_items
FROM order_item_prices
"""))
