# Databricks notebook source
# MAGIC %md
# MAGIC # Learning: higher-order array functions
# MAGIC Reference notebook, not part of the production pipeline. Originally
# MAGIC "21 Higher Order Functions" — `TRANSFORM`, `FILTER`, `EXISTS`, and `AGGREGATE`
# MAGIC work on arrays without exploding them into rows first, e.g. for computing an
# MAGIC order total across nested line items in place.
# MAGIC
# MAGIC Syntax: `function_name(array_column, lambda_expression)`

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW order_items AS
SELECT * FROM VALUES
  (1, array('smartphone', 'laptop', 'monitor')),
  (2, array('tablet', 'headphones', 'smartwatch')),
  (3, array('keyboard', 'mouse'))
AS orders(order_id, items)
""")
display(spark.sql("SELECT * FROM order_items"))

# COMMAND ----------

display(spark.sql("""
SELECT order_id, TRANSFORM(items, x -> upper(x)) AS upper_items
FROM order_items
"""))

# COMMAND ----------

display(spark.sql("""
SELECT order_id, FILTER(items, x -> x LIKE '%smart%') AS smart_items
FROM order_items
"""))

# COMMAND ----------

display(spark.sql("""
SELECT order_id, EXISTS(items, x -> x LIKE '%monitor%') AS has_monitor
FROM order_items
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## The same, over an array of structs

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW order_items_priced AS
SELECT * FROM VALUES
  (1, array(
        named_struct('name', 'smartphone', 'price', 699),
        named_struct('name', 'laptop', 'price', 1199),
        named_struct('name', 'monitor', 'price', 399)
  )),
  (2, array(
        named_struct('name', 'tablet', 'price', 599),
        named_struct('name', 'headphones', 'price', 199),
        named_struct('name', 'smartwatch', 'price', 299)
  )),
  (3, array(
        named_struct('name', 'keyboard', 'price', 89),
        named_struct('name', 'mouse', 'price', 59)
  ))
AS orders(order_id, items)
""")
display(spark.sql("SELECT * FROM order_items_priced"))

# COMMAND ----------

# Original used `x.price + 1.10` (adds a flat $1.10 to every price). Changed to
# `x.price * 1.10` here, since the intent — labeled "with_tax" — is clearly a 10% rate.
display(spark.sql("""
SELECT order_id,
  TRANSFORM(items, x -> named_struct(
    'name', upper(x.name),
    'price', round(x.price * 1.10, 2)
  )) AS items_with_tax
FROM order_items_priced
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate

# COMMAND ----------

display(spark.sql("""
SELECT order_id, AGGREGATE(items, 0, (acc, x) -> acc + x.price) AS order_total
FROM order_items_priced
"""))
