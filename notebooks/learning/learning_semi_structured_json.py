# Databricks notebook source
# MAGIC %md
# MAGIC # Learning: querying semi-structured JSON
# MAGIC Reference notebook, not part of the production pipeline. Originally
# MAGIC "15 Play with Orders Json" — the `:` path-extraction syntax for poking at JSON
# MAGIC text without parsing it into a typed struct first. The production Silver orders
# MAGIC flow (`notebooks/silver/silver_orders.py`) uses `from_json` with an explicit
# MAGIC schema instead, once the shape of the data is known.

# COMMAND ----------

dbutils.widgets.text("catalog", "retailco", "Catalog name")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top-level extraction: `value:field`

# COMMAND ----------

display(spark.sql(f"""
SELECT value:order_id AS order_id, value
FROM {catalog}.bronze.v_orders
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Array indexing: `value:items[n]`

# COMMAND ----------

display(spark.sql(f"""
SELECT
  value:order_id AS order_id,
  value:items[0] AS item_1,
  value:items[1] AS item_2,
  value:items AS items
FROM {catalog}.bronze.v_orders
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Nested field extraction and casting

# COMMAND ----------

display(spark.sql(f"""
SELECT
  value:order_id AS order_id,
  value:items[0].item_id::int AS item_1_item_id,
  value:items[0] AS item_1,
  value:items[1] AS item_2
FROM {catalog}.bronze.v_orders
"""))
