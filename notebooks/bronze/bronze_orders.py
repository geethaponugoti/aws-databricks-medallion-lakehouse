# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: orders
# MAGIC The raw orders export is newline-delimited JSON with one malformed field
# MAGIC (`order_date` isn't quoted), which breaks strict JSON parsing. Bronze reads it as
# MAGIC `text` so nothing is lost; the fix-up and parsing happens in Silver.

# COMMAND ----------

dbutils.widgets.text("catalog", "retailco", "Catalog name")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.bronze.v_orders AS
SELECT * FROM text.`/Volumes/{catalog}/landing/operational_data/orders`
""")

display(spark.table(f"{catalog}.bronze.v_orders"))
