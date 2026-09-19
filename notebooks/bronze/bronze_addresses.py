# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: addresses
# MAGIC Tab-delimited CSV export with one row per (customer, address type).

# COMMAND ----------

dbutils.widgets.text("catalog", "retailco", "Catalog name")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.bronze.v_addresses AS
SELECT * FROM read_files(
  '/Volumes/{catalog}/landing/operational_data/addresses',
  format => 'csv',
  delimiter => '\t',
  header => true
)
""")

display(spark.table(f"{catalog}.bronze.v_addresses"))
