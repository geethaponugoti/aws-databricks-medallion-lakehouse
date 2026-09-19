# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: customers
# MAGIC Exposes the raw customer JSON files from the landing volume as
# MAGIC `retailco.bronze.v_customers`, tagged with source file lineage.

# COMMAND ----------

dbutils.widgets.text("catalog", "retailco", "Catalog name")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

display(dbutils.fs.ls(f"/Volumes/{catalog}/landing/operational_data/customers/"))

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.bronze.v_customers AS
SELECT *, _metadata.file_path AS file_path
FROM json.`/Volumes/{catalog}/landing/operational_data/customers`
""")

display(spark.table(f"{catalog}.bronze.v_customers"))
