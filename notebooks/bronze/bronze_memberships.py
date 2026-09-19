# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: memberships
# MAGIC Membership card images, one PNG per customer, read as `binaryFile`.
# MAGIC `customer_id` is embedded in the filename and extracted in Silver.

# COMMAND ----------

dbutils.widgets.text("catalog", "retailco", "Catalog name")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

display(dbutils.fs.ls(f"/Volumes/{catalog}/landing/operational_data/memberships/"))

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.bronze.v_memberships AS
SELECT * FROM binaryFile.`/Volumes/{catalog}/landing/operational_data/memberships/*/*.png`
""")

display(spark.table(f"{catalog}.bronze.v_memberships"))
