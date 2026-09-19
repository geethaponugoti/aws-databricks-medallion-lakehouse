# Databricks notebook source
# MAGIC %md
# MAGIC # Learning: reading files and view types
# MAGIC Reference notebook, not part of the production pipeline. Originally
# MAGIC "03 Extract Customer Data" — the different ways to query files directly in Spark
# MAGIC SQL, and the three kinds of views Databricks supports. The actual Bronze customer
# MAGIC view lives in `notebooks/bronze/bronze_customers.py`; this only keeps the
# MAGIC exploratory cells that show *why* that approach was chosen.
# MAGIC
# MAGIC (Fixed a typo from the original: "Qyerying" -> "Querying".)

# COMMAND ----------

dbutils.widgets.text("catalog", "retailco", "Catalog name")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Querying a single file vs. multiple files vs. a whole folder

# COMMAND ----------

# a single file
spark.sql(f"""
SELECT * FROM json.`/Volumes/{catalog}/landing/operational_data/customers/customers_2024_10.json`
""").show(5)

# COMMAND ----------

# a glob pattern across multiple files
spark.sql(f"""
SELECT * FROM json.`/Volumes/{catalog}/landing/operational_data/customers/customers_2024_*.json`
""").show(5)

# COMMAND ----------

# the whole folder
spark.sql(f"""
SELECT * FROM json.`/Volumes/{catalog}/landing/operational_data/customers`
""").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `_metadata` — per-row file lineage
# MAGIC This is what `bronze_customers.py` uses to tag every row with its source file.

# COMMAND ----------

spark.sql(f"""
SELECT _metadata, _metadata.file_path AS file_path, *
FROM json.`/Volumes/{catalog}/landing/operational_data/customers`
""").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## View, temporary view, and global temporary view
# MAGIC - a regular view is a Unity Catalog object, visible to anyone with access to the schema
# MAGIC - a temporary view only exists for the current Spark session
# MAGIC - a global temporary view exists for the lifetime of the cluster (all sessions)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW tv_customers AS
SELECT *, _metadata.file_path AS file_path
FROM json.`/Volumes/{catalog}/landing/operational_data/customers`
""")
spark.sql("SELECT * FROM tv_customers").show(5)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMPORARY VIEW gtv_customers AS
SELECT *, _metadata.file_path AS file_path
FROM json.`/Volumes/{catalog}/landing/operational_data/customers`
""")
spark.sql("SELECT * FROM global_temp.gtv_customers").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Same thing, three different Spark APIs
# MAGIC `spark.sql`, the DataFrame reader API, and `spark.table` all end up in the same
# MAGIC place — pick whichever reads more clearly for the task.

# COMMAND ----------

df = spark.sql(f"SELECT * FROM json.`/Volumes/{catalog}/landing/operational_data/customers/`")
display(df)

# COMMAND ----------

df = spark.read.format("json").load(f"/Volumes/{catalog}/landing/operational_data/customers/")
# equivalent shorthand:
df = spark.read.json(f"/Volumes/{catalog}/landing/operational_data/customers/")
display(df)

# COMMAND ----------

df = spark.table(f"{catalog}.bronze.v_addresses")
display(df)
