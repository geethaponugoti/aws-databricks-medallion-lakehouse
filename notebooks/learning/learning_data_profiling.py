# Databricks notebook source
# MAGIC %md
# MAGIC # Learning: data profiling
# MAGIC Reference notebook, not part of the production pipeline. Originally
# MAGIC "09 Data Profiling" — the ad hoc exploration that led to the checks now
# MAGIC formalized in `src/retailco_lakehouse/data_quality.py` and run by
# MAGIC `notebooks/data_quality/run_data_quality_checks.py`.

# COMMAND ----------

dbutils.widgets.text("catalog", "retailco", "Catalog name")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Profile using the UI / `dbutils.data.summarize`

# COMMAND ----------

customers = spark.table(f"{catalog}.bronze.v_customers")
customers.printSchema()
display(customers.summary())

# COMMAND ----------

dbutils.data.summarize(customers)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Null counts with `count_if`

# COMMAND ----------

display(spark.sql(f"""
SELECT
  count(*) AS total_rows,
  count_if(customer_id IS NULL) AS null_customer_ids,
  count_if(email IS NULL) AS null_emails,
  count_if(telephone IS NULL) AS null_telephones
FROM {catalog}.bronze.v_customers
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Duplicate analysis

# COMMAND ----------

display(spark.sql(f"""
SELECT
  count(*) AS total_records,
  count(DISTINCT customer_id) AS unique_customer_ids
FROM {catalog}.bronze.v_customers
WHERE customer_id IS NOT NULL
"""))
