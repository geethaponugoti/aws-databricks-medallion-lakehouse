# Databricks notebook source
# MAGIC %md
# MAGIC # Learning: SQL user-defined functions
# MAGIC Reference notebook, not part of the production pipeline. Originally "20 UDFs".
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REPLACE FUNCTION catalog.schema.function_name(param1 TYPE, param2 TYPE)
# MAGIC RETURNS TYPE
# MAGIC RETURN expression;
# MAGIC ```

# COMMAND ----------

dbutils.widgets.text("catalog", "retailco", "Catalog name")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {catalog}.default.get_full_name(firstname STRING, surname STRING)
RETURNS STRING
RETURN CONCAT(initcap(firstname), ' ', initcap(surname))
""")

display(spark.sql(f"SELECT {catalog}.default.get_full_name('amanullah', 's') AS full_name"))

# COMMAND ----------

display(spark.sql(f"DESC FUNCTION EXTENDED {catalog}.default.get_full_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC A SQL UDF is a clean way to package a small piece of business logic (like a
# MAGIC status-code lookup) for reuse across notebooks — here as an alternative to the
# MAGIC `CASE` expression `silver_payments.py` uses inline for the same mapping.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {catalog}.default.get_payment_status(payment_status INT)
RETURNS STRING
RETURN CASE payment_status
  WHEN 1 THEN 'Success'
  WHEN 2 THEN 'Pending'
  WHEN 3 THEN 'Cancelled'
  WHEN 4 THEN 'Failed'
END
""")

display(spark.sql(f"""
SELECT
  payment_id,
  customer_id,
  {catalog}.default.get_payment_status(CAST(payment_status AS INT)) AS payment_status
FROM {catalog}.bronze.payments
"""))
