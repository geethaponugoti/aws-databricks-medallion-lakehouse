# Databricks notebook source
# MAGIC %md
# MAGIC # Setup: catalog, schemas, and landing volume
# MAGIC Idempotent bootstrap for a new environment. Creates the `retailco` catalog (name
# MAGIC configurable per target — e.g. `retailco_dev`), the `landing` / `bronze` / `silver`
# MAGIC / `gold` / `audit` schemas, and the external volume raw files land in. Safe to
# MAGIC re-run: every statement is `IF NOT EXISTS`.

# COMMAND ----------

dbutils.widgets.text("catalog", "retailco", "Catalog name")
dbutils.widgets.text("bucket_name", "", "S3 bucket name")

catalog = dbutils.widgets.get("catalog")
bucket_name = dbutils.widgets.get("bucket_name")
assert bucket_name, "Set the bucket_name widget/parameter before running this notebook."

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catalog

# COMMAND ----------

spark.sql(f"""
CREATE CATALOG IF NOT EXISTS {catalog}
MANAGED LOCATION 's3://{bucket_name}/'
COMMENT 'RetailCo data lakehouse catalog'
""")
spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schemas
# MAGIC - `landing` — raw file volume, nothing queryable directly
# MAGIC - `bronze` / `silver` / `gold` — the medallion layers
# MAGIC - `audit` — data-quality check results

# COMMAND ----------

for schema in ("landing", "bronze", "silver", "gold", "audit"):
    spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}
    MANAGED LOCATION 's3://{bucket_name}/{schema}'
    """)

display(spark.sql(f"SHOW SCHEMAS IN {catalog}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Landing volume
# MAGIC External volume over the raw operational data files (customers, orders,
# MAGIC memberships, addresses) that Bronze ingests from.

# COMMAND ----------

spark.sql(f"""
CREATE EXTERNAL VOLUME IF NOT EXISTS {catalog}.landing.operational_data
LOCATION 's3://{bucket_name}/operational_data/'
""")

display(dbutils.fs.ls(f"/Volumes/{catalog}/landing/operational_data/"))
