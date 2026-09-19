# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: orders
# MAGIC Combines what were originally two notebooks ("Transform Orders Data" and
# MAGIC "Transform Orders Data - Explode Arrays") into one Silver flow:
# MAGIC 1. Fix the one malformed field in the raw JSON text (`order_date` isn't quoted).
# MAGIC 2. Parse it into a struct with a known schema.
# MAGIC 3. Explode the `items` array to one row per line item and flatten it.
# MAGIC 4. Upsert into `retailco.silver.orders` with `MERGE INTO`, keyed on
# MAGIC    `(order_id, item_id)` since the grain is one row per order line item.
# MAGIC
# MAGIC The original persisted step 2's output as its own table (`silver.orders_json`)
# MAGIC before exploding it in a second notebook. That intermediate table added no
# MAGIC value downstream, so it's collapsed into a single in-memory step here.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "retailco", "Catalog name")
catalog = dbutils.widgets.get("catalog")
target_table = f"{catalog}.silver.orders"

ORDER_SCHEMA = (
    "STRUCT<"
    "customer_id: BIGINT, "
    "items: ARRAY<STRUCT<"
    "  category: STRING, "
    "  details: STRUCT<brand: STRING, color: STRING>, "
    "  item_id: BIGINT, "
    "  name: STRING, "
    "  price: BIGINT, "
    "  quantity: BIGINT"
    ">>, "
    "order_date: STRING, "
    "order_id: BIGINT, "
    "order_status: STRING, "
    "payment_method: STRING, "
    "total_amount: BIGINT, "
    "transaction_timestamp: STRING"
    ">"
)

# COMMAND ----------

bronze = spark.table(f"{catalog}.bronze.orders")

fixed = bronze.withColumn(
    "fixed_value",
    F.regexp_replace(
        F.col("value"),
        r'"order_date": (\d{4}-\d{2}-\d{2})',
        r'"order_date":"$1"',
    ),
)

parsed = fixed.select(F.from_json("fixed_value", ORDER_SCHEMA).alias("json_value"))

exploded = parsed.select(
    "json_value", F.explode(F.array_distinct("json_value.items")).alias("item")
)

updates = exploded.select(
    F.col("json_value.order_id").alias("order_id"),
    F.col("json_value.order_status").alias("order_status"),
    F.col("json_value.payment_method").alias("payment_method"),
    F.col("json_value.total_amount").alias("total_amount"),
    F.col("json_value.transaction_timestamp").cast("timestamp").alias("transaction_timestamp"),
    F.col("json_value.customer_id").alias("customer_id"),
    F.col("item.item_id").alias("item_id"),
    F.col("item.name").alias("name"),
    F.col("item.price").alias("price"),
    F.col("item.quantity").alias("quantity"),
    F.col("item.category").alias("category"),
    F.col("item.details.brand").alias("brand"),
    F.col("item.details.color").alias("color"),
)

# COMMAND ----------

if spark.catalog.tableExists(target_table):
    (
        DeltaTable.forName(spark, target_table)
        .alias("target")
        .merge(
            updates.alias("updates"),
            "target.order_id = updates.order_id AND target.item_id = updates.item_id",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    updates.write.format("delta").option("mergeSchema", "true").saveAsTable(target_table)

# COMMAND ----------

display(spark.table(target_table))
