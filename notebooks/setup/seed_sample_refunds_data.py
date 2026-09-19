# Databricks notebook source
# MAGIC %md
# MAGIC # Seed sample refunds data (dev/demo only)
# MAGIC The billing system export for refunds doesn't exist in this project's sample
# MAGIC data, so `bronze_refunds.py` has nothing to `COPY INTO` from without this.
# MAGIC Writes the same ten rows the original project seeded via `INSERT` statements
# MAGIC as a CSV to `external_data/refunds/`, so the incremental Bronze load has
# MAGIC something to ingest. Not part of the scheduled Workflow — run this once per
# MAGIC environment before the first `bronze_refunds` run, if you don't have a real
# MAGIC refunds export to point at instead.

# COMMAND ----------

dbutils.widgets.text("bucket_name", "", "S3 bucket name")
bucket_name = dbutils.widgets.get("bucket_name")
assert bucket_name, "Set the bucket_name widget/parameter before running this notebook."

# COMMAND ----------

sample_refunds = spark.createDataFrame(
    [
        (1, 66, "2025-01-10 11:30:00", 85.75, "Payment Error:Retailer"),
        (2, 69, "2025-01-03 12:40:15", 120.50, "Order Cancelled:Customer"),
        (3, 72, "2025-01-06 14:45:30", 65.00, "Product Returned:Customer"),
        (4, 73, "2025-01-07 16:10:45", 210.99, "Order Cancelled:Customer"),
        (5, 75, "2025-01-09 18:25:00", 45.20, "Payment Error:Retailer"),
        (6, 80, "2025-01-10 09:35:20", 130.15, "Order Cancelled:Customer"),
        (7, 83, "2025-01-12 11:20:40", 150.00, "Product Returned:Customer"),
        (8, 85, "2025-01-14 13:15:30", 89.99, "Order Cancelled:Customer"),
        (9, 89, "2025-01-15 15:00:00", 78.50, "Payment Error:Retailer"),
        (10, 91, "2025-01-17 16:45:15", 250.75, "Product Returned:Customer"),
    ],
    schema="refund_id INT, payment_id INT, refund_timestamp STRING, refund_amount DOUBLE, refund_reason STRING",
)

(
    sample_refunds.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"s3://{bucket_name}/external_data/refunds/")
)

display(sample_refunds)
