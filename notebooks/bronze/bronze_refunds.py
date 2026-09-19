# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: refunds
# MAGIC `refund_reason` packs two values as `"<reason>:<source>"` (e.g.
# MAGIC `"Order Cancelled:Customer"`) — split out in Silver.
# MAGIC
# MAGIC The upstream billing export doesn't include a refunds file yet, so the original
# MAGIC project seeded this table with `INSERT` statements. Kept as-is here (table name
# MAGIC lowercased to match the rest of the catalog); converted to `COPY INTO` from an S3
# MAGIC CSV extract, matching `bronze_payments`, once that export exists — see the
# MAGIC incremental-load version of this notebook.

# COMMAND ----------

dbutils.widgets.text("catalog", "retailco", "Catalog name")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.refunds (
    refund_id INT,
    payment_id INT NOT NULL,
    refund_timestamp TIMESTAMP NOT NULL,
    refund_amount DECIMAL(10, 2) NOT NULL,
    refund_reason STRING NOT NULL
)
""")

# COMMAND ----------

spark.sql(f"""
INSERT INTO {catalog}.bronze.refunds
  (refund_id, payment_id, refund_timestamp, refund_amount, refund_reason)
VALUES
  (1, 66, '2025-01-10 11:30:00', 85.75, 'Payment Error:Retailer'),
  (2, 69, '2025-01-03 12:40:15', 120.50, 'Order Cancelled:Customer'),
  (3, 72, '2025-01-06 14:45:30', 65.00, 'Product Returned:Customer'),
  (4, 73, '2025-01-07 16:10:45', 210.99, 'Order Cancelled:Customer'),
  (5, 75, '2025-01-09 18:25:00', 45.20, 'Payment Error:Retailer'),
  (6, 80, '2025-01-10 09:35:20', 130.15, 'Order Cancelled:Customer'),
  (7, 83, '2025-01-12 11:20:40', 150.00, 'Product Returned:Customer'),
  (8, 85, '2025-01-14 13:15:30', 89.99, 'Order Cancelled:Customer'),
  (9, 89, '2025-01-15 15:00:00', 78.50, 'Payment Error:Retailer'),
  (10, 91, '2025-01-17 16:45:15', 250.75, 'Product Returned:Customer')
""")

display(spark.table(f"{catalog}.bronze.refunds"))
