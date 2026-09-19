"""Silver -> Gold transformations."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

EXCLUDED_ORDER_STATUSES = ("Cancelled", "Pending")


def build_customer_address(customers_df: DataFrame, addresses_df: DataFrame) -> DataFrame:
    """Join customer profile with their shipping/billing address."""
    return customers_df.alias("c").join(addresses_df.alias("a"), on="customer_id").select(
        F.col("c.customer_id"),
        F.col("c.customer_name"),
        F.col("c.date_of_birth"),
        F.col("c.email"),
        F.col("c.telephone"),
        F.col("a.shipping_address_line_1"),
        F.col("a.shipping_city"),
        F.col("a.shipping_state"),
        F.col("a.shipping_postcode"),
        F.col("a.billing_address_line_1"),
        F.col("a.billing_city"),
        F.col("a.billing_state"),
        F.col("a.billing_postcode"),
    )


def build_monthly_order_summary(orders_df: DataFrame) -> DataFrame:
    """Total orders, items, and revenue per customer per month.

    Excludes cancelled/pending orders, since neither represents realized
    revenue. The output column is ``total_amount`` — the original pipeline
    had a typo here (``total_amnount``).
    """
    return (
        orders_df.filter(~F.col("order_status").isin(*EXCLUDED_ORDER_STATUSES))
        .withColumn("transaction_month", F.date_format("transaction_timestamp", "yyyy-MM"))
        .groupBy("customer_id", "transaction_month")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("quantity").alias("total_items"),
            F.sum(F.col("quantity") * F.col("price")).alias("total_amount"),
        )
    )
