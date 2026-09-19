"""Bronze -> Silver transformation for payments."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

MERGE_KEYS = ["payment_id"]

PAYMENT_STATUS_MAP = {"1": "Success", "2": "Pending", "3": "Cancelled", "4": "Failed"}


def clean_payments(bronze_df: DataFrame) -> DataFrame:
    """Split ``payment_date`` into date/time and decode the status code."""
    status_map = F.create_map([F.lit(x) for pair in PAYMENT_STATUS_MAP.items() for x in pair])

    return bronze_df.select(
        F.col("payment_id"),
        F.col("customer_id"),
        F.to_date("payment_date").alias("payment_date"),
        F.date_format("payment_date", "HH:mm:ss").alias("payment_time"),
        status_map[F.col("payment_status").cast("string")].alias("payment_status"),
        F.col("payment_method"),
    )
