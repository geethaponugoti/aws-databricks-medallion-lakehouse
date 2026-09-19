"""Bronze -> Silver transformation for refunds."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

MERGE_KEYS = ["refund_id"]


def clean_refunds(bronze_df: DataFrame) -> DataFrame:
    """Split ``refund_timestamp`` into date/time and ``refund_reason`` into reason/source.

    ``refund_reason`` in the source packs two values as ``"<reason>:<source>"``,
    e.g. ``"Order Cancelled:Customer"``.
    """
    return bronze_df.select(
        F.col("refund_id"),
        F.col("payment_id"),
        F.to_date("refund_timestamp").alias("refund_date"),
        F.date_format("refund_timestamp", "HH:mm:ss").alias("refund_time"),
        F.col("refund_amount"),
        F.split(F.col("refund_reason"), ":").getItem(0).alias("refund_reason"),
        F.split(F.col("refund_reason"), ":").getItem(1).alias("refund_source"),
    )
