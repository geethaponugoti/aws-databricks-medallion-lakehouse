"""Bronze -> Silver transformation for addresses."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

MERGE_KEYS = ["customer_id"]


def clean_addresses(bronze_df: DataFrame) -> DataFrame:
    """Pivot one row per (customer, address type) into one row per customer.

    Produces ``shipping_address_line_1``, ``shipping_city``, ...,
    ``billing_address_line_1``, ``billing_city``, ... columns.
    """
    return (
        bronze_df.groupBy("customer_id")
        .pivot("address_type", ["shipping", "billing"])
        .agg(
            F.max("address_line_1").alias("address_line_1"),
            F.max("city").alias("city"),
            F.max("state").alias("state"),
            F.max("postcode").alias("postcode"),
        )
    )
