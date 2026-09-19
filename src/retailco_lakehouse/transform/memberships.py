"""Bronze -> Silver transformation for memberships."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

MERGE_KEYS = ["customer_id"]

_FILENAME_CUSTOMER_ID_PATTERN = r".*/([0-9]+)\.png$"


def clean_memberships(bronze_df: DataFrame) -> DataFrame:
    """Extract ``customer_id`` from the membership card image's filename."""
    return bronze_df.select(
        F.regexp_extract("path", _FILENAME_CUSTOMER_ID_PATTERN, 1).alias("customer_id"),
        F.col("content").alias("membership_card"),
    )
