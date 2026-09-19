"""Bronze -> Silver transformation for customers."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

MERGE_KEYS = ["customer_id"]


def clean_customers(bronze_df: DataFrame) -> DataFrame:
    """Drop rows with a null key, keep the latest record per customer, and type-cast.

    A customer can appear more than once in Bronze (e.g. a re-ingested file);
    this keeps only the row with the most recent ``created_timestamp`` for
    each ``customer_id``.
    """
    latest_per_customer = Window.partitionBy("customer_id").orderBy(F.col("created_timestamp").desc())

    return (
        bronze_df.filter(F.col("customer_id").isNotNull())
        .withColumn("_rn", F.row_number().over(latest_per_customer))
        .filter(F.col("_rn") == 1)
        .select(
            F.col("customer_id").cast("long").alias("customer_id"),
            F.col("customer_name"),
            F.col("date_of_birth").cast("date").alias("date_of_birth"),
            F.col("email"),
            F.col("member_since").cast("date").alias("member_since"),
            F.col("telephone"),
            F.col("created_timestamp").cast("timestamp").alias("created_timestamp"),
        )
    )
