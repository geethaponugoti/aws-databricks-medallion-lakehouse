"""Bronze -> Silver transformation for orders.

The raw export is newline-delimited JSON with one malformed field —
``order_date`` isn't quoted — which breaks strict JSON parsing. Bronze reads
it as plain text (see ``notebooks/bronze/bronze_orders.py``); this module
fixes the field, parses it against a known schema, and explodes the nested
``items`` array to one row per order line item.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

MERGE_KEYS = ["order_id", "item_id"]

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

_UNQUOTED_ORDER_DATE_PATTERN = r'"order_date": (\d{4}-\d{2}-\d{2})'
_QUOTED_ORDER_DATE_REPLACEMENT = r'"order_date":"$1"'


def parse_orders(bronze_df: DataFrame) -> DataFrame:
    """Parse the raw order text into one struct-typed row per order."""
    fixed = bronze_df.withColumn(
        "fixed_value",
        F.regexp_replace(F.col("value"), _UNQUOTED_ORDER_DATE_PATTERN, _QUOTED_ORDER_DATE_REPLACEMENT),
    )
    return fixed.select(F.from_json("fixed_value", ORDER_SCHEMA).alias("json_value"))


def explode_order_items(parsed_df: DataFrame) -> DataFrame:
    """Explode ``json_value.items`` (deduplicated) into one row per line item, flattened."""
    exploded = parsed_df.select(
        "json_value", F.explode(F.array_distinct("json_value.items")).alias("item")
    )
    return exploded.select(
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


def clean_orders(bronze_df: DataFrame) -> DataFrame:
    """Full Bronze -> Silver orders transformation: parse, then explode/flatten."""
    return explode_order_items(parse_orders(bronze_df))
