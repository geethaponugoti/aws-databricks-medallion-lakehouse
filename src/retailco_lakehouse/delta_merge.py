"""Generic Delta upsert helper used by every Silver notebook."""

from __future__ import annotations

from collections.abc import Sequence

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


def merge_into(
    spark: SparkSession,
    target_table: str,
    updates: DataFrame,
    merge_keys: Sequence[str],
) -> None:
    """Upsert ``updates`` into ``target_table``, keyed on ``merge_keys``.

    Creates ``target_table`` on the first run (so a fresh environment doesn't
    need a separate DDL step), and upserts on every subsequent run — matched
    rows are fully overwritten with the incoming values, unmatched rows are
    inserted. Re-running with the same input is a no-op.
    """
    if not merge_keys:
        raise ValueError("merge_keys must contain at least one column")

    if not spark.catalog.tableExists(target_table):
        updates.write.format("delta").option("mergeSchema", "true").saveAsTable(target_table)
        return

    condition = " AND ".join(f"target.{key} = updates.{key}" for key in merge_keys)
    (
        DeltaTable.forName(spark, target_table)
        .alias("target")
        .merge(updates.alias("updates"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
