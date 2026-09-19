"""Auto Loader ingestion helpers for the file-based Bronze sources."""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery


def read_autoloader_stream(
    spark: SparkSession,
    source_path: str,
    file_format: str,
    schema_tracking_path: str,
    schema_evolution_mode: str = "addNewColumns",
    reader_options: dict[str, str] | None = None,
) -> DataFrame:
    """Open a streaming read of ``source_path`` with Auto Loader.

    ``schema_evolution_mode="addNewColumns"`` means a new source column is
    picked up automatically (the stream stops once to pick up the new
    schema, then resumes) instead of the read silently ignoring it or
    failing outright.
    """
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", file_format)
        .option("cloudFiles.schemaLocation", schema_tracking_path)
        .option("cloudFiles.schemaEvolutionMode", schema_evolution_mode)
    )
    for key, value in (reader_options or {}).items():
        reader = reader.option(key, value)
    return reader.load(source_path)


def write_autoloader_stream(
    stream_df: DataFrame,
    target_table: str,
    checkpoint_path: str,
    trigger_available_now: bool = True,
) -> StreamingQuery:
    """Write a streaming DataFrame to a managed Delta table.

    ``trigger(availableNow=True)`` processes everything currently available
    and then stops, which is what a scheduled batch job wants — a
    continuously running stream isn't appropriate for a nightly Workflow.
    """
    writer = (
        stream_df.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
    )
    if trigger_available_now:
        writer = writer.trigger(availableNow=True)
    return writer.toTable(target_table)


def run_autoloader_to_table(
    spark: SparkSession,
    source_path: str,
    file_format: str,
    target_table: str,
    checkpoint_path: str,
    schema_tracking_path: str,
    reader_options: dict[str, str] | None = None,
    extra_columns: dict[str, Any] | None = None,
) -> None:
    """Read and write one Auto Loader batch, blocking until it completes.

    ``extra_columns`` is a mapping of column name -> PySpark Column
    expression (e.g. ``{"ingested_at": F.current_timestamp()}``), applied to
    every ingested row before it's written.
    """
    stream_df = read_autoloader_stream(
        spark, source_path, file_format, schema_tracking_path, reader_options=reader_options
    )
    for column_name, expression in (extra_columns or {}).items():
        stream_df = stream_df.withColumn(column_name, expression)
    query = write_autoloader_stream(stream_df, target_table, checkpoint_path)
    query.awaitTermination()
