"""COPY INTO helper for the external, non-streaming Bronze sources."""

from __future__ import annotations

from pyspark.sql import SparkSession


def copy_into(
    spark: SparkSession,
    target_table: str,
    source_path: str,
    file_format: str = "CSV",
    format_options: dict[str, str] | None = None,
    copy_options: dict[str, str] | None = None,
) -> None:
    """Load new files from ``source_path`` into ``target_table``.

    ``COPY INTO`` tracks which files it has already loaded into
    ``target_table``, so calling this repeatedly against the same source only
    ever loads files it hasn't seen before — no de-duplication logic needed
    on the caller's side.
    """
    format_options = format_options or {"header": "true", "delimiter": ","}
    copy_options = copy_options or {"mergeSchema": "true"}

    format_opts_sql = ", ".join(f"'{k}' = '{v}'" for k, v in format_options.items())
    copy_opts_sql = ", ".join(f"'{k}' = '{v}'" for k, v in copy_options.items())

    spark.sql(f"""
        COPY INTO {target_table}
        FROM '{source_path}'
        FILEFORMAT = {file_format}
        FORMAT_OPTIONS ({format_opts_sql})
        COPY_OPTIONS ({copy_opts_sql})
    """)
