"""Data quality checks and the audit log they write to.

Each check function takes a DataFrame and returns a :class:`CheckResult` —
no table reads or writes — so they're unit testable with small in-memory
DataFrames. ``run_checks``/``write_results`` do the I/O: reading the tables
a real run checks and appending to the audit table.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

AUDIT_TABLE_SCHEMA = StructType(
    [
        StructField("check_name", StringType(), False),
        StructField("layer", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("status", StringType(), False),
        StructField("row_count", LongType(), False),
        StructField("run_timestamp", TimestampType(), False),
    ]
)


@dataclass(frozen=True)
class CheckResult:
    check_name: str
    layer: str
    table_name: str
    status: str  # "PASS" or "FAIL"
    row_count: int  # offending/affected row count — meaning depends on the check

    @property
    def passed(self) -> bool:
        return self.status == "PASS"


def check_not_null(df: DataFrame, column: str, *, layer: str, table_name: str) -> CheckResult:
    """Fail if any row has a null value in ``column``."""
    bad = df.filter(F.col(column).isNull()).count()
    return CheckResult("not_null", layer, table_name, "PASS" if bad == 0 else "FAIL", bad)


def check_no_duplicates(
    df: DataFrame, key_columns: Sequence[str], *, layer: str, table_name: str
) -> CheckResult:
    """Fail if any combination of ``key_columns`` appears more than once."""
    dup_groups = df.groupBy(*key_columns).count().filter(F.col("count") > 1).count()
    return CheckResult("no_duplicates", layer, table_name, "PASS" if dup_groups == 0 else "FAIL", dup_groups)


def check_row_count_reconciliation(
    upstream_df: DataFrame, downstream_df: DataFrame, *, layer: str, table_name: str
) -> CheckResult:
    """Fail if the downstream table has more rows than its upstream source.

    A Silver table that filters/deduplicates its Bronze source should never
    end up larger than that source; if it does, something in the
    transformation is fanning rows out unexpectedly.
    """
    upstream_count = upstream_df.count()
    downstream_count = downstream_df.count()
    status = "PASS" if downstream_count <= upstream_count else "FAIL"
    return CheckResult(
        "row_count_reconciliation", layer, table_name, status, abs(upstream_count - downstream_count)
    )


def check_valid_values(
    df: DataFrame, column: str, allowed_values: Sequence[str], *, layer: str, table_name: str
) -> CheckResult:
    """Fail if any non-null value in ``column`` falls outside ``allowed_values``."""
    bad = df.filter(F.col(column).isNotNull() & ~F.col(column).isin(list(allowed_values))).count()
    return CheckResult("valid_values", layer, table_name, "PASS" if bad == 0 else "FAIL", bad)


def results_to_dataframe(
    spark: SparkSession, results: Sequence[CheckResult], run_timestamp: datetime | None = None
) -> DataFrame:
    """Build the audit-table DataFrame for a batch of check results."""
    run_timestamp = run_timestamp or datetime.now(timezone.utc)
    rows = [
        (r.check_name, r.layer, r.table_name, r.status, int(r.row_count), run_timestamp) for r in results
    ]
    return spark.createDataFrame(rows, schema=AUDIT_TABLE_SCHEMA)


def write_results(spark: SparkSession, results: Sequence[CheckResult], audit_table: str) -> None:
    """Append a batch of check results to the audit table."""
    results_to_dataframe(spark, results).write.format("delta").mode("append").option(
        "mergeSchema", "true"
    ).saveAsTable(audit_table)
