"""Shared pytest fixtures: local, in-process SparkSessions.

No Databricks workspace or cloud credentials are needed to run this suite —
everything runs against ``local[2]``. Delta support is configured through
``delta-spark`` so the merge-helper tests can exercise real `MERGE INTO`
semantics against a temporary local warehouse.
"""

from __future__ import annotations

import shutil
import tempfile
from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """A plain local SparkSession, for tests that don't need Delta tables."""
    session = (
        SparkSession.builder.master("local[2]")
        .appName("retailco-lakehouse-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def delta_spark() -> Iterator[SparkSession]:
    """A SparkSession with Delta Lake enabled and its own temp warehouse.

    Separate from ``spark`` because configuring Delta's SQL extensions has to
    happen before the session is created.
    """
    from delta import configure_spark_with_delta_pip

    warehouse_dir = tempfile.mkdtemp(prefix="retailco-lakehouse-test-warehouse-")

    builder = (
        SparkSession.builder.master("local[2]")
        .appName("retailco-lakehouse-delta-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", warehouse_dir)
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()
    shutil.rmtree(warehouse_dir, ignore_errors=True)
