"""Shared pytest fixtures: a single local, in-process SparkSession.

No Databricks workspace or cloud credentials are needed to run this suite —
everything runs against ``local[2]``. Delta support is configured through
``delta-spark`` so the merge-helper tests can exercise real `MERGE INTO`
semantics against a temporary local warehouse.

Deliberately a single session-scoped SparkSession, not one per fixture:
Spark only allows one SparkContext per JVM, so ``SparkSession.getOrCreate()``
silently reuses whatever session was created first, config and all. Two
differently-configured session fixtures in the same test run raced to be
"first" and produced hard-to-debug catalog errors — one Delta-enabled
session, shared by every test, avoids that entirely and costs nothing (Delta
just adds a catalog; it doesn't restrict plain DataFrame use).
"""

from __future__ import annotations

import shutil
import tempfile
from collections.abc import Iterator

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    warehouse_dir = tempfile.mkdtemp(prefix="retailco-lakehouse-test-warehouse-")

    builder = (
        SparkSession.builder.master("local[2]")
        .appName("retailco-lakehouse-tests")
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
