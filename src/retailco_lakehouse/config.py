"""Runtime configuration for the pipeline.

Values are resolved from Databricks widgets/job parameters when a ``dbutils``
handle is available, falling back to environment variables for local runs
and CI, and finally to safe defaults. Nothing here reads real infrastructure,
so it's usable from unit tests without a Databricks workspace.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineConfig:
    """Resolved configuration for a single pipeline run."""

    catalog: str
    bucket_name: str

    @property
    def landing_volume_root(self) -> str:
        """Root path of the landing volume raw files are ingested from."""
        return f"/Volumes/{self.catalog}/landing/operational_data"

    def landing_path(self, dataset: str) -> str:
        """Path to a dataset's raw files under the landing volume."""
        return f"{self.landing_volume_root}/{dataset}"

    def external_path(self, dataset: str) -> str:
        """S3 path to a dataset ingested from outside the landing volume."""
        return f"s3://{self.bucket_name}/external_data/{dataset}/"

    def checkpoint_path(self, layer: str, dataset: str) -> str:
        """Auto Loader checkpoint location for a given layer/dataset."""
        return f"s3://{self.bucket_name}/_checkpoints/{layer}/{dataset}"

    def schema_tracking_path(self, layer: str, dataset: str) -> str:
        """Auto Loader schema-tracking location for a given layer/dataset."""
        return f"s3://{self.bucket_name}/_schemas/{layer}/{dataset}"

    def table(self, layer: str, name: str) -> str:
        """Fully qualified three-part table name."""
        return f"{self.catalog}.{layer}.{name}"


def _get_param(name: str, default: str, dbutils=None) -> str:
    if dbutils is not None:
        try:
            return dbutils.widgets.get(name)
        except Exception:
            pass
    return os.environ.get(name.upper(), default)


def load_config(dbutils=None) -> PipelineConfig:
    """Resolve a :class:`PipelineConfig` from widgets, env vars, or defaults.

    ``bucket_name`` has no safe default on purpose — never hardcode it. Pass
    it as a notebook/job parameter (``bucket_name``) or set the
    ``BUCKET_NAME`` environment variable locally.
    """
    catalog = _get_param("catalog", "retailco", dbutils)
    bucket_name = _get_param("bucket_name", "", dbutils)
    return PipelineConfig(catalog=catalog, bucket_name=bucket_name)
