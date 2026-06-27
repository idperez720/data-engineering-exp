"""This module implements entity-driven structured data loading utilities for flint."""

import os
from typing import Any, Optional, cast

import pandas as pd

from flint_core.core.catalog import DataCatalog, DatasetConfiguration

try:
    from pyspark.sql import SparkSession

    HAS_SPARK = True
except ImportError:
    SparkSession = cast(Any, None)
    HAS_SPARK = False


class DataLoader:
    """Handles dynamic loading of data elements mapping properties from strict catalog entities."""

    def __init__(self, catalog: Optional[DataCatalog] = None) -> None:
        """Initializes the DataLoader with a specific DataCatalog context reference.

        Args:
            catalog: An active repository pipeline tracking declarations.
        """
        self.catalog: DataCatalog = catalog if catalog is not None else DataCatalog()

    def load(self, dataset_name: str, spark: Optional[Any] = None) -> Any:
        """Loads data matrices from local or distributed assets routing structural properties.

        Args:
            dataset_name: Canonical alphanumeric string key identification tag.
            spark: A user-supplied runtime context cluster reference.

        Returns:
            Any: Active matrix execution context mapping Pandas or distributed Spark elements.

        Raises:
            ValueError: If an unsupported asset specification rule is detected.
            ImportError: If processing frameworks elements are missing inside dependencies.
        """
        dataset: DatasetConfiguration = self.catalog.get_dataset(dataset_name)
        path: str = dataset.storage_path

        if not os.path.isabs(path):
            path = os.path.normpath(os.path.join(self.catalog.project_root, path))

        if dataset.engine == "pandas":
            return self._load_with_pandas(path, dataset.format)
        elif dataset.engine == "spark":
            return self._load_with_spark(path, dataset.format, spark)
        else:
            raise ValueError(f"Unsupported storage processing platform backend: '{dataset.engine}'.")

    def _load_with_pandas(self, path: str, data_format: str) -> pd.DataFrame:
        """Routes stream extractions operations safely to local execution pools."""
        if data_format == "parquet":
            return pd.read_parquet(path)
        elif data_format == "csv":
            return pd.read_csv(path)
        else:
            raise ValueError(f"Unsupported Pandas file layout standard parsing rule: '{data_format}'.")

    def _load_with_spark(self, path: str, data_format: str, spark: Optional[Any]) -> Any:
        """Routes dataset evaluation execution plans safely to parallel Spark executors clusters."""
        if not HAS_SPARK:
            raise ImportError("PySpark environment pipelines modules components are missing inside environment.")

        session = spark if spark is not None else SparkSession.getActiveSession()

        if session is None:
            raise ValueError(
                "No distributed cluster session manager could be resolved globally inside active contexts."
            )

        if data_format == "parquet":
            return session.read.parquet(path)
        elif data_format == "csv":
            return session.read.csv(path, header=True, inferSchema=True)
        else:
            raise ValueError(f"Unsupported distributed Big Data stream loading parameter: '{data_format}'.")
