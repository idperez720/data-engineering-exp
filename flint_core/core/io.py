"""This module implements metadata-driven data loading utilities for flint."""

import os
from typing import Any, Optional, cast

import pandas as pd

from flint_core.core.catalog import DataCatalog

try:
    from pyspark.sql import SparkSession

    HAS_SPARK = True
except ImportError:
    SparkSession = cast(Any, None)
    HAS_SPARK = False


class DataLoader:
    """Handles dynamic loading of datasets using catalog metadata definitions.

    Supports loading data via Pandas or PySpark based on the specified
    execution engine and format configurations.
    """

    def __init__(self, catalog: Optional[DataCatalog] = None) -> None:
        """Initializes the DataLoader with a DataCatalog instance.

        Args:
            catalog(Optional[DataCatalog]): An instance of DataCatalog. If
                None, a new instance is automatically discovered.

        Returns:
            None: Initializes the class instance.
        """
        self.catalog = catalog if catalog is not None else DataCatalog()

    def load(self, dataset_name: str, spark: Optional[Any] = None) -> Any:
        """Loads a dataset from storage based on its catalog specification.

        Args:
            dataset_name(str): Unique identifier string of the dataset.
            spark(Optional[Any]): Active PySpark SparkSession instance. Required
                if engine is 'spark' and no active session is globally found.

        Returns:
            Any: Loaded Pandas or PySpark DataFrame object.

        Raises:
            KeyError: If mandatory keys like 'engine', 'format' or
                'storage_path' are missing from metadata.
            ValueError: If an unsupported engine or format is provided.
            ImportError: If the PySpark engine is requested but not installed.
        """
        meta = self.catalog.get_dataset_metadata(dataset_name)

        engine = meta.get("engine")
        data_format = meta.get("format")
        path = meta.get("storage_path")

        if not engine or not data_format or not path:
            raise KeyError(
                f"Dataset '{dataset_name}' metadata must contain "
                f"'engine', 'format', and 'storage_path'."
            )

        # Convention resolution: anchor relative paths safely to project root
        if not os.path.isabs(path):
            path = os.path.normpath(os.path.join(self.catalog.project_root, path))

        if engine == "pandas":
            return self._load_with_pandas(path, data_format)
        elif engine == "spark":
            return self._load_with_spark(path, data_format, spark)
        else:
            raise ValueError(f"Unsupported execution engine: '{engine}'.")

    def _load_with_pandas(self, path: str, data_format: str) -> pd.DataFrame:
        """Internal helper to route reading operations to Pandas.

        Args:
            path(str): Target system string file location path.
            data_format(str): File format layout specification identifier.

        Returns:
            pd.DataFrame: Loaded Pandas DataFrame object.

        Raises:
            ValueError: If the file format layout is unsupported by Pandas.
        """
        if data_format == "parquet":
            return pd.read_parquet(path)
        elif data_format == "csv":
            return pd.read_csv(path)
        else:
            raise ValueError(f"Unsupported Pandas format: '{data_format}'.")

    def _load_with_spark(
        self, path: str, data_format: str, spark: Optional[Any]
    ) -> Any:
        """Internal worker to route reading operations to PySpark.

        Args:
            path(str): Target system string file location path.
            data_format(str): File format layout specification identifier.
            spark(Optional[Any]): Explicit user supplied SparkSession object.

        Returns:
            Any: Loaded PySpark DataFrame object.

        Raises:
            ImportError: If the PySpark dependency library is uninstalled.
            ValueError: If no valid operational Spark session context is found
                or if the layout format is unsupported.
        """
        if not HAS_SPARK:
            raise ImportError("PySpark is required but could not be imported.")

        session = spark
        if session is None:
            session = SparkSession.getActiveSession()

        if session is None:
            raise ValueError(
                "A valid SparkSession must be provided or active globally "
                "to load data using the 'spark' engine."
            )

        if data_format == "parquet":
            return session.read.parquet(path)
        elif data_format == "csv":
            return session.read.csv(path, header=True, inferSchema=True)
        else:
            raise ValueError(f"Unsupported Spark format: '{data_format}'.")
