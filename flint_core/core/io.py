"""This module implements metadata-driven, environment-agnostic data loading utilities for flint."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any, Optional

# Only standard library and isolated local core domain definitions
from flint_core.core.catalog.engine import DataCatalog
from flint_core.core.catalog.models import DatasetConfiguration

# Static analysis guard block. Only executed by IDEs/Mypy/Pylance.
# This eliminates top-level missing import warnings while retaining autocomplete.
if TYPE_CHECKING:
    import pandas as pd  # pyright: ignore[reportMissingModuleSource]
    from pyspark.sql import SparkSession  # type: ignore # noqa: F401


class DataLoader:
    """Handles dynamic loading of data elements mapping properties from strict catalog entities."""

    def __init__(self, catalog: Optional[DataCatalog] = None) -> None:
        """Initializes the DataLoader with a specific DataCatalog context reference.

        Args:
            catalog: An active repository pipeline tracking declarations.
        """
        self.catalog: DataCatalog = catalog if catalog is not None else DataCatalog()

    def load(self, dataset_name: str, spark: Optional[Any] = None) -> Any:
        """Loads a dataset from storage based on its catalog specification.

        Args:
            dataset_name: Unique identifier string of the dataset.
            spark: Active PySpark SparkSession instance. Optional if active globally.

        Returns:
            Any: Loaded Pandas or PySpark DataFrame object.

        Raises:
            KeyError: If mandatory keys like 'engine' or 'storage_path' are missing.
            ValueError: If an unsupported engine or format is provided.
            ImportError: If the target backend engine is requested but not installed.
        """
        dataset: DatasetConfiguration = self.catalog.get_dataset(dataset_name)
        path: str = dataset.storage_path

        # Convention resolution: anchor relative paths safely to project root
        if not os.path.isabs(path):
            path = os.path.normpath(os.path.join(self.catalog.project_root, path))

        if dataset.engine == "pandas":
            return self._load_with_pandas(path, dataset.format)
        elif dataset.engine == "spark":
            return self._load_with_spark(path, dataset.format, spark)
        else:
            raise ValueError(f"Unsupported storage processing platform backend: '{dataset.engine}'.")

    def _load_with_pandas(self, path: str, data_format: str) -> pd.DataFrame:
        """Routes stream extractions operations safely to local execution pools.

        Raises:
            ImportError: If pandas dependencies are missing from the runtime environment.
            ValueError: If the serialization layout format is unsupported.
        """
        try:
            # pylint: disable=import-outside-toplevel
            import pandas as pd  # type: ignore[import-not-found]
        except ImportError as e:
            raise ImportError(
                "The 'pandas' engine is required to load this dataset, but it is not installed. "
                "Please run 'poetry add pandas' or install flint with the [pandas] extra options."
            ) from e

        if data_format == "parquet":
            return pd.read_parquet(path)
        elif data_format == "csv":
            return pd.read_csv(path)
        else:
            raise ValueError(f"Unsupported Pandas file layout standard parsing rule: '{data_format}'.")

    def _load_with_spark(self, path: str, data_format: str, spark: Optional[Any]) -> Any:
        """Routes dataset evaluation execution plans safely to parallel Spark executors clusters.

        Raises:
            ImportError: If pyspark dependencies are missing from the runtime environment.
            ValueError: If no active SparkSession context can be resolved globally.
        """
        try:
            # pylint: disable=import-outside-toplevel
            from pyspark.sql import SparkSession  # type: ignore[import-not-found]
        except ImportError as e:
            raise ImportError(
                "The 'spark' engine is required to load this dataset, but it is not installed. "
                "Please run 'poetry add pyspark' or install flint with the [spark] extra options."
            ) from e

        session = spark if spark is not None else SparkSession.getActiveSession()

        if session is None:
            raise ValueError(
                "No distributed cluster session manager could be resolved globally inside active contexts. "
                "Please instantiate an active SparkSession before attempting distributed loading routines."
            )

        if data_format == "parquet":
            return session.read.parquet(path)
        elif data_format == "csv":
            return session.read.csv(path, header=True, inferSchema=True)
        else:
            raise ValueError(f"Unsupported distributed Big Data stream loading parameter: '{data_format}'.")
