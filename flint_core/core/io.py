"""Metadata-driven, environment-agnostic data loading utilities for flint."""

from __future__ import annotations

import os
from typing import Any, Optional

from flint_core.core.base import EngineRegistry
from flint_core.core.catalog.models import DatasetConfiguration


class DataLoader:
    """Handles dynamic loading of data elements by delegating to engines."""

    def __init__(self, catalog: Optional[Any] = None) -> None:
        """Initializes the DataLoader with a specific DataCatalog reference."""
        from flint_core.core.catalog.engine import DataCatalog

        self.catalog: DataCatalog = catalog if catalog is not None else DataCatalog()

    def load(self, dataset_name: str, spark: Optional[Any] = None) -> Any:
        """Loads a dataset from storage utilizing dynamic engine dispatching."""
        dataset: DatasetConfiguration = self.catalog.get_dataset(dataset_name)
        path: str = dataset.storage_path

        if not os.path.isabs(path):
            path = os.path.normpath(os.path.join(self.catalog.project_root, path))

        # Pure Inversion of Control pattern execution
        engine = EngineRegistry.get_engine(dataset.engine)
        return engine.load(
            path=path,
            data_format=dataset.format,
            columns=dataset.columns,
            spark=spark,
        )
