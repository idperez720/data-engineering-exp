"""Metadata-driven, environment-agnostic data loading utilities for flint."""

from __future__ import annotations

import os
from typing import Any, Dict, Optional

from flint_core.core.base import EngineRegistry
from flint_core.core.catalog.models import DatasetConfiguration


class DataLoader:
    """Handles dynamic loading of data elements by delegating to engines."""

    def __init__(self, catalog: Optional[Any] = None) -> None:
        """Initializes the DataLoader with a specific DataCatalog reference."""
        from flint_core.core.catalog.engine import DataCatalog

        self.catalog: DataCatalog = catalog if catalog is not None else DataCatalog()

    def load(
        self,
        dataset_name: str,
        spark: Optional[Any] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Loads a dataset from storage utilizing dynamic engine dispatching."""
        dataset: DatasetConfiguration = self.catalog.get_dataset(dataset_name)
        path: str = dataset.storage_path

        if not os.path.isabs(path):
            path = os.path.normpath(os.path.join(self.catalog.project_root, path))

        # Safely extract and merge nested catalog options with runtime overrides
        raw_catalog_opts = dataset.metadata.get("options", {})
        catalog_options = raw_catalog_opts if isinstance(raw_catalog_opts, dict) else {}
        runtime_options = options if options is not None else {}

        combined_metadata = dataset.metadata.copy()
        combined_metadata["options"] = {
            **catalog_options,
            **runtime_options,
        }

        # Pure Inversion of Control pattern execution
        engine = EngineRegistry.get_engine(dataset.engine)
        return engine.load(
            path=path,
            data_format=dataset.format,
            columns=dataset.columns,
            metadata=combined_metadata,
            spark=spark,
        )
