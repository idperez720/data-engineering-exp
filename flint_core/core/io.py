"""Metadata-driven, environment-agnostic data loading and saving utilities."""

from __future__ import annotations

from pathlib import Path
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
        raw_path = dataset.storage_path

        # Resolution utilizing object-oriented Path conventions
        file_path = Path(raw_path)
        if not file_path.is_absolute():
            file_path = (self.catalog.project_root / file_path).resolve()
        else:
            file_path = file_path.resolve()

        # Merge static catalog options with dynamic runtime overrides
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
            path=str(file_path),
            data_format=dataset.format,
            columns=dataset.columns,
            metadata=combined_metadata,
            spark=spark,
        )


class DataSaver:
    """Handles dynamic saving of data elements by delegating to engines."""

    def __init__(self, catalog: Optional[Any] = None) -> None:
        """Initializes the DataSaver with a specific DataCatalog reference."""
        from flint_core.core.catalog.engine import DataCatalog

        self.catalog: DataCatalog = catalog if catalog is not None else DataCatalog()

    def save(
        self,
        df: Any,
        dataset_name: str,
        mode: str = "error",
        spark: Optional[Any] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Saves a dataframe utilizing dynamic engine dispatching."""
        dataset = self.catalog.get_dataset(dataset_name)
        raw_path = dataset.storage_path

        # Resolution utilizing object-oriented Path conventions
        file_path = Path(raw_path)
        if not file_path.is_absolute():
            file_path = (self.catalog.project_root / file_path).resolve()
        else:
            file_path = file_path.resolve()

        # Merge static catalog options with dynamic runtime overrides
        raw_catalog_opts = dataset.metadata.get("options", {})
        catalog_options = raw_catalog_opts if isinstance(raw_catalog_opts, dict) else {}
        runtime_options = options if options is not None else {}

        combined_metadata = dataset.metadata.copy()
        combined_metadata["options"] = {
            **catalog_options,
            **runtime_options,
        }

        # Dynamic routing leveraging EngineRegistry
        engine = EngineRegistry.get_engine(dataset.engine)
        engine.save(
            df=df,
            path=str(file_path),
            data_format=dataset.format,
            mode=mode,
            metadata=combined_metadata,
            spark=spark,
        )
