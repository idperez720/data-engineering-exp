"""This module implements the concurrent orchestrator engine managing data catalogs."""

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import RLock
from typing import Any, Dict, List, Optional, Union

import yaml

from flint_core.core.catalog.descriptors import DatasetDescriptor
from flint_core.core.catalog.models import ColumnDefinition, DatasetConfiguration
from flint_core.core.exceptions import CatalogParseError

logger = logging.getLogger(__name__)


class DataCatalog:
    """Enterprise-grade declarative file parsing mapping synchronizer control center."""

    _executor: ThreadPoolExecutor = ThreadPoolExecutor(thread_name_prefix="FlintCatalogWorker")

    def __init__(self, catalog_path: Optional[Union[str, Path]] = None) -> None:
        """Initializes a concurrent, thread-safe project layout engine parser cluster."""
        self._lock: RLock = RLock()
        self.project_root: Path = Path()
        self._datasets: Dict[str, DatasetConfiguration] = {}

        if catalog_path is None:
            resolved_path = self._discover_catalog_path()
        else:
            resolved_path = Path(catalog_path).resolve()
            self._find_project_root_from_path(resolved_path)

        if not resolved_path.exists():
            raise FileNotFoundError(f"Catalog source path not found at: {resolved_path}")

        self.reload_catalog(resolved_path)

    def __getitem__(self, dataset_name: str) -> DatasetConfiguration:
        """Provides high-performance key-based square bracket index item access."""
        return self.get_dataset(dataset_name)

    def __contains__(self, dataset_name: str) -> bool:
        """Allows conditional containment query execution tests over active clusters."""
        with self._lock:
            return dataset_name in self._datasets

    @property
    def dataset_names(self) -> List[str]:
        """Isolates the current metadata keys as an independent immutable array."""
        with self._lock:
            return list(self._datasets.keys())

    def get_dataset(self, dataset_name: str) -> DatasetConfiguration:
        """Fetches complete models ensuring strict state execution parameters."""
        with self._lock:
            if dataset_name not in self._datasets:
                raise KeyError(f"Dataset '{dataset_name}' missing from catalog.")
            return self._datasets[dataset_name]

    def load(self, dataset_name: str, spark: Optional[Any] = None) -> Any:
        """Unified framework facade entrypoint to load data assets directly from the catalog.

        Unlocks the traditional interface style: catalog.load("sample_table")
        Leverages lazy import execution blocks to prevent circular packages compilation errors.

        Args:
            dataset_name: Unique identity string registry key target.
            spark: Optional distributed active SparkSession execution engine runner.

        Returns:
            Any: An evaluated Pandas or PySpark DataFrame matrix.
        """
        # Elite lazy import execution block to bypass compilation circular links
        from flint_core.core.io import DataLoader

        with self._lock:
            # Reuses the exact infrastructure parameters avoiding context fragmentation
            loader = DataLoader(catalog=self)
            return loader.load(dataset_name, spark=spark)

    def reload_catalog(self, path: Path) -> None:
        """Rebuilds operational configurations safely separating threading memory boundaries."""
        with self._lock:
            self._datasets.clear()
            self._load_catalog_sources(path)
            self._bind_dynamic_descriptors()

    async def reload_catalog_async(self, path: Path) -> None:
        """Executes complete updates inside isolated worker thread loops non-blockingly."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self._executor, self.reload_catalog, path)

    def _bind_dynamic_descriptors(self) -> None:
        """Appends descriptor references runtime attributes pipelines maps."""
        for name in self._datasets:
            if not hasattr(self, name):
                setattr(self.__class__, name, DatasetDescriptor(name))

    def _discover_catalog_path(self) -> Path:
        current_dir = Path.cwd().resolve()
        for parent in [current_dir] + list(current_dir.parents):
            if (parent / "pyproject.toml").exists():
                self.project_root = parent
                return parent / "conf" / "catalog"

        raise FileNotFoundError(
            "Configuration file (pyproject.toml) could not be located. "
            "Please run 'flint init' to establish a valid project layout."
        )

    def _find_project_root_from_path(self, path: Path) -> None:
        start_dir = path if path.is_dir() else path.parent
        for parent in [start_dir] + list(start_dir.parents):
            if (parent / "pyproject.toml").exists():
                self.project_root = parent
                return
        self.project_root = start_dir

    def _load_catalog_sources(self, path: Path) -> None:
        if path.is_file():
            if path.suffix in (".yml", ".yaml"):
                self._parse_file(path)
            return

        for file_path in path.rglob("*"):
            if file_path.is_file() and file_path.suffix in (".yml", ".yaml"):
                self._parse_file(file_path)

    def _parse_file(self, file_path: Path) -> None:
        try:
            with open(file_path, "r", encoding="utf-8") as stream:
                content = yaml.safe_load(stream)
        except yaml.YAMLError as e:
            logger.error("Failed to parse YAML file at %s: %s", file_path, e)
            raise CatalogParseError(f"Syntax error in catalog file '{file_path.name}': {e}") from e

        if content and isinstance(content, dict):
            for dataset_name, dataset_body in content.items():
                if not isinstance(dataset_body, dict):
                    continue

                engine = dataset_body.get("engine")
                data_format = dataset_body.get("format")
                storage_path = dataset_body.get("storage_path")

                if not engine or not data_format or not storage_path:
                    raise KeyError(
                        f"Dataset '{dataset_name}' metadata must contain 'engine', 'format', and 'storage_path'."
                    )

                raw_columns = dataset_body.get("columns", []) or []
                column_definitions: List[ColumnDefinition] = []

                for col in raw_columns:
                    if isinstance(col, dict) and "name" in col:
                        column_definitions.append(
                            ColumnDefinition(
                                name=col["name"],
                                data_type=col.get("type"),
                                description=col.get("description"),
                            )
                        )

                metadata_payload = {
                    k: v for k, v in dataset_body.items() if k not in ("columns", "engine", "format", "storage_path")
                }

                with self._lock:
                    if dataset_name in self._datasets:
                        logger.warning(
                            "Duplicate dataset definitions detected and overwritten: %s found in '%s'",
                            dataset_name,
                            file_path.name,
                        )

                    # Pass self as catalog_ref to establish the live context dependency link
                    self._datasets[dataset_name] = DatasetConfiguration(
                        name=dataset_name,
                        engine=engine,
                        data_format=data_format,
                        storage_path=storage_path,
                        columns=column_definitions,
                        metadata=metadata_payload,
                        catalog_ref=self,
                    )
