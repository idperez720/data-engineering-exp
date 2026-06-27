"""This module implements decentralized data catalog engines for flint."""

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Union, cast

import yaml

# Initialize logger for the library
logger = logging.getLogger(__name__)

# Static type checking support for DataFrames without forcing runtime imports
if TYPE_CHECKING:
    import pandas as pd
    from pyspark.sql import DataFrame as SparkDataFrame

    DataFrameType = Union["pd.DataFrame", "SparkDataFrame"]


class CatalogParseError(Exception):
    """Raised when a YAML catalog file cannot be parsed due to syntax errors."""

    pass


class DataCatalog:
    """Parses and consolidates declarative configurations from files or directories.

    Scans catalog definitions recursively, allowing user architectures to be
    split into mini-YAML files or kept within a single configuration layout.
    """

    def __init__(self, catalog_path: Optional[Union[str, Path]] = None) -> None:
        """Initializes the DataCatalog by scanning files or configuration paths.

        If no path is provided, it automatically seeks upwards from the current
        working directory to find the standard 'conf/catalog' directory based on
        the pyproject.toml root anchor file.

        Args:
            catalog_path: System directory path or file pointing to catalog definitions.

        Raises:
            FileNotFoundError: If the resolved path or pyproject.toml does not exist.
        """
        self.project_root: Path = Path()
        self._datasets: dict[str, dict[str, Any]] = {}

        # Resolve the catalog path safely using pathlib
        if catalog_path is None:
            resolved_path = self._discover_catalog_path()
        else:
            resolved_path = Path(catalog_path).resolve()
            self._find_project_root_from_path(resolved_path)

        if not resolved_path.exists():
            raise FileNotFoundError(
                f"Catalog source path not found at: {resolved_path}"
            )

        self._load_catalog_sources(resolved_path)

    @property
    def dataset_names(self) -> list[str]:
        """Exposes the list of all dataset identifiers present in the catalog."""
        return list(self._datasets.keys())

    def get_dataset_metadata(self, dataset_name: str) -> dict[str, Any]:
        """Retrieves core metadata mapping configurations for a target dataset.

        Args:
            dataset_name: Unique identifier string of the dataset.

        Returns:
            A dictionary containing properties like description, format, etc.

        Raises:
            KeyError: If the target dataset name is missing from the catalog.
        """
        if dataset_name not in self._datasets:
            raise KeyError(f"Dataset '{dataset_name}' missing from catalog.")

        meta = self._datasets[dataset_name].copy()
        meta.pop("columns", None)
        return meta

    def get_column_names(self, dataset_name: str) -> list[str]:
        """Extracts the expected list of column names for a target dataset.

        Args:
            dataset_name: Unique identifier string of the dataset.

        Returns:
            Ordered list of strings representing expected columns.

        Raises:
            KeyError: If the target dataset name is missing from the catalog.
        """
        if dataset_name not in self._datasets:
            raise KeyError(f"Dataset '{dataset_name}' missing from catalog.")

        columns_spec = self._datasets[dataset_name].get("columns", []) or []
        return [col["name"] for col in columns_spec if "name" in col]

    def validate_schema_presence(self, df: "DataFrameType", dataset_name: str) -> bool:
        """Validates that all columns defined in the catalog exist in the dataframe.

        Args:
            df: Active input Pandas or PySpark DataFrame object.
            dataset_name: Catalog identifier string of the dataset target.

        Returns:
            True if all expected columns are present.

        Raises:
            TypeError: If the input dataframe structure is not supported.
            ValueError: If a column mismatch is discovered against the catalog.
        """
        expected_cols = set(self.get_column_names(dataset_name))
        df_type = type(df).__name__
        module_type = type(df).__module__

        if df_type == "DataFrame" and "pandas" in module_type:
            actual_cols = set(df.columns.tolist())  # type: ignore
        elif df_type == "DataFrame" and "pyspark" in module_type:
            actual_cols = set(df.columns)  # type: ignore
        else:
            raise TypeError(
                f"Unsupported DataFrame type '{type(df)}' for validation. "
                "Only Pandas and PySpark DataFrames are supported."
            )

        missing_cols = expected_cols - actual_cols
        if missing_cols:
            raise ValueError(
                f"Schema mismatch for '{dataset_name}'. "
                f"Missing expected catalog columns: {list(missing_cols)}"
            )

        return True

    def _discover_catalog_path(self) -> Path:
        """Traverses parent directories upwards to locate the pyproject.toml file."""
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
        """Safely searches upwards from an explicit path to detect the project root."""
        start_dir = path if path.is_dir() else path.parent
        for parent in [start_dir] + list(start_dir.parents):
            if (parent / "pyproject.toml").exists():
                self.project_root = parent
                return
        # Fallback if no pyproject.toml is found near the explicit path
        self.project_root = start_dir

    def _load_catalog_sources(self, path: Path) -> None:
        """Internal worker to process and parse path targets recursively."""
        if path.is_file():
            if path.suffix in (".yml", ".yaml"):
                self._parse_file(path)
            return

        # Modern alternative to os.walk using pathlib glob pattern recursively
        for file_path in path.rglob("*"):
            if file_path.is_file() and file_path.suffix in (".yml", ".yaml"):
                self._parse_file(file_path)

    def _parse_file(self, file_path: Path) -> None:
        """Parses an individual YAML catalog file and updates target states.

        Raises:
            CatalogParseError: If the YAML file contains structural or syntax errors.
        """
        try:
            with open(file_path, "r", encoding="utf-8") as stream:
                content = yaml.safe_load(stream)
        except yaml.YAMLError as e:
            logger.error("Failed to parse YAML file at %s: %s", file_path, e)
            raise CatalogParseError(
                f"Syntax error in catalog file '{file_path.name}': {e}"
            ) from e

        if content and isinstance(content, dict):
            typed_content = cast(dict[str, dict[str, Any]], content)

            # Check for duplicate dataset definitions across files before updating
            duplicates = set(typed_content.keys()) & set(self._datasets.keys())
            if duplicates:
                logger.warning(
                    "Duplicate dataset definitions detected and overwritten: %s found in '%s'", # noqa
                    duplicates,
                    file_path.name,
                )

            self._datasets.update(typed_content)
