"""This module implements decentralized data catalog engines for dex."""

import os
from typing import Any, Dict, List, cast

import yaml


class DataCatalog:
    """Parses and consolidates declarative configurations from files or directories.

    Scans catalog definitions recursively, allowing user architectures to be
    split into mini-YAML files or kept within a single configuration layout.
    """

    def __init__(self, catalog_path: str):
        """Initializes the DataCatalog by scanning files or configuration paths.

        Args:
            catalog_path(str): System directory path or individual file string
                pointing to the declarative catalog source definitions.

        Returns:
            None: Initializes the class instance.
        """
        if not os.path.exists(catalog_path):
            raise FileNotFoundError(f"Catalog source path not found at: {catalog_path}")

        # Strongly typed nested dictionary prevents Any leakage down the line
        self._datasets: Dict[str, Dict[str, Any]] = {}
        self._load_catalog_sources(catalog_path)

    @property
    def dataset_names(self) -> List[str]:
        """Exposes the list of all dataset identifiers present in the catalog.

        Args:

        Returns:
            List[str]: List of string names for all loaded datasets.
        """
        return list(self._datasets.keys())

    def get_dataset_metadata(self, dataset_name: str) -> Dict[str, Any]:
        """Retrieves core metadata mapping configurations for a target dataset.

        Args:
            dataset_name(str): Unique identifier string of the dataset.

        Returns:
            Dict[str, Any]: Dictionary containing properties like description,
                format, primary_keys, and storage_path.

        Raises:
            KeyError: If the target dataset name is missing from the catalog.
        """
        if dataset_name not in self._datasets:
            raise KeyError(f"Dataset '{dataset_name}' missing from catalog.")

        # Properly infers type as Dict[str, Any] matching function signature
        meta = self._datasets[dataset_name].copy()
        meta.pop("columns", None)
        return meta

    def get_column_names(self, dataset_name: str) -> List[str]:
        """Extracts the expected list of column names for a target dataset.

        Args:
            dataset_name(str): Unique identifier string of the dataset.

        Returns:
            List[str]: Ordered list of strings representing expected columns.

        Raises:
            KeyError: If the target dataset name is missing from the catalog.
        """
        if dataset_name not in self._datasets:
            raise KeyError(f"Dataset '{dataset_name}' missing from catalog.")

        columns_spec = self._datasets[dataset_name].get("columns", [])
        return [col["name"] for col in columns_spec]

    def validate_schema_presence(self, df: Any, dataset_name: str) -> bool:
        """Validates that all columns defined in the catalog exist in the dataframe.

        Args:
            df(Any): Active input Pandas or PySpark DataFrame object.
            dataset_name(str): Catalog identifier string of the dataset target.

        Returns:
            bool: True if all expected columns are present, raises ValueError
                otherwise.

        Raises:
            TypeError: If the input dataframe structure is not supported.
            ValueError: If a column mismatch is discovered against the catalog.
        """
        expected_cols = set(self.get_column_names(dataset_name))

        df_type = type(df).__name__
        if df_type == "DataFrame" and "pandas" in type(df).__module__:
            actual_cols = set(df.columns.tolist())
        elif df_type == "DataFrame" and "pyspark" in type(df).__module__:
            actual_cols = set(df.columns)
        else:
            raise TypeError("Unsupported DataFrame type for validation.")

        missing_cols = expected_cols - actual_cols
        if missing_cols:
            raise ValueError(
                f"Schema mismatch for '{dataset_name}'. "
                f"Missing expected catalog columns: {list(missing_cols)}"
            )

        return True

    def _load_catalog_sources(self, path: str) -> None:
        """Internal worker to process and parse path targets recursively.

        Args:
            path(str): Targeting file path or folder configuration directory.

        Returns:
            None: Populates internal dictionary instances.
        """
        if os.path.isfile(path):
            if path.endswith((".yml", ".yaml")):
                self._parse_file(path)
            return

        for root, _, files in os.walk(path):
            for file in files:
                if file.endswith((".yml", ".yaml")):
                    full_path = os.path.join(root, file)
                    self._parse_file(full_path)

    def _parse_file(self, file_path: str) -> None:
        """Parses an individual YAML catalog file and updates target states.

        Args:
            file_path(str): Exact system string location pointing to file.

        Returns:
            None: Updates the core datasets dictionary data states.
        """
        # Explicit encoding set to utf-8 ensures cross-platform parsing safety
        with open(file_path, "r", encoding="utf-8") as stream:
            content = yaml.safe_load(stream)

        if content and isinstance(content, dict):
            typed_content = cast(Dict[str, Dict[str, Any]], content)
            self._datasets.update(typed_content)
