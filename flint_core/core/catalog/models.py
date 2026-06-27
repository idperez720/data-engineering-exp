"""This module implements memory-optimized data entities for the flint catalog."""

from typing import Any, Dict, List, Set

from flint_core.core.catalog.adapters import AdapterRegistry
from flint_core.core.exceptions import ColumnValidationError


class ColumnDefinition:
    """Memory-isolated data model capturing individual dataset columns specifications."""

    __slots__ = ("name", "data_type", "description")

    def __init__(self, name: str, data_type: str | None = None, description: str | None = None) -> None:
        """Initializes a specific column validation mapping template."""
        self.name: str = name
        self.data_type: str | None = data_type
        self.description: str | None = description


class DatasetConfiguration:
    """High-performance structural entity tracking comprehensive entity rules layouts."""

    __slots__ = ("name", "engine", "format", "storage_path", "columns", "metadata", "_column_names_set")

    def __init__(
        self,
        name: str,
        engine: str,
        data_format: str,
        storage_path: str,
        columns: List[ColumnDefinition],
        metadata: Dict[str, Any],
    ) -> None:
        """Initializes an optimized declarative state configuration bundle."""
        self.name: str = name
        self.engine: str = engine
        self.format: str = data_format
        self.storage_path: str = storage_path
        self.columns: List[ColumnDefinition] = columns
        self.metadata: Dict[str, Any] = metadata
        self._column_names_set: Set[str] = {col.name for col in columns}

    @property
    def column_names(self) -> List[str]:
        """Preserves precise declaration matrix column sequencing orders."""
        return [col.name for col in self.columns]

    def validate_schema(self, df: Any) -> bool:
        """Executes verification tests leveraging the synchronized AdapterRegistry pipeline.

        Args:
            df: An arbitrary object entity passing dynamic validation adapters.

        Returns:
            True if schema rules find perfect matches.

        Raises:
            ColumnValidationError: If anomalous deviations are identified.
        """
        adapter = AdapterRegistry.resolve_adapter(df)
        actual_cols = adapter.extract_columns(df)
        missing_cols = self._column_names_set - actual_cols

        if missing_cols:
            raise ColumnValidationError(
                f"Schema mismatch for '{self.name}'. Missing expected catalog columns: {list(missing_cols)}"
            )
        return True
