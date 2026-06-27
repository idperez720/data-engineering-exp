"""This module implements memory-optimized data entities with formats."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Set

from flint_core.core.catalog.adapters import AdapterRegistry
from flint_core.core.exceptions import ColumnValidationError


class ColumnDefinition:
    """Memory-isolated data model capturing column specifications."""

    __slots__ = ("name", "data_type", "description", "format", "timezone")

    def __init__(
        self,
        name: str,
        data_type: Optional[str] = None,
        description: Optional[str] = None,
        column_format: Optional[str] = None,  # Clears the built-in warning
        timezone: Optional[str] = None,
    ) -> None:
        """Initializes an optimized declarative column template."""
        self.name: str = name
        self.data_type: Optional[str] = data_type
        self.description: Optional[str] = description
        self.format: Optional[str] = column_format  # Exposed cleanly as .format
        self.timezone: Optional[str] = timezone


class DatasetConfiguration:
    """High-performance structural entity tracking comprehensive layouts."""

    __slots__ = (
        "name",
        "engine",
        "format",
        "storage_path",
        "columns",
        "metadata",
        "_column_names_set",
        "_catalog_ref",
    )

    def __init__(
        self,
        name: str,
        engine: str,
        data_format: str,
        storage_path: str,
        columns: List[ColumnDefinition],
        metadata: Dict[str, Any],
        catalog_ref: Optional[Any] = None,
    ) -> None:
        """Initializes an optimized declarative state bundle."""
        self.name: str = name
        self.engine: str = engine
        self.format: str = data_format
        self.storage_path: str = storage_path
        self.columns: List[ColumnDefinition] = columns
        self.metadata: Dict[str, Any] = metadata
        self._column_names_set: Set[str] = {col.name for col in columns}
        self._catalog_ref: Optional[Any] = catalog_ref

    @property
    def column_names(self) -> List[str]:
        """Preserves precise declaration matrix sequencing orders."""
        return [col.name for col in self.columns]

    def validate_schema(self, df: Any) -> bool:
        """Executes verification tests against catalog schemas."""
        adapter = AdapterRegistry.resolve_adapter(df)
        actual_cols = adapter.extract_columns(df)
        missing_cols = self._column_names_set - actual_cols

        if missing_cols:
            raise ColumnValidationError(
                f"Schema mismatch for '{self.name}'. Missing expected catalog columns: {list(missing_cols)}"
            )
        return True

    def load(self, spark: Optional[Any] = None) -> Any:
        """Triggers fluid domain-driven data loading via parent catalog."""
        if self._catalog_ref is None:
            raise RuntimeError(
                f"DatasetConfiguration entity '{self.name}' is detached from an active DataCatalog context."
            )
        return self._catalog_ref.load(self.name, spark=spark)
