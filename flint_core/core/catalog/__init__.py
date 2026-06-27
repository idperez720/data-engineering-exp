"""Data catalog subpackage exposing high-performance semantic configuration utilities."""

# PEP 484 explicit re-exports to unlock perfect IDE autocomplete and type safety.
from flint_core.core.catalog.adapters import AdapterRegistry, BaseAdapter
from flint_core.core.catalog.engine import DataCatalog
from flint_core.core.catalog.models import ColumnDefinition, DatasetConfiguration
from flint_core.core.exceptions import CatalogParseError

__all__ = [
    "DataCatalog",
    "CatalogParseError",
    "DatasetConfiguration",
    "ColumnDefinition",
    "AdapterRegistry",
    "BaseAdapter",
]
