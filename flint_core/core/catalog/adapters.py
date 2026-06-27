"""This module implements a dynamic, self-registering backend validation adapter layer."""

import abc
import logging
from typing import Any, ClassVar, Dict, Set

logger = logging.getLogger(__name__)


class AdapterRegistry:
    """Central registry driven by Inversion of Control to manage catalog verification adapters."""

    _REGISTRY: ClassVar[Dict[str, Any]] = {}

    @classmethod
    def resolve_adapter(cls, df: Any) -> Any:
        """Inspects the dataframe's module to dynamically dispatch the correct adapter.

        Follows the exact same semantic lookup pattern as EngineRegistry inside the core framework.

        Args:
            df: The incoming dynamic dataframe instance to evaluate.

        Returns:
            An instance of the mapped concrete catalog verification adapter.

        Raises:
            TypeError: If no adapter class matches the dataframe's original module path.
        """
        df_module = type(df).__module__.lower()

        for keyword, adapter_cls in cls._REGISTRY.items():
            if keyword in df_module:
                return adapter_cls()

        raise TypeError(
            f"No catalog verification adapter registered for dataframe module: '{type(df).__module__}'. "
            f"Supported validation backends: {list(cls._REGISTRY.keys())}"
        )


class BaseAdapter(abc.ABC):
    """Abstract Base Class orchestrating automated catalog registration via metaprogramming."""

    __slots__ = ()

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Automatically registers any inheriting adapter subclass into the central registry.

        Mirrors the exact runtime binding algorithm executed by BaseEngine.
        """
        super().__init_subclass__(**kwargs)
        keyword = cls.__name__.replace("Adapter", "").lower()
        if keyword:
            AdapterRegistry._REGISTRY[keyword] = cls

    @abc.abstractmethod
    def extract_columns(self, df: Any) -> Set[str]:
        """Abstract contract forcing concrete backends to normalize column extraction.

        Args:
            df: The verified valid concrete dataframe target instance.

        Returns:
            A set of strings tracking column signatures.
        """
        pass


# =============================================================================
# CONCRETE AGNOSTIC ADAPTERS (Zero top-level third-party library imports)
# =============================================================================


class PandasAdapter(BaseAdapter):
    """Agnostic adapter providing metadata extraction for local Pandas structures."""

    def extract_columns(self, df: Any) -> Set[str]:
        return set(df.columns.tolist())


class PysparkAdapter(BaseAdapter):
    """Agnostic adapter providing metadata extraction for distributed Spark structures."""

    def extract_columns(self, df: Any) -> Set[str]:
        return set(df.columns)
