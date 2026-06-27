"""Foundational infrastructure for engine registration and dynamic dispatching."""

from __future__ import annotations

import abc
import logging
from typing import Any, ClassVar, Dict, Generic, List, Type, TypeVar, Union

from flint_core.core.exceptions import ColumnValidationError, UnsupportedBackendError

logger = logging.getLogger(__name__)

DataFrameT = TypeVar("DataFrameT", bound=Any)


class EngineRegistry:
    """Central registry driven by Inversion of Control to manage concrete engines."""

    _REGISTRY: ClassVar[Dict[str, Type[BaseEngine[Any]]]] = {}

    @classmethod
    def resolve_engine(cls, df: Any) -> BaseEngine[Any]:
        """Inspects the dataframe's module to dynamically dispatch the correct engine
        instance.

        Args:
            df: The incoming dataframe instance to evaluate.

        Returns:
            BaseEngine[Any]: A stateless concrete engine instance mapped to the
            dataframe type.

        Raises:
            UnsupportedBackendError: If no engine class matches the dataframe's
            original module path.
        """
        df_module = type(df).__module__.lower()

        for keyword, engine_cls in cls._REGISTRY.items():
            if keyword in df_module:
                return engine_cls()

        raise UnsupportedBackendError(
            f"No execution engine registered for dataframe module: '{type(df).__module__}'. "  # noqa: E501
            f"Supported backends: {list(cls._REGISTRY.keys())}"
        )


class BaseEngine(abc.ABC, Generic[DataFrameT]):
    """Abstract Base Class orchestrating automated registration via metaprogramming."""

    __slots__ = ()

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Automatically registers any inheriting engine subclass into the central
        registry.

        Args:
            **kwargs: Arbitrary keyword arguments passed during subclass definition.
        """
        super().__init_subclass__(**kwargs)
        keyword = cls.__name__.replace("Engine", "").lower()
        if keyword:
            EngineRegistry._REGISTRY[keyword] = cls

    def _validate_columns(self, df: Any, columns: List[str]) -> None:
        """Universal utility to validate column existence safely across any dataframe
        type.

        Args:
            df: The incoming dataframe instance containing columns attributes.
            columns: A sequence of column names expected to exist within the dataframe.

        Raises:
            ColumnValidationError: If one or more requested columns are absent from the
            schema.
        """
        df_cols = getattr(df, "columns", [])
        missing = [col for col in columns if col not in df_cols]
        if missing:
            raise ColumnValidationError(
                f"The following required columns are missing from the DataFrame: {missing}"  # noqa: E501
            )

    @abc.abstractmethod
    def latest(self, df: DataFrameT, keys: List[str], order_by_col: str) -> DataFrameT:
        """Abstract implementation requirement for latest() protocol."""
        pass

    @abc.abstractmethod
    def first(self, df: DataFrameT, keys: List[str], order_by_col: str) -> DataFrameT:
        """Abstract implementation requirement for first() protocol."""
        pass

    @abc.abstractmethod
    def by_order(
        self,
        df: DataFrameT,
        keys: List[str],
        order_by_cols: List[str],
        ascending: Union[bool, List[bool]] = True,
    ) -> DataFrameT:
        """Abstract implementation requirement for by_order() protocol."""
        pass

    @abc.abstractmethod
    def combined(self, df: DataFrameT, keys: List[str]) -> DataFrameT:
        """Abstract implementation requirement for combined() protocol."""
        pass
