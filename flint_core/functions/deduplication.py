"""Deduplication feature facade and protocol interface."""

from __future__ import annotations

import logging
from typing import Any, List, Protocol, TypeVar, Union, cast, overload

from flint_core.core.base import EngineRegistry

logger = logging.getLogger(__name__)

DataFrameT = TypeVar("DataFrameT", bound=Any)


class DeduplicationProtocol(Protocol[DataFrameT]):
    """Structural protocol defining the contract for deduplication capabilities."""

    def latest(
        self, df: DataFrameT, keys: List[str], order_by_col: str
    ) -> DataFrameT: ...
    def first(
        self, df: DataFrameT, keys: List[str], order_by_col: str
    ) -> DataFrameT: ...
    def by_order(
        self,
        df: DataFrameT,
        keys: List[str],
        order_by_cols: List[str],
        ascending: Union[bool, List[bool]],
    ) -> DataFrameT: ...
    def combined(self, df: DataFrameT, keys: List[str]) -> DataFrameT: ...


def latest(df: DataFrameT, keys: List[str], order_by_col: str) -> DataFrameT:
    """Extracts the most recent record per business key group.

    Args:
        df: Input dataframe instance (e.g., Pandas, Spark, or Polars DataFrame).
        keys: List of target columns representing the deterministic business key.
        order_by_col: Column name used to determine chronological recency.

    Returns:
        DataFrameT: A deduplicated dataframe containing only the latest rows.
    """
    engine = cast(DeduplicationProtocol[DataFrameT], EngineRegistry.resolve_engine(df))
    return engine.latest(df, keys, order_by_col)


def first(df: DataFrameT, keys: List[str], order_by_col: str) -> DataFrameT:
    """Extracts the earliest chronological record per business key group.

    Args:
        df: Input dataframe instance (e.g., Pandas, Spark, or Polars DataFrame).
        keys: List of target columns representing the deterministic business key.
        order_by_col: Column name used to determine chronological priority.

    Returns:
        DataFrameT: A deduplicated dataframe containing only the earliest rows.
    """
    engine = cast(DeduplicationProtocol[DataFrameT], EngineRegistry.resolve_engine(df))
    return engine.first(df, keys, order_by_col)


@overload
def by_order(
    df: DataFrameT, keys: List[str], order_by_cols: List[str], ascending: bool = True
) -> DataFrameT: ...


@overload
def by_order(
    df: DataFrameT, keys: List[str], order_by_cols: List[str], ascending: List[bool]
) -> DataFrameT: ...


def by_order(
    df: DataFrameT,
    keys: List[str],
    order_by_cols: List[str],
    ascending: Union[bool, List[bool]] = True,
) -> DataFrameT:
    """Deduplicates rows by sorting through multiple custom column criteria.

    Args:
        df: Input dataframe instance (e.g., Pandas, Spark, or Polars DataFrame).
        keys: List of target columns representing the deterministic business key.
        order_by_cols: Sequence of columns used to prioritize the row retention sort
        order.
        ascending: Sorting direction configuration matching the order_by_cols length or
        a global flag.

    Returns:
        DataFrameT: The deduplicated subset dataframe.
    """
    engine = cast(DeduplicationProtocol[DataFrameT], EngineRegistry.resolve_engine(df))
    return engine.by_order(df, keys, order_by_cols, ascending)


def combined(df: DataFrameT, keys: List[str]) -> DataFrameT:
    """Stitches rows together by compacting nulls into a single golden record.

    Args:
        df: Input dataframe instance (e.g., Pandas, Spark, or Polars DataFrame).
        keys: List of target columns representing the deterministic business key.

    Returns:
        DataFrameT: Consolidated dataframe with aggregated records ignoring inner null
        values.
    """
    engine = cast(DeduplicationProtocol[DataFrameT], EngineRegistry.resolve_engine(df))
    return engine.combined(df, keys)
