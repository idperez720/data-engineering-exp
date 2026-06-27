"""Pandas implementation mixin for deduplication operations."""

from __future__ import annotations

import logging
from typing import List, Union

import pandas as pd

logger = logging.getLogger(__name__)


class PandasDeduplicationMixin:
    """Provides deduplication capabilities to the main Pandas engine."""

    def latest(self, df: pd.DataFrame, keys: List[str], order_by_col: str) -> pd.DataFrame:
        """Extracts the most recent record per business key group via Pandas grouping.

        Args:
            df: Target Pandas DataFrame to process.
            keys: Columns making up the unique business key.
            order_by_col: Metric column used to identify chronological sorting order.

        Returns:
            pd.DataFrame: A filtered Pandas DataFrame holding the last matching row
            states.
        """
        self._validate_columns(df, keys + [order_by_col])  # type: ignore[attr-defined]
        logger.debug("Executing Pandas deduplication via 'latest'")
        return df.sort_values(by=order_by_col, ascending=True).groupby(keys, as_index=False).last()

    def first(self, df: pd.DataFrame, keys: List[str], order_by_col: str) -> pd.DataFrame:
        """Extracts the earliest chronological record per business key group via Pandas
        grouping.

        Args:
            df: Target Pandas DataFrame to process.
            keys: Columns making up the unique business key.
            order_by_col: Metric column used to identify chronological sorting order.

        Returns:
            pd.DataFrame: A filtered Pandas DataFrame holding the first matching row
            states.
        """
        self._validate_columns(df, keys + [order_by_col])  # type: ignore[attr-defined]
        logger.debug("Executing Pandas deduplication via 'first'")
        return df.sort_values(by=order_by_col, ascending=True).groupby(keys, as_index=False).first()

    def by_order(
        self,
        df: pd.DataFrame,
        keys: List[str],
        order_by_cols: List[str],
        ascending: Union[bool, List[bool]] = True,
    ) -> pd.DataFrame:
        """Deduplicates rows by sorting through multiple custom column criteria using
        Pandas values sort.

        Args:
            df: Target Pandas DataFrame to process.
            keys: Columns making up the unique business key.
            order_by_cols: Sequence of priority columns used for sorting operations.
            ascending: Orientation mapping corresponding to order_by_cols sorting
            instructions.

        Returns:
            pd.DataFrame: The prioritized deduplicated Pandas DataFrame subset.

        Raises:
            ValueError: If the iterable ascending parameters size mismatches the sorted
            columns sequence.
        """
        self._validate_columns(df, keys + order_by_cols)  # type: ignore[attr-defined]
        if isinstance(ascending, list) and len(ascending) != len(order_by_cols):
            raise ValueError(
                f"Length of 'ascending' list ({len(ascending)}) must match "
                f"length of 'order_by_cols' ({len(order_by_cols)})."
            )
        logger.debug("Executing Pandas deduplication via custom 'by_order'")
        return df.sort_values(by=order_by_cols, ascending=ascending).groupby(keys, as_index=False).first()

    def combined(self, df: pd.DataFrame, keys: List[str]) -> pd.DataFrame:
        """Stitches rows together by compacting nulls into a single golden record using
        native Pandas logic.

        Args:
            df: Target Pandas DataFrame to process.
            keys: Columns making up the unique business key.

        Returns:
            pd.DataFrame: A consolidated Pandas DataFrame tracking combined row values.
        """
        self._validate_columns(df, keys)  # type: ignore[attr-defined]
        logger.debug("Executing Pandas deduplication via 'combined'")
        return df.groupby(keys, as_index=False).first()
