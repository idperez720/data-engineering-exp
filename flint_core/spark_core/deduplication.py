"""Spark implementation mixin for high-performance windowed deduplication operations."""

from __future__ import annotations

import logging
from typing import List, Union

import pyspark.sql.functions as F
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import Window

logger = logging.getLogger(__name__)


class SparkDeduplicationMixin:
    """Provides distributed deduplication capabilities to the main Spark engine."""

    def latest(self, df: SparkDataFrame, keys: List[str], order_by_col: str) -> SparkDataFrame:
        """Extracts the most recent record per business key group using a row_number
        Spark Window.

        Args:
            df: High-volume PySpark DataFrame pipeline reference.
            keys: Columns representing the partitioned business boundary keys.
            order_by_col: Metric column driving the descending priority strategy inside
            the window specification.

        Returns:
            SparkDataFrame: The single row per partition target Spark lazy evaluation
            plan.
        """
        self._validate_columns(df, keys + [order_by_col])  # type: ignore[attr-defined]
        logger.debug("Executing PySpark windowed deduplication via 'latest'")

        window_spec = Window.partitionBy([F.col(k) for k in keys]).orderBy(F.col(order_by_col).desc())
        return (
            df.withColumn("_row_num", F.row_number().over(window_spec)).filter(F.col("_row_num") == 1).drop("_row_num")
        )

    def first(self, df: SparkDataFrame, keys: List[str], order_by_col: str) -> SparkDataFrame:
        """Extracts the earliest chronological record per business key group using a
        row_number Spark Window.

        Args:
            df: High-volume PySpark DataFrame pipeline reference.
            keys: Columns representing the partitioned business boundary keys.
            order_by_col: Metric column driving the ascending priority strategy inside
            the window specification.

        Returns:
            SparkDataFrame: The single row per partition target Spark lazy evaluation
            plan.
        """
        self._validate_columns(df, keys + [order_by_col])  # type: ignore[attr-defined]
        logger.debug("Executing PySpark windowed deduplication via 'first'")

        window_spec = Window.partitionBy([F.col(k) for k in keys]).orderBy(F.col(order_by_col).asc())
        return (
            df.withColumn("_row_num", F.row_number().over(window_spec)).filter(F.col("_row_num") == 1).drop("_row_num")
        )

    def by_order(
        self,
        df: SparkDataFrame,
        keys: List[str],
        order_by_cols: List[str],
        ascending: Union[bool, List[bool]] = True,
    ) -> SparkDataFrame:
        """Deduplicates rows by sorting through multiple custom column criteria within a
        native Spark partition schema.

        Args:
            df: High-volume PySpark DataFrame pipeline reference.
            keys: Columns representing the partitioned business boundary keys.
            order_by_cols: Sequence of priority sorting target columns tracking
            distributed partition ranks.
            ascending: Boolean configurations matching the sorting requirements index.

        Returns:
            SparkDataFrame: The ranked and deduplicated lazy execution plan dataframe.

        Raises:
            ValueError: If the list configuration mapping orientation parameters
            mismatch the ordering elements size.
        """
        self._validate_columns(df, keys + order_by_cols)  # type: ignore[attr-defined]

        if isinstance(ascending, list) and len(ascending) != len(order_by_cols):
            raise ValueError(
                f"Length of 'ascending' list ({len(ascending)}) must match "
                f"length of 'order_by_cols' ({len(order_by_cols)})."
            )

        logger.debug(
            "Executing PySpark windowed deduplication via custom multi-column 'by_order'"  # noqa: E501
        )
        asc_list = [ascending] * len(order_by_cols) if isinstance(ascending, bool) else ascending

        order_exprs = [
            F.col(col).asc() if asc_dir else F.col(col).desc() for col, asc_dir in zip(order_by_cols, asc_list)
        ]

        window_spec = Window.partitionBy([F.col(k) for k in keys]).orderBy(*order_exprs)
        return (
            df.withColumn("_row_num", F.row_number().over(window_spec)).filter(F.col("_row_num") == 1).drop("_row_num")
        )

    def combined(self, df: SparkDataFrame, keys: List[str]) -> SparkDataFrame:
        """Stitches rows together by compacting nulls into a single golden record
        utilizing Spark aggregations.

        Args:
            df: High-volume PySpark DataFrame pipeline reference.
            keys: Columns representing the partitioned business boundary keys.

        Returns:
            SparkDataFrame: Structural consolidated aggregation plan omitting internal
            null elements.
        """
        self._validate_columns(df, keys)  # type: ignore[attr-defined]
        logger.debug("Executing PySpark structural aggregation via 'combined'")

        agg_cols = [F.first(F.col(c), ignorenulls=True).alias(c) for c in df.columns if c not in keys]
        return df.groupBy([F.col(k) for k in keys]).agg(*agg_cols)
