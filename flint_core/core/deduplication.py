"""This module implements versatile data deduplication utilities for flint."""

from typing import Any, List, Optional, Union

# Handle optional dependencies at the top of the file
try:
    import pyspark.sql.functions as F
    from pyspark.sql import Window

    HAS_SPARK = True
except ImportError:
    Window: Any = None
    F: Any = None
    HAS_SPARK = False


class Deduplicator:
    """Utility class providing multiple strategies for data deduplication.

    Supports Pandas and PySpark DataFrames dynamically, allowing extraction of
    first, latest, multi-sorted, consolidated, or strictly unique records.
    """

    def __init__(self):
        """Initializes the Deduplicator instance.

        Args:

        Returns:
            None: Initializes the class instance.
        """
        pass

    def latest(self, df: Any, keys: List[str], order_by_col: str) -> Any:
        """Extracts the most recent record per business key group.

        Args:
            df(Any): Input Pandas or PySpark DataFrame.
            keys(List[str]): Columns that identify a business entity.
            order_by_col(str): Column used to determine chronological order.

        Returns:
            Any: Deduplicated DataFrame containing only the latest events.
        """
        framework = self._detect_framework(df)
        if framework == "pandas":
            return (
                df.sort_values(by=order_by_col, ascending=True)
                .groupby(keys)
                .last()
                .reset_index()
            )

        window_spec = Window.partitionBy(keys).orderBy(F.col(order_by_col).desc())
        return (
            df.withColumn("_row_num", F.row_number().over(window_spec))
            .filter(F.col("_row_num") == 1)
            .drop("_row_num")
        )

    def first(self, df: Any, keys: List[str], order_by_col: str) -> Any:
        """Extracts the earliest chronological record per business key group.

        Args:
            df(Any): Input Pandas or PySpark DataFrame.
            keys(List[str]): Columns that identify a business entity.
            order_by_col(str): Column used to determine chronological order.

        Returns:
            Any: Deduplicated DataFrame containing only the first events.
        """
        framework = self._detect_framework(df)
        if framework == "pandas":
            return (
                df.sort_values(by=order_by_col, ascending=True)
                .groupby(keys)
                .first()
                .reset_index()
            )

        window_spec = Window.partitionBy(keys).orderBy(F.col(order_by_col).asc())
        return (
            df.withColumn("_row_num", F.row_number().over(window_spec))
            .filter(F.col("_row_num") == 1)
            .drop("_row_num")
        )

    def distinct(self, df: Any, subset: Optional[List[str]] = None) -> Any:
        """Removes strict duplicate rows from the DataFrame.

        Args:
            df(Any): Input Pandas or PySpark DataFrame.
            subset(Optional[List[str]]): Specific columns to consider when
                identifying duplicates. Defaults to None (all columns).

        Returns:
            Any: Clean DataFrame with unique records only.
        """
        framework = self._detect_framework(df)
        if framework == "pandas":
            return df.drop_duplicates(subset=subset).reset_index(drop=True)

        return df.dropDuplicates(subset=subset)

    def by_order(
        self,
        df: Any,
        keys: List[str],
        order_by_cols: List[str],
        ascending: Union[bool, List[bool]] = True,
    ) -> Any:
        """Deduplicates rows by sorting through multiple custom column criteria.

        Args:
            df(Any): Input Pandas or PySpark DataFrame.
            keys(List[str]): Columns that identify a business entity.
            order_by_cols(List[str]): Ordered list of columns to sort by.
            ascending(Union[bool, List[bool]]): Type of sorting direction for
                each column. Defaults to True.

        Returns:
            Any: Deduplicated DataFrame containing the top-ranked records.
        """
        framework = self._detect_framework(df)
        if framework == "pandas":
            return (
                df.sort_values(by=order_by_cols, ascending=ascending)
                .groupby(keys)
                .first()
                .reset_index()
            )

        if isinstance(ascending, bool):
            asc_list = [ascending] * len(order_by_cols)
        else:
            asc_list = ascending

        order_exprs = []
        for col, asc_dir in zip(order_by_cols, asc_list):
            expr = F.col(col).asc() if asc_dir else F.col(col).desc()
            order_exprs.append(expr)

        window_spec = Window.partitionBy(keys).orderBy(*order_exprs)
        return (
            df.withColumn("_row_num", F.row_number().over(window_spec))
            .filter(F.col("_row_num") == 1)
            .drop("_row_num")
        )

    def combined(self, df: Any, keys: List[str]) -> Any:
        """Stitches rows together by compacting nulls into a golden record.

        Args:
            df(Any): Input Pandas or PySpark DataFrame.
            keys(List[str]): Columns that identify a business entity.

        Returns:
            Any: Consolidated DataFrame with filled attributes per group.
        """
        framework = self._detect_framework(df)
        if framework == "pandas":
            return df.groupby(keys).first().reset_index()

        agg_cols = [
            F.first(c, ignorenulls=True).alias(c) for c in df.columns if c not in keys
        ]
        return df.groupBy(keys).agg(*agg_cols)

    def _detect_framework(self, df: Any) -> str:
        """Internal helper to identify the dataframe framework type safely.

        Args:
            df(Any): Input dataframe object.

        Returns:
            str: String identifier ("pandas" or "spark").
        """
        df_type = type(df).__name__
        module_type = type(df).__module__

        if "pandas" in module_type and df_type == "DataFrame":
            return "pandas"

        if "pyspark" in module_type and df_type == "DataFrame":
            if not HAS_SPARK:
                raise ImportError("PySpark is required but could not be imported.")
            return "spark"

        raise TypeError(
            f"Unsupported type: {type(df)}. "
            "Only Pandas and PySpark DataFrames are supported."
        )
