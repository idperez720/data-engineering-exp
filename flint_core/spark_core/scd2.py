"""Spark implementation mixin for enterprise Slowly Changing Dimensions (SCD) Type 2 pipelines."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, List

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame

if TYPE_CHECKING:
    from flint_core.core.base import BaseEngine  # noqa: F401

logger = logging.getLogger(__name__)


class SparkSCD2Mixin:
    """Provides distributed SCD Type 2 processing capabilities to the main Spark engine."""

    if TYPE_CHECKING:

        def _validate_columns(self, df: Any, columns: List[str]) -> None: ...

    def scd2_process(
        self,
        df_new: DataFrame,
        df_existing: DataFrame,
        business_keys: List[str],
        compare_columns: List[str],
        effective_date_col: str = "effective_date",
        end_date_col: str = "end_date",
        current_flag_col: str = "is_current",
        input_timestamp_col: str = "input_timestamp",
        max_date: str = "9999-12-31",
    ) -> DataFrame:
        """Processes transactional high-volume delta rows against distributed Spark dimensions."""
        self._validate_columns(df_new, business_keys + compare_columns + [input_timestamp_col])
        self._validate_columns(
            df_existing, business_keys + compare_columns + [effective_date_col, end_date_col, current_flag_col]
        )

        logger.debug("Executing PySpark distributed SCD2 processing pipeline.")
        target_columns = df_existing.columns
        new_alias, exist_alias = "new", "exist"

        df_new_prepared = (
            df_new.withColumn(effective_date_col, F.col(input_timestamp_col))
            .withColumn(end_date_col, F.lit(max_date))
            .withColumn(current_flag_col, F.lit(True))
        )

        join_condition = [F.col(f"{new_alias}.{col}") == F.col(f"{exist_alias}.{col}") for col in business_keys]
        df_joined = df_new_prepared.alias(new_alias).join(df_existing.alias(exist_alias), on=join_condition, how="left")

        change_cond = self._build_change_condition(compare_columns, new_alias, exist_alias)
        unchanged_cond = self._build_unchanged_condition(compare_columns, new_alias, exist_alias)

        key_col = f"{exist_alias}.{business_keys[0]}"
        select_new_exprs = [F.col(f"{new_alias}.{c}").alias(c) for c in target_columns]
        df_new_only = df_joined.filter(F.col(key_col).isNull()).select(select_new_exprs)

        is_current_cond = F.col(f"{exist_alias}.{current_flag_col}")
        df_changed = df_joined.filter(is_current_cond).filter(change_cond).select(select_new_exprs)

        select_exist_exprs = [F.col(f"{exist_alias}.{c}").alias(c) for c in target_columns]
        df_unchanged = df_joined.filter(is_current_cond).filter(unchanged_cond).select(select_exist_exprs)

        select_expired_exprs = []
        for c in target_columns:
            if c not in [end_date_col, current_flag_col]:
                select_expired_exprs.append(F.col(f"{exist_alias}.{c}").alias(c))
            elif c == end_date_col:
                select_expired_exprs.append(F.col(f"{new_alias}.input_timestamp").alias(end_date_col))
            else:
                select_expired_exprs.append(F.lit(False).alias(current_flag_col))
        df_expired = df_joined.filter(is_current_cond).filter(change_cond).select(select_expired_exprs)

        df_preserved = (
            df_existing.alias(exist_alias)
            .join(df_new.alias(new_alias), on=business_keys, how="left_anti")
            .filter(is_current_cond)
            .select(select_exist_exprs)
        )

        return (
            df_new_only.unionByName(df_changed)
            .unionByName(df_expired)
            .unionByName(df_preserved)
            .unionByName(df_unchanged)
        )

    def _build_change_condition(self, compare_columns: List[str], new_alias: str, exist_alias: str) -> Column:
        if not compare_columns:
            raise ValueError("compare_columns cannot be empty for SCD2 processing.")
        condition = F.col(f"{new_alias}.{compare_columns[0]}") != F.col(f"{exist_alias}.{compare_columns[0]}")
        for col in compare_columns[1:]:
            condition |= F.col(f"{new_alias}.{col}") != F.col(f"{exist_alias}.{col}")
        return condition

    def _build_unchanged_condition(self, compare_columns: List[str], new_alias: str, exist_alias: str) -> Column:
        condition = F.lit(True)
        for col in compare_columns:
            condition &= F.col(f"{new_alias}.{col}") == F.col(f"{exist_alias}.{col}")
        return condition
