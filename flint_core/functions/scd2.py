"""SCD2 feature facade and protocol interface for the flint framework."""

from __future__ import annotations

import logging
from typing import Any, List, Protocol, TypeVar, cast

from flint_core.core.base import EngineRegistry

logger = logging.getLogger(__name__)

DataFrameT = TypeVar("DataFrameT", bound=Any)


class SCD2Protocol(Protocol[DataFrameT]):
    """Structural protocol defining the contract for Slowly Changing Dimension Type 2 capabilities."""

    def scd2_process(
        self,
        df_new: DataFrameT,
        df_existing: DataFrameT,
        business_keys: List[str],
        compare_columns: List[str],
        effective_date_col: str,
        end_date_col: str,
        current_flag_col: str,
        input_timestamp_col: str,
        max_date: str,
    ) -> DataFrameT:
        """Orchestrates the SCD2 pipeline process and returns the unified result dataframe."""
        ...


def process(
    df_new: DataFrameT,
    df_existing: DataFrameT,
    business_keys: List[str],
    compare_columns: List[str],
    effective_date_col: str = "effective_date",
    end_date_col: str = "end_date",
    current_flag_col: str = "is_current",
    input_timestamp_col: str = "input_timestamp",
    max_date: str = "9999-12-31",
) -> DataFrameT:
    """Orchestrates the Slowly Changing Dimension Type 2 (SCD2) pipeline across registered engines.

    Args:
        df_new: Incoming batch of raw or transformed delta data.
        df_existing: Current operational state of the target historical dimension.
        business_keys: Sequence of columns identifying unique corporate business identities.
        compare_columns: Sequence of attributes monitored to trigger historical mutations.
        effective_date_col: Column name marking record validation start timeline. Defaults to "effective_date".
        end_date_col: Column name marking record validation close timeline. Defaults to "end_date".
        current_flag_col: Target boolean field mapping operational row records state. Defaults to "is_current".
        input_timestamp_col: Record execution context state entry timestamp identifier. Defaults to "input_timestamp".
        max_date: Target infinity upper bound value assigned onto operational records. Defaults to "9999-12-31".

    Returns:
        DataFrameT: Re-unified structural matrix containing active, mutated and preserved row histories.
    """
    engine = cast(SCD2Protocol[DataFrameT], EngineRegistry.resolve_engine(df_new))
    return engine.scd2_process(
        df_new=df_new,
        df_existing=df_existing,
        business_keys=business_keys,
        compare_columns=compare_columns,
        effective_date_col=effective_date_col,
        end_date_col=end_date_col,
        current_flag_col=current_flag_col,
        input_timestamp_col=input_timestamp_col,
        max_date=max_date,
    )
