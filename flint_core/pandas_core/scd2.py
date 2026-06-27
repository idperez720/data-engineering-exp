"""Pandas implementation mixin for Slowly Changing Dimensions (SCD) Type 2 pipelines."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, List, Tuple

import pandas as pd

if TYPE_CHECKING:
    # This keeps Pylance happy without creating an actual empty method at runtime
    from flint_core.core.base import BaseEngine  # noqa: F401

logger = logging.getLogger(__name__)


class PandasSCD2Mixin:
    """Provides stateless SCD Type 2 processing capabilities to the main Pandas engine."""

    if TYPE_CHECKING:

        def _validate_columns(self, df: Any, columns: List[str]) -> None: ...

    def scd2_process(
        self,
        df_new: pd.DataFrame,
        df_existing: pd.DataFrame,
        business_keys: List[str],
        compare_columns: List[str],
        effective_date_col: str = "effective_date",
        end_date_col: str = "end_date",
        current_flag_col: str = "is_current",
        input_timestamp_col: str = "input_timestamp",
        max_date: str = "9999-12-31",
    ) -> pd.DataFrame:
        """Processes transactional delta data rows against local Pandas historical snapshots."""
        if not compare_columns:
            raise ValueError("compare_columns cannot be empty for SCD2 processing.")

        # This will now correctly trigger the BaseEngine validation block
        self._validate_columns(df_new, business_keys + compare_columns + [input_timestamp_col])
        self._validate_columns(
            df_existing, business_keys + compare_columns + [effective_date_col, end_date_col, current_flag_col]
        )

        logger.debug("Executing Pandas SCD2 processing pipeline.")
        df_active = df_existing[df_existing[current_flag_col]]

        df_exist_idx = df_active.set_index(business_keys)
        df_new_idx = df_new.set_index(business_keys)

        new_keys = df_new_idx.index.difference(df_exist_idx.index)
        common_keys = df_new_idx.index.intersection(df_exist_idx.index)
        preserved_keys = df_exist_idx.index.difference(df_new_idx.index)

        changed_keys, unchanged_keys = self._split_common_keys(df_new_idx, df_exist_idx, common_keys, compare_columns)

        df_new_only = self._get_new_records(
            df_new_idx, new_keys, effective_date_col, end_date_col, current_flag_col, input_timestamp_col, max_date
        )
        df_changed = self._get_changed_records(
            df_new_idx, changed_keys, effective_date_col, end_date_col, current_flag_col, input_timestamp_col, max_date
        )
        df_expired = self._get_expired_records(
            df_new_idx, df_exist_idx, changed_keys, end_date_col, current_flag_col, input_timestamp_col
        )
        df_unchanged = self._get_unchanged_records(df_exist_idx, unchanged_keys)
        df_preserved = self._get_preserved_records(df_exist_idx, preserved_keys)

        target_cols = df_existing.columns.tolist()
        return pd.concat(
            [df_new_only, df_changed, df_expired, df_unchanged, df_preserved],
            ignore_index=True,
        )[target_cols]

    def _split_common_keys(
        self,
        df_new_idx: pd.DataFrame,
        df_exist_idx: pd.DataFrame,
        common_keys: pd.Index,
        compare_columns: List[str],
    ) -> Tuple[pd.Index, pd.Index]:
        df_new_common = df_new_idx.loc[common_keys]
        df_exist_common = df_exist_idx.loc[common_keys]

        change_mask = pd.Series(False, index=common_keys)
        for col in compare_columns:
            change_mask |= df_new_common[col] != df_exist_common[col]

        return common_keys[change_mask], common_keys[~change_mask]

    def _get_new_records(
        self,
        df_new_idx: pd.DataFrame,
        new_keys: pd.Index,
        eff_col: str,
        end_col: str,
        flag_col: str,
        ts_col: str,
        max_dt: str,
    ) -> pd.DataFrame:
        df_new_only = df_new_only = df_new_idx.loc[new_keys].reset_index()
        df_new_only[eff_col] = df_new_only[ts_col]
        df_new_only[end_col] = max_dt
        df_new_only[flag_col] = True
        return df_new_only

    def _get_changed_records(
        self,
        df_new_idx: pd.DataFrame,
        changed_keys: pd.Index,
        eff_col: str,
        end_col: str,
        flag_col: str,
        ts_col: str,
        max_dt: str,
    ) -> pd.DataFrame:
        df_changed = df_new_idx.loc[changed_keys].reset_index()
        df_changed[eff_col] = df_changed[ts_col]
        df_changed[end_col] = max_dt
        df_changed[flag_col] = True
        return df_changed

    def _get_expired_records(
        self,
        df_new_idx: pd.DataFrame,
        df_exist_idx: pd.DataFrame,
        changed_keys: pd.Index,
        end_col: str,
        flag_col: str,
        ts_col: str,
    ) -> pd.DataFrame:
        df_expired = df_exist_idx.loc[changed_keys].reset_index()
        closing_timestamps = df_new_idx.loc[changed_keys, ts_col].values
        df_expired[end_col] = closing_timestamps
        df_expired[flag_col] = False
        return df_expired

    def _get_unchanged_records(self, df_exist_idx: pd.DataFrame, unchanged_keys: pd.Index) -> pd.DataFrame:
        return df_exist_idx.loc[unchanged_keys].reset_index()

    def _get_preserved_records(self, df_exist_idx: pd.DataFrame, preserved_keys: pd.Index) -> pd.DataFrame:
        return df_exist_idx.loc[preserved_keys].reset_index()
