"""Pandas concrete engine implementation for multi-format data interaction."""

from __future__ import annotations

import decimal
from pathlib import Path
from typing import Any, ClassVar, Dict, List, Optional
from urllib.parse import urlparse

import pandas as pd

from flint_core.core.base import BaseEngine
from flint_core.core.catalog.models import ColumnDefinition
from flint_core.pandas_core.deduplication import PandasDeduplicationMixin
from flint_core.pandas_core.scd2 import PandasSCD2Mixin


class PandasEngine(PandasDeduplicationMixin, PandasSCD2Mixin, BaseEngine[pd.DataFrame]):
    """Unified Pandas engine orchestrating clean multi-format parsing."""

    __slots__ = ()

    PANDAS_TYPE_MAP: ClassVar[Dict[str, str]] = {
        "integer": "Int64",
        "string": "str",
        "double": "float64",
        "float": "float32",
        "boolean": "bool",
    }

    def load(
        self,
        path: str,
        data_format: str,
        columns: List[ColumnDefinition],
        metadata: Optional[Dict[str, Any]] = None,
        spark: Optional[Any] = None,
    ) -> pd.DataFrame:
        """Loads data into a Pandas DataFrame with custom reader options."""
        dtype_dict: Any = {}
        parse_dates_fallback: List[str] = []

        options = metadata.get("options", {}) if metadata else {}

        for col in columns:
            if col.data_type is None:
                continue
            dt_clean = col.data_type.strip().lower()

            if dt_clean == "timestamp" and not col.format:
                parse_dates_fallback.append(col.name)
            elif dt_clean in self.PANDAS_TYPE_MAP:
                dtype_dict[col.name] = self.PANDAS_TYPE_MAP[dt_clean]

        fmt = data_format.strip().lower()

        if fmt == "csv":
            df = pd.read_csv(
                path,
                dtype=dtype_dict if dtype_dict else None,
                parse_dates=parse_dates_fallback if parse_dates_fallback else None,
                **options,
            )
        elif fmt == "parquet":
            df = pd.read_parquet(path, **options)
            df = self._apply_primitive_dtypes(df, dtype_dict)
        elif fmt == "json":
            orient_val = options.pop("orient", "records")
            df = pd.read_json(path, orient=orient_val, dtype=dtype_dict, **options)
        elif fmt == "orc":
            df = pd.read_orc(path, **options)
            df = self._apply_primitive_dtypes(df, dtype_dict)
        else:
            raise ValueError(f"Unsupported Pandas format: '{fmt}'.")

        return self._enforce_rich_types(df, columns, parse_dates_fallback)

    def save(
        self,
        df: pd.DataFrame,
        path: str,
        data_format: str,
        columns: List[ColumnDefinition],
        mode: str = "error",
        metadata: Optional[Dict[str, Any]] = None,
        spark: Optional[Any] = None,
    ) -> None:
        """Saves a Pandas DataFrame enforcing schemas or falling back to raw."""
        options = metadata.get("options", {}) if metadata else {}
        fmt = data_format.strip().lower()

        # 1. Fail-Fast I/O Boundary Verification (Bypass for Cloud URIs)
        is_cloud = urlparse(path).scheme in (
            "s3",
            "gs",
            "gcs",
            "abfss",
            "az",
        )

        if not is_cloud:
            file_path = Path(path)
            if file_path.exists():
                if mode == "error":
                    raise FileExistsError(f"Target path already exists on system: '{path}'.")
                if mode == "ignore":
                    return

            # 3. Generate parent folders safely duplicating layout boundaries
            if not file_path.parent.exists():
                file_path.parent.mkdir(parents=True, exist_ok=True)

        # 2. Graceful degradation for Schema-less writes
        if not columns:
            df_enforced = df.copy()
        else:
            catalog_names = [col.name for col in columns]
            missing_cols = [c for c in catalog_names if c not in df.columns]
            if missing_cols:
                raise ValueError(
                    "Schema enforcement failed on write. Missing required "
                    f"catalog columns in input DataFrame: {missing_cols}"
                )

            df_enforced = df[catalog_names].copy()
            dtype_dict: Any = {}
            fallbacks: List[str] = []

            for col in columns:
                if col.data_type is None:
                    continue
                dt_clean = col.data_type.strip().lower()
                if dt_clean == "timestamp" and not col.format:
                    fallbacks.append(col.name)
                elif dt_clean in self.PANDAS_TYPE_MAP:
                    dtype_dict[col.name] = self.PANDAS_TYPE_MAP[dt_clean]

            df_enforced = self._apply_primitive_dtypes(df_enforced, dtype_dict)
            df_enforced = self._enforce_rich_types(df_enforced, columns, fallbacks)

        if fmt == "csv":
            index_val = options.pop("index", False)
            df_enforced.to_csv(path, index=index_val, **options)
        elif fmt == "parquet":
            df_enforced.to_parquet(path, **options)
        elif fmt == "json":
            orient_val = options.pop("orient", "records")
            df_enforced.to_json(path, orient=orient_val, **options)
        elif fmt == "orc":
            df_enforced.to_orc(path, **options)
        else:
            raise ValueError(f"Unsupported Pandas write format: '{fmt}'.")

    def _apply_primitive_dtypes(self, df: pd.DataFrame, dtype_dict: Any) -> pd.DataFrame:
        """Applies primitive data types safely onto an existing DataFrame."""
        for col_name, dtype_val in dtype_dict.items():
            if col_name in df.columns:
                df[col_name] = df[col_name].astype(dtype_val)
        return df

    def _enforce_rich_types(
        self,
        df: pd.DataFrame,
        columns: List[ColumnDefinition],
        fallbacks: List[str],
    ) -> pd.DataFrame:
        """Enforces column-specific advanced business formats and timezones."""
        for col in columns:
            if col.data_type is None or col.name not in df.columns:
                continue
            dt_clean = col.data_type.strip().lower()

            if dt_clean == "timestamp":
                if col.format:
                    df[col.name] = pd.to_datetime(df[col.name], format=col.format)
                elif col.name not in fallbacks:
                    df[col.name] = pd.to_datetime(df[col.name])

                if col.timezone:
                    if df[col.name].dt.tz is None:
                        df[col.name] = df[col.name].dt.tz_localize(col.timezone)
                    else:
                        df[col.name] = df[col.name].dt.tz_convert(col.timezone)

            elif dt_clean == "date":
                df[col.name] = pd.to_datetime(df[col.name], format=col.format if col.format else None).dt.date

            elif dt_clean.startswith("decimal"):
                df[col.name] = pd.Series(
                    [decimal.Decimal(str(x)) if pd.notnull(x) else None for x in df[col.name]],
                    index=df.index,
                    dtype="object",
                )

        return df
