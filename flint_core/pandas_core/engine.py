"""Pandas concrete engine implementation for multi-format data interaction."""

from __future__ import annotations

import decimal
from pathlib import Path
from typing import Any, ClassVar, Dict, List, Optional

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
            raise ValueError(f"Unsupported Pandas format parameter: '{fmt}'.")

        return self._enforce_rich_types(df, columns, parse_dates_fallback)

    def save(
        self,
        df: pd.DataFrame,
        path: str,
        data_format: str,
        mode: str = "error",
        metadata: Optional[Dict[str, Any]] = None,
        spark: Optional[Any] = None,
    ) -> None:
        """Saves a Pandas DataFrame into the designated storage location."""
        options = metadata.get("options", {}) if metadata else {}
        fmt = data_format.strip().lower()

        # Strict object-oriented validation utilizing pathlib
        file_path = Path(path)

        if file_path.exists():
            if mode == "error":
                raise FileExistsError(f"Target file path already exists on system: '{path}'.")
            elif mode == "ignore":
                return

        # Generate parent folders safely imitating initialization layers
        if not file_path.parent.exists():
            file_path.parent.mkdir(parents=True, exist_ok=True)

        if fmt == "csv":
            index_val = options.pop("index", False)
            df.to_csv(path, index=index_val, **options)
        elif fmt == "parquet":
            df.to_parquet(path, **options)
        elif fmt == "json":
            orient_val = options.pop("orient", "records")
            df.to_json(path, orient=orient_val, **options)
        elif fmt == "orc":
            df.to_orc(path, **options)
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
