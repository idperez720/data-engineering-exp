"""Pandas concrete engine implementation for data loading."""

from __future__ import annotations

from typing import Any, ClassVar, Dict, List, Optional

import pandas as pd

from flint_core.core.base import BaseEngine
from flint_core.core.catalog.models import ColumnDefinition
from flint_core.pandas_core.deduplication import PandasDeduplicationMixin
from flint_core.pandas_core.scd2 import PandasSCD2Mixin


class PandasEngine(PandasDeduplicationMixin, PandasSCD2Mixin, BaseEngine[pd.DataFrame]):
    """Unified Pandas engine orchestrating read-time schema enforcement."""

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
        spark: Optional[Any] = None,
    ) -> pd.DataFrame:
        """Loads data into a Pandas DataFrame enforcing declarative schemas."""
        # Declaring the variable as Any bypasses third-party stub variance bugs
        dtype_dict: Any = {}
        parse_dates: List[str] = []

        for col in columns:
            if col.data_type == "timestamp":
                parse_dates.append(col.name)
            elif col.data_type in self.PANDAS_TYPE_MAP:
                dtype_dict[col.name] = self.PANDAS_TYPE_MAP[col.data_type]

        if data_format == "csv":
            return pd.read_csv(
                path,
                dtype=dtype_dict if dtype_dict else None,
                parse_dates=parse_dates if parse_dates else None,
            )
        elif data_format == "parquet":
            df = pd.read_parquet(path)
            for col_name, dtype_val in dtype_dict.items():
                if col_name in df.columns:
                    df[col_name] = df[col_name].astype(dtype_val)
            for col_name in parse_dates:
                if col_name in df.columns:
                    df[col_name] = pd.to_datetime(df[col_name])
            return df

        raise ValueError(f"Unsupported Pandas file layout: '{data_format}'.")
