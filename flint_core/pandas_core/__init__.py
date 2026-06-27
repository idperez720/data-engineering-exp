"""Pandas concrete engine assembly module."""

from __future__ import annotations

import pandas as pd

from flint_core.core.base import BaseEngine
from flint_core.pandas_core.deduplication import PandasDeduplicationMixin

# from flint_core.pandas_core.scd2 import PandasSCD2Mixin


class PandasEngine(PandasDeduplicationMixin, BaseEngine[pd.DataFrame]):
    """
    The unified, production-grade Pandas engine assembled via modular Feature
    Mixins.
    """

    __slots__ = ()
