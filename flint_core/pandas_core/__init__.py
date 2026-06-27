"""Pandas concrete engine assembly module."""

from __future__ import annotations

# Exposing the engine triggers the BaseEngine IoC initialization side-effect
from flint_core.pandas_core.engine import PandasEngine

__all__ = ["PandasEngine"]
