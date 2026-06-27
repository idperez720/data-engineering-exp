"""Spark concrete engine assembly module."""

from __future__ import annotations

# Exposing the engine triggers the BaseEngine IoC initialization side-effect
from flint_core.spark_core.engine import SparkEngine

__all__ = ["SparkEngine"]
