"""Spark concrete engine assembly module."""

from __future__ import annotations

from pyspark.sql import DataFrame as SparkDataFrame

from flint_core.core.base import BaseEngine
from flint_core.spark_core.deduplication import SparkDeduplicationMixin


class SparkEngine(SparkDeduplicationMixin, BaseEngine[SparkDataFrame]):
    """
    The unified, enterprise-scale PySpark engine assembled via modular Feature Mixins.
    """

    __slots__ = ()
