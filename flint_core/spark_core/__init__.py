"""Spark concrete engine assembly module."""

from __future__ import annotations

from pyspark.sql import DataFrame as SparkDataFrame

from flint_core.core.base import BaseEngine
from flint_core.spark_core.deduplication import SparkDeduplicationMixin
from flint_core.spark_core.scd2 import SparkSCD2Mixin


class SparkEngine(SparkDeduplicationMixin, SparkSCD2Mixin, BaseEngine[SparkDataFrame]):
    """
    The unified, enterprise-scale PySpark engine assembled via modular Feature Mixins.
    """

    __slots__ = ()
