"""PySpark concrete engine implementation for data loading."""

from __future__ import annotations

from typing import Any, List, Optional

from pyspark.sql import DataFrame as SparkDataFrame

from flint_core.core.base import BaseEngine
from flint_core.core.catalog.models import ColumnDefinition
from flint_core.spark_core.deduplication import SparkDeduplicationMixin
from flint_core.spark_core.scd2 import SparkSCD2Mixin


class SparkEngine(SparkDeduplicationMixin, SparkSCD2Mixin, BaseEngine[SparkDataFrame]):
    """Enterprise PySpark engine orchestrating windowed schema execution."""

    __slots__ = ()

    def load(
        self,
        path: str,
        data_format: str,
        columns: List[ColumnDefinition],
        spark: Optional[Any] = None,
    ) -> SparkDataFrame:
        """Loads distributed data files into a strict structured Spark schema."""
        from pyspark.sql import SparkSession
        from pyspark.sql.types import (
            BooleanType,
            DoubleType,
            FloatType,
            IntegerType,
            LongType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        session = spark if spark is not None else SparkSession.getActiveSession()
        if session is None:
            raise ValueError("No distributed cluster session manager could be resolved.")

        spark_type_map = {
            "string": StringType(),
            "integer": IntegerType(),
            "long": LongType(),
            "double": DoubleType(),
            "float": FloatType(),
            "boolean": BooleanType(),
            "timestamp": TimestampType(),
        }

        fields = []
        for col in columns:
            dt = col.data_type
            # Type guard ensures dt is strictly str before dict lookup
            if dt is not None:
                spark_type = spark_type_map.get(dt, StringType())
            else:
                spark_type = StringType()

            fields.append(StructField(col.name, spark_type, True))

        spark_schema = StructType(fields) if fields else None

        if data_format == "parquet":
            reader = session.read
            if spark_schema:
                reader = reader.schema(spark_schema)
            return reader.parquet(path)
        elif data_format == "csv":
            if spark_schema:
                return session.read.options(header=True).schema(spark_schema).csv(path)
            return session.read.csv(path, header=True, inferSchema=True)

        raise ValueError(f"Unsupported distributed format: '{data_format}'.")
