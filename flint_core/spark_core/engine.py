"""PySpark concrete engine implementation for multi-format distributed data."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

import pyspark.sql.functions as F
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.types import DataType, StructField, StructType

from flint_core.core.base import BaseEngine
from flint_core.core.catalog.models import ColumnDefinition
from flint_core.spark_core.deduplication import SparkDeduplicationMixin
from flint_core.spark_core.scd2 import SparkSCD2Mixin


class SparkEngine(SparkDeduplicationMixin, SparkSCD2Mixin, BaseEngine[SparkDataFrame]):
    """Enterprise PySpark engine orchestrating advanced multi-format schemas."""

    __slots__ = ()

    def load(
        self,
        path: str,
        data_format: str,
        columns: List[ColumnDefinition],
        metadata: Optional[Dict[str, Any]] = None,
        spark: Optional[Any] = None,
    ) -> SparkDataFrame:
        """Loads distributed data formats into a strict defined schema."""
        from pyspark.sql import SparkSession
        from pyspark.sql.types import (
            BooleanType,
            DateType,
            DecimalType,
            DoubleType,
            FloatType,
            IntegerType,
            LongType,
            StringType,
            TimestampType,
        )

        session = spark if spark is not None else SparkSession.getActiveSession()
        if session is None:
            raise ValueError("No distributed cluster session manager could be resolved.")

        spark_type_map: dict[str, DataType] = {
            "string": StringType(),
            "integer": IntegerType(),
            "long": LongType(),
            "double": DoubleType(),
            "float": FloatType(),
            "boolean": BooleanType(),
            "timestamp": TimestampType(),
            "date": DateType(),
        }

        fields = []
        post_load_conversions = []
        fmt = data_format.strip().lower()

        for col in columns:
            if col.data_type is None:
                fields.append(StructField(col.name, StringType(), True))
                continue

            dt_clean = col.data_type.strip().lower()

            if fmt in ("csv", "json") and col.format and (dt_clean in ("date", "timestamp")):
                fields.append(StructField(col.name, StringType(), True))
                post_load_conversions.append(col)
            elif dt_clean.startswith("decimal"):
                match = re.match(r"decimal\((\d+),?\s*(\d+)\)", dt_clean)
                if match:
                    p, s = int(match.group(1)), int(match.group(2))
                    fields.append(StructField(col.name, DecimalType(p, s), True))
                else:
                    fields.append(StructField(col.name, DecimalType(38, 18), True))
            else:
                s_type = spark_type_map.get(dt_clean, StringType())
                fields.append(StructField(col.name, s_type, True))

        spark_schema = StructType(fields) if fields else None
        reader = session.read

        # Dynamic read-time option injection for Spark nested options
        if metadata and "options" in metadata:
            opts = metadata["options"]
            if isinstance(opts, dict):
                reader = reader.options(**opts)

        # Execute multi-format router mapping schemas
        if fmt == "parquet":
            if spark_schema:
                reader = reader.schema(spark_schema)
            df = reader.parquet(path)
        elif fmt == "orc":
            if spark_schema:
                reader = reader.schema(spark_schema)
            df = reader.orc(path)
        elif fmt == "csv":
            reader = reader.options(header=True)
            if spark_schema:
                df = reader.schema(spark_schema).csv(path)
            else:
                df = reader.csv(path, inferSchema=True)
        elif fmt == "json":
            if spark_schema:
                df = reader.schema(spark_schema).json(path)
            else:
                df = reader.json(path)
        else:
            raise ValueError(f"Unsupported distributed format target: '{fmt}'.")

        # Column-level string formats vectorized post-load enforcement
        for col in post_load_conversions:
            dt_clean = col.data_type.strip().lower() if col.data_type else ""
            if dt_clean == "date":
                df = df.withColumn(col.name, F.to_date(F.col(col.name), col.format))
            elif dt_clean == "timestamp":
                df = df.withColumn(col.name, F.to_timestamp(F.col(col.name), col.format))

        return df
