"""PySpark concrete engine implementation for multi-format distributed data."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

import pyspark.sql.functions as F
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

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

        # Resolve and heal potential ghost sessions dynamically via reflection
        session = spark if spark is not None else SparkSession.getActiveSession()
        if session is not None and getattr(session, "_sc", None) is None:
            if hasattr(SparkSession, "_activeSession"):
                setattr(SparkSession, "_activeSession", None)
            if hasattr(SparkSession, "_instantiatedContext"):
                setattr(SparkSession, "_instantiatedContext", None)
            session = SparkSession.builder.getOrCreate()

        if session is None:
            session = SparkSession.builder.getOrCreate()

        spark_type_map: dict[str, DataType] = {
            "string": StringType(),
            "integer": IntegerType(),
            "long": LongType(),
            "double": DoubleType(),
            "float": FloatType(),
            "boolean": BooleanType(),
            "timestamp": TimestampType(),
            "date": DateType(),
            "decimal": DecimalType(38, 18),
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

        if metadata and "options" in metadata:
            opts = metadata["options"]
            if isinstance(opts, dict):
                reader = reader.options(**opts)

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

        for col in post_load_conversions:
            dt_clean = col.data_type.strip().lower() if col.data_type else ""
            if dt_clean == "date":
                df = df.withColumn(col.name, F.to_date(F.col(col.name), col.format))
            elif dt_clean == "timestamp":
                df = df.withColumn(col.name, F.to_timestamp(F.col(col.name), col.format))

        return df

    def save(
        self,
        df: SparkDataFrame,
        path: str,
        data_format: str,
        columns: List[ColumnDefinition],
        mode: str = "error",
        metadata: Optional[Dict[str, Any]] = None,
        spark: Optional[Any] = None,
    ) -> None:
        """Saves a PySpark DataFrame enforcing schemas or falling back to raw."""
        from pyspark.sql import SparkSession

        # Resolve and heal potential ghost sessions dynamically via reflection
        session = spark if spark is not None else SparkSession.getActiveSession()
        if session is not None and getattr(session, "_sc", None) is None:
            if hasattr(SparkSession, "_activeSession"):
                setattr(SparkSession, "_activeSession", None)
            if hasattr(SparkSession, "_instantiatedContext"):
                setattr(SparkSession, "_instantiatedContext", None)
            session = SparkSession.builder.getOrCreate()

        if session is None:
            session = SparkSession.builder.getOrCreate()

        options = metadata.get("options", {}) if metadata else {}
        fmt = data_format.strip().lower()

        # Graceful degradation for Schema-less writes
        if not columns:
            df_enforced = df
        else:
            # 1. Structural schema verification
            catalog_names = [col.name for col in columns]
            missing_cols = [c for c in catalog_names if c not in df.columns]
            if missing_cols:
                raise ValueError(
                    f"Schema enforcement failed on distributed write. Missing "
                    f"catalog columns in input Spark DataFrame: {missing_cols}"
                )

            # 2. Filtering / Column selection (Drops extra columns gracefully)
            df_enforced = df.select(*catalog_names)

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

            # 3. Dynamic Write-Time Coercion
            for col in columns:
                if col.data_type is None:
                    continue
                dt_clean = col.data_type.strip().lower()

                if dt_clean.startswith("decimal"):
                    match = re.match(r"decimal\((\d+),?\s*(\d+)\)", dt_clean)
                    if match:
                        p, s = int(match.group(1)), int(match.group(2))
                        s_type: DataType = DecimalType(p, s)
                    else:
                        s_type = DecimalType(38, 18)
                else:
                    s_type = spark_type_map.get(dt_clean, StringType())

                if fmt in ("csv", "json") and col.format and (dt_clean in ("date", "timestamp")):
                    df_enforced = df_enforced.withColumn(col.name, F.date_format(F.col(col.name), col.format))
                else:
                    df_enforced = df_enforced.withColumn(col.name, F.col(col.name).cast(s_type))

        # 4. Distributed Persist Execution
        writer = df_enforced.write.mode(mode)
        if options:
            writer = writer.options(**options)

        if fmt == "parquet":
            writer.parquet(path)
        elif fmt == "orc":
            writer.orc(path)
        elif fmt == "csv":
            writer.options(header=True).csv(path)
        elif fmt == "json":
            writer.json(path)
        else:
            raise ValueError(f"Unsupported distributed write format parameters: '{fmt}'.")
