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

    def _inject_infrastructure(self, session: Any, metadata: Optional[Dict[str, Any]]) -> None:
        """Injects cloud infrastructure properties dynamically into Spark."""
        if not metadata:
            return
        infra_opts = metadata.get("infrastructure", {})
        if not isinstance(infra_opts, dict):
            return

        for k, v in infra_opts.items():
            # 1. Propagate to SQL Conf for cluster executors
            spark_key = k if k.startswith("spark.hadoop.") else f"spark.hadoop.{k}"
            session.conf.set(spark_key, str(v))

            # 2. Sync directly with JVM Hadoop Configuration for driver checks
            hadoop_key = k[13:] if k.startswith("spark.hadoop.") else k
            if hasattr(session, "_jsc") and session._jsc is not None:
                session._jsc.hadoopConfiguration().set(hadoop_key, str(v))

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

        session = spark if spark is not None else SparkSession.getActiveSession()
        if session is None or getattr(session, "_sc", None) is None:
            raise ValueError(
                "No active distributed SparkSession could be resolved. "
                "You must initialize a SparkSession before interacting "
                "with Spark catalog datasets."
            )

        # Apply Hadoop multi-cloud settings seamlessly to SQL and JVM layers
        self._inject_infrastructure(session, metadata)

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

            if fmt in ("csv", "json") and (dt_clean in ("date", "timestamp")):
                if col.format:
                    fields.append(StructField(col.name, StringType(), True))
                    post_load_conversions.append(col)
                    continue

            if dt_clean.startswith("decimal"):
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

        if fmt == "csv":
            df = reader.csv(path, schema=spark_schema)
        elif fmt == "parquet":
            df = reader.parquet(path)
        elif fmt == "json":
            df = reader.json(path, schema=spark_schema)
        elif fmt == "orc":
            df = reader.orc(path)
        else:
            raise ValueError(f"Unsupported Spark load format: '{fmt}'.")

        for col in post_load_conversions:
            if col.data_type == "date":
                df = df.withColumn(col.name, F.to_date(F.col(col.name), col.format))
            elif col.data_type == "timestamp":
                df = df.withColumn(col.name, F.to_timestamp(F.col(col.name), col.format))

        return df

    def save(
        self,
        df: Any,
        path: str,
        data_format: str,
        columns: List[ColumnDefinition],
        mode: str = "error",
        metadata: Optional[Dict[str, Any]] = None,
        spark: Optional[Any] = None,
    ) -> None:
        """Saves a distributed Spark DataFrame enforcing strict catalog schemas."""
        from pyspark.sql import SparkSession

        session = spark if spark is not None else SparkSession.getActiveSession()
        if session is None or getattr(session, "_sc", None) is None:
            raise ValueError("No active distributed SparkSession could be resolved.")

        # Apply Hadoop multi-cloud settings seamlessly to SQL and JVM layers
        self._inject_infrastructure(session, metadata)

        writer = df.write.mode(mode)

        if metadata and "options" in metadata:
            opts = metadata["options"]
            if isinstance(opts, dict):
                writer = writer.options(**opts)

        if columns:
            catalog_names = [col.name for col in columns]
            df = df.select(*catalog_names)

        fmt = data_format.strip().lower()

        if fmt == "csv":
            writer.csv(path)
        elif fmt == "parquet":
            writer.parquet(path)
        elif fmt == "json":
            writer.json(path)
        elif fmt == "orc":
            writer.orc(path)
        else:
            raise ValueError(f"Unsupported Spark save format: '{fmt}'.")
