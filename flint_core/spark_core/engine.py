"""PySpark concrete engine implementation for multi-format distributed data."""

from __future__ import annotations

import abc
import logging
import re
import threading
from typing import Any, ClassVar, Dict, List, Mapping, Optional, Set, Type

import pyspark.sql.functions as F
from pyspark.sql import Column, SparkSession
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.readwriter import DataFrameReader, DataFrameWriter
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
from flint_core.core.exceptions import ColumnValidationError, UnsupportedBackendError
from flint_core.spark_core.deduplication import SparkDeduplicationMixin
from flint_core.spark_core.scd2 import SparkSCD2Mixin

logger = logging.getLogger(__name__)


# =============================================================================
# BASE FORMAT STRATEGY INTERFACE
# =============================================================================


class SparkFormatHandler(abc.ABC):
    """Abstract Base Class governing format-specific read and write operations.

    Leverages metaprogramming to enforce definition-time self-registration
    across concrete extension subclasses.
    """

    __slots__ = ()
    format_key: ClassVar[str] = ""

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Automatically registers inheriting formats into the global engine boundaries."""
        super().__init_subclass__(**kwargs)
        fmt = getattr(cls, "format_key", "").strip().lower()
        if fmt:
            SparkEngine.register_custom_format(fmt, cls)

    @abc.abstractmethod
    def read(self, reader: DataFrameReader, path: str, schema: Optional[StructType]) -> SparkDataFrame:
        """Reads data from the specified path using the provided reader configuration.

        Args:
            reader: The PySpark DataFrameReader instance context.
            path: Absolute source location identifier pathway.
            schema: Optional strict structural target schema encapsulation matrix.

        Returns:
            SparkDataFrame: The distributed execution lineage plan.
        """
        pass

    @abc.abstractmethod
    def write(self, writer: DataFrameWriter, path: str) -> None:
        """Writes the DataFrame using the provided writer configuration.

        Args:
            writer: The PySpark DataFrameWriter instance context.
            path: Destination pathway boundary to commit storage layers.
        """
        pass


# =============================================================================
# CORE ENTERPRISE SPARK EXECUTION ENGINE (DEFINED EARLY TO PREVENT NAMEERRORS)
# =============================================================================


class SparkEngine(SparkDeduplicationMixin, SparkSCD2Mixin, BaseEngine[SparkDataFrame]):
    """Enterprise PySpark engine orchestrating advanced multi-format schemas.

    Attributes:
        SPARK_TYPE_MAP (ClassVar[Dict[str, DataType]]): Shared mapping of schema tokens
            to pre-allocated immutable Spark execution data types.
        FORMAT_REGISTRY (ClassVar[Dict[str, Type[SparkFormatHandler]]]): Dynamic registry
            enabling seamless third-party format extensibility.
        _REGISTRY_LOCK (ClassVar[threading.Lock]): Thread-safe primitive lock safeguarding
            atomic mutations over the format handler mappings.
    """

    __slots__ = ()

    # Pre-allocate stateless types to remove Garbage Collection overhead under heavy orchestration loops
    SPARK_TYPE_MAP: ClassVar[Dict[str, DataType]] = {
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

    # Open-Closed Principle registry for format plug-ability
    FORMAT_REGISTRY: ClassVar[Dict[str, Type[SparkFormatHandler]]] = {}

    _REGISTRY_LOCK: ClassVar[threading.Lock] = threading.Lock()

    @classmethod
    def register_custom_format(cls, format_name: str, handler_class: Type[SparkFormatHandler]) -> None:
        """Allows external plug-ins or extensions to inject custom formats into the engine safely.

        Args:
            format_name: Lowercase registration key identifier (e.g., 'delta', 'iceberg').
            handler_class: Concrete subclass implementation conforming to SparkFormatHandler.
        """
        with cls._REGISTRY_LOCK:
            cls.FORMAT_REGISTRY[format_name.strip().lower()] = handler_class
        logger.debug("Successfully bound format strategy '%s' to key '%s'", handler_class.__name__, format_name)

    def _inject_infrastructure(self, session: SparkSession, metadata: Optional[Mapping[str, Any]]) -> None:
        """Injects cloud infrastructure storage credentials safely into Spark runtime contexts."""
        if not metadata:
            return
        infra_opts = metadata.get("infrastructure", {})
        if not isinstance(infra_opts, dict):
            return

        for k, v in infra_opts.items():
            val_str = str(v)
            # 1. Synchronize cluster executor configurations via SQL Conf (Works everywhere, including Spark Connect)
            spark_key = k if k.startswith("spark.hadoop.") else f"spark.hadoop.{k}"
            session.conf.set(spark_key, val_str)

            # 2. Defensive type-safe bridging to JVM Hadoop Configuration resolving Pylance optional calls
            # Includes graceful degradation safeguards for modern decoupled Spark Connect clients.
            jsc = getattr(session, "_jsc", None)
            if jsc is not None:
                hadoop_conf = getattr(jsc, "hadoopConfiguration", None)
                if hadoop_conf is not None and callable(hadoop_conf):
                    # Py4J methods are resolved via dynamic reflection proxies.
                    # We isolate the execution safely and inform the type checker.
                    hadoop_conf().set(k[13:] if k.startswith("spark.hadoop.") else k, val_str)  # type: ignore[operator]
            else:
                logger.debug(
                    "JVM Gateway context is missing. Skipping driver-side JVM infrastructure injection fallback. "
                    "This is expected if executing under decoupled Spark Connect client runtimes."
                )

    def _resolve_format_handler(self, data_format: str) -> SparkFormatHandler:
        """Resolves concrete format strategies dynamically from the micro-kernel registry."""
        fmt_clean = data_format.strip().lower()
        with self._REGISTRY_LOCK:
            handler_class = self.FORMAT_REGISTRY.get(fmt_clean)

        if not handler_class:
            raise UnsupportedBackendError(
                f"No storage strategy registered for format: '{data_format}'. "
                f"Supported options: {list(self.FORMAT_REGISTRY.keys())}"
            )
        return handler_class()

    def load(
        self,
        path: str,
        data_format: str,
        columns: List[ColumnDefinition],
        metadata: Optional[Mapping[str, Any]] = None,
        spark: Optional[SparkSession] = None,
    ) -> SparkDataFrame:
        """Loads distributed data formats enforcing catalog structural schemas transactionally."""
        session = spark if spark is not None else SparkSession.getActiveSession()
        if session is None or getattr(session, "_sc", None) is None:
            raise ValueError(
                "No active distributed SparkSession could be resolved. "
                "You must initialize a SparkSession before interacting with Spark catalog datasets."
            )

        self._inject_infrastructure(session, metadata)
        handler = self._resolve_format_handler(data_format)

        fields = []
        lazy_projections: List[Column] = []
        fmt = data_format.strip().lower()

        for col in columns:
            if col.data_type is None:
                fields.append(StructField(col.name, StringType(), True))
                lazy_projections.append(F.col(col.name))
                continue

            dt_clean = col.data_type.strip().lower()

            # Handle parsing bottlenecks for text serialization targets
            if fmt in ("csv", "json") and (dt_clean in ("date", "timestamp")) and col.format:
                fields.append(StructField(col.name, StringType(), True))
                if dt_clean == "date":
                    lazy_projections.append(F.to_date(F.col(col.name), col.format).alias(col.name))
                else:
                    lazy_projections.append(F.to_timestamp(F.col(col.name), col.format).alias(col.name))
                continue

            # Dynamically compile structural metadata tokens
            if dt_clean.startswith("decimal"):
                match = re.match(r"decimal\((\d+),?\s*(\d+)\)", dt_clean)
                s_type: DataType = (
                    DecimalType(int(match.group(1)), int(match.group(2))) if match else DecimalType(38, 18)
                )
            else:
                s_type = self.SPARK_TYPE_MAP.get(dt_clean, StringType())

            fields.append(StructField(col.name, s_type, True))
            lazy_projections.append(F.col(col.name))

        spark_schema = StructType(fields) if fields else None
        reader = session.read

        if metadata and "options" in metadata:
            opts = metadata["options"]
            if isinstance(opts, dict):
                reader = reader.options(**opts)

        # Delegate execution down to the mapped strategy
        df = handler.read(reader, path, schema=spark_schema)

        # Mitigate Lineage Bleating by compounding projections into a single physical operation block
        if lazy_projections:
            df = df.select(*lazy_projections)

        return df

    def save(
        self,
        df: SparkDataFrame,
        path: str,
        data_format: str,
        columns: List[ColumnDefinition],
        mode: str = "error",
        metadata: Optional[Mapping[str, Any]] = None,
        spark: Optional[SparkSession] = None,
    ) -> None:
        """Saves a distributed Spark DataFrame executing strict structural fail-fast verifications."""
        session = spark if spark is not None else SparkSession.getActiveSession()
        if session is None or getattr(session, "_sc", None) is None:
            raise ValueError("No active distributed SparkSession could be resolved.")

        self._inject_infrastructure(session, metadata)
        handler = self._resolve_format_handler(data_format)
        writer = df.write.mode(mode)

        if metadata and "options" in metadata:
            opts = metadata["options"]
            if isinstance(opts, dict):
                writer = writer.options(**opts)

        if columns:
            catalog_names = [col.name for col in columns]
            input_cols: Set[str] = set(df.columns)
            missing_cols = [c for c in catalog_names if c not in input_cols]

            # Fail-fast engine verification layer to prevent corrupt transaction commits
            if missing_cols:
                raise ColumnValidationError(
                    f"Schema validation assertion failed on save phase. "
                    f"Missing expected catalog columns in client input DataFrame: {missing_cols}"
                )
            df = df.select(*catalog_names)

        # Route down to backend persistent strategies
        handler.write(writer, path)


# =============================================================================
# CONCRETE FORMAT STRATEGIES (EVALUATED AFTER THE ENGINE REGISTER LAYER)
# =============================================================================


class CSVFormatHandler(SparkFormatHandler):
    """Strategy handler for delimited text files."""

    __slots__ = ()
    format_key: ClassVar[str] = "csv"

    def read(self, reader: DataFrameReader, path: str, schema: Optional[StructType]) -> SparkDataFrame:
        return reader.csv(path, schema=schema)

    def write(self, writer: DataFrameWriter, path: str) -> None:
        writer.csv(path)


class ParquetFormatHandler(SparkFormatHandler):
    """Strategy handler for self-describing columnar Parquet files."""

    __slots__ = ()
    format_key: ClassVar[str] = "parquet"

    def read(self, reader: DataFrameReader, path: str, schema: Optional[StructType]) -> SparkDataFrame:
        return reader.parquet(path)

    def write(self, writer: DataFrameWriter, path: str) -> None:
        writer.parquet(path)


class JSONFormatHandler(SparkFormatHandler):
    """Strategy handler for semi-structured JSON lines datasets."""

    __slots__ = ()
    format_key: ClassVar[str] = "json"

    def read(self, reader: DataFrameReader, path: str, schema: Optional[StructType]) -> SparkDataFrame:
        return reader.json(path, schema=schema)

    def write(self, writer: DataFrameWriter, path: str) -> None:
        writer.json(path)


class ORCFormatHandler(SparkFormatHandler):
    """Strategy handler for Optimized Row Columnar deployments."""

    __slots__ = ()
    format_key: ClassVar[str] = "orc"

    def read(self, reader: DataFrameReader, path: str, schema: Optional[StructType]) -> SparkDataFrame:
        return reader.orc(path)

    def write(self, writer: DataFrameWriter, path: str) -> None:
        writer.orc(path)


# Initialize internal default format definitions seamlessly through side-effects
_DEFAULTS = [CSVFormatHandler, ParquetFormatHandler, JSONFormatHandler, ORCFormatHandler]
