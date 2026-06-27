"""Global pytest configurations and shared distributed session fixtures."""

from __future__ import annotations

from typing import Any, Generator

import pytest


@pytest.fixture(scope="function")
def spark_session() -> Generator[Any, None, None]:
    """Provides a resilient, isolated function-scoped PySpark active manager."""
    from pyspark.sql import SparkSession

    # Reset singletons safely via reflection to avoid Pylance stub warnings
    if hasattr(SparkSession, "_activeSession"):
        setattr(SparkSession, "_activeSession", None)
    if hasattr(SparkSession, "_instantiatedContext"):
        setattr(SparkSession, "_instantiatedContext", None)

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("FlintCoreTestContext")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    yield spark

    spark.stop()

    if hasattr(SparkSession, "_activeSession"):
        setattr(SparkSession, "_activeSession", None)
    if hasattr(SparkSession, "_instantiatedContext"):
        setattr(SparkSession, "_instantiatedContext", None)
