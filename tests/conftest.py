"""Global pytest configuration and shared fixtures."""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    """Creates a local Spark Session shared across all tests.

    Args:

    Returns:
        SparkSession: A local PySpark session instance.
    """
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("dex-test-suite")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield spark
    spark.stop()
