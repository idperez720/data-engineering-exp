"""Pytest configuration."""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """
    Creates and returns a SparkSession configured for local testing.

    Returns:
        pyspark.sql.SparkSession: A SparkSession object with the
        application name 'tests' and master set to 'local[1]'.
    """
    return SparkSession.builder.appName("tests").master("local[1]").getOrCreate()
