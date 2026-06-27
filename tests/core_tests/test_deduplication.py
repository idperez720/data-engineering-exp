"""
Unit tests for the flint-core deduplication engine and facade routing
capabilities.
"""

from __future__ import annotations

from typing import Any, Dict, List

import pytest

from flint_core.core.exceptions import ColumnValidationError, UnsupportedBackendError

# Core architecture imports
from flint_core.functions.deduplication import by_order, combined, first, latest

# Safely check for pandas and spark availability for conditional test execution
try:
    import pandas as pd

    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

try:
    import pyspark.sql.functions as F
    from pyspark.sql import SparkSession

    HAS_SPARK = True
except ImportError:
    HAS_SPARK = False


# --- PYTEST FIXTURES ---


@pytest.fixture
def mock_raw_data() -> List[Dict[str, Any]]:
    """Provides a standardized matrix of raw dictionary records containing duplicates.

    Returns:
        List[Dict[str, Any]]: Baseline mock records for engineering pipelines.
    """
    return [
        {
            "id": 1,
            "state": "TX",
            "updated_at": "2026-01-01",
            "score": 10,
            "note": "oldest",
        },
        {"id": 1, "state": "TX", "updated_at": "2026-06-01", "score": 30, "note": None},
        {
            "id": 1,
            "state": "TX",
            "updated_at": "2026-03-01",
            "score": 20,
            "note": "middle",
        },
        {
            "id": 2,
            "state": "CA",
            "updated_at": "2026-05-01",
            "score": 50,
            "note": "active",
        },
    ]


@pytest.fixture
def pandas_df(mock_raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
    """Converts mock matrix into a standard local Pandas DataFrame.

    Args:
        mock_raw_data: Baseline raw dictionary records.

    Returns:
        pd.DataFrame: Active pandas structural matrix.
    """
    if not HAS_PANDAS:
        pytest.skip("Pandas environment dependency is absent.")
    return pd.DataFrame(mock_raw_data)


@pytest.fixture(scope="module")
def spark_session() -> SparkSession:  # type: ignore
    """Initializes a local, thread-isolated active Spark Session for testing scopes.

    Yields:
        SparkSession: Context engine for distributed data frames operations.
    """
    if not HAS_SPARK:
        pytest.skip("PySpark environment dependency is absent.")

    session = (
        SparkSession.builder.appName("FlintUnitTests")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def spark_df(spark_session: SparkSession, mock_raw_data: List[Dict[str, Any]]) -> Any:
    """Converts mock matrix into an active distributed PySpark DataFrame plan.

    Args:
        spark_session: Active isolation framework cluster reference.
        mock_raw_data: Baseline raw dictionary records.

    Returns:
        SparkDataFrame: Evaluated distributed dataframe reference.
    """
    return spark_session.createDataFrame(mock_raw_data)  # type: ignore


# --- ARCHITECTURAL ROUTING & EDGE CASE TESTS ---


def test_unsupported_backend_raises_error() -> None:
    """Validates that passing a raw unmapped object triggers UnsupportedBackendError."""
    invalid_data = {"key": "value"}  # Dicts are not registered inside our EngineRegistry

    with pytest.raises(UnsupportedBackendError) as exc_info:
        latest(invalid_data, keys=["id"], order_by_col="updated_at")

    assert "No execution engine registered" in str(exc_info.value)


def test_column_validation_raises_error(pandas_df: pd.DataFrame) -> None:
    """Validates that tracking non-existent columns triggers a ColumnValidationError."""
    with pytest.raises(ColumnValidationError) as exc_info:
        latest(pandas_df, keys=["non_existent_key"], order_by_col="updated_at")

    assert "required columns are missing" in str(exc_info.value)


def test_by_order_length_mismatch_raises_error(pandas_df: pd.DataFrame) -> None:
    """
    Validates that sorting sequence mismatches with ascending configuration arrays fail.
    """
    with pytest.raises(ValueError) as exc_info:
        by_order(
            pandas_df,
            keys=["id"],
            order_by_cols=["score", "updated_at"],
            ascending=[True],
        )

    assert "Length of 'ascending' list" in str(exc_info.value)


# --- PANDAS CORE ALGORITHMIC TESTS ---


@pytest.mark.skipif(not HAS_PANDAS, reason="Pandas is required to run this assertion.")
def test_pandas_latest_deduplication(pandas_df: pd.DataFrame) -> None:
    """
    Asserts that latest() retains the latest chronological record on Pandas backend.
    """
    result = latest(pandas_df, keys=["id", "state"], order_by_col="updated_at")

    # Assert row count reduction (should compact id=1 down to 1 record, total 2 rows)
    assert len(result) == 2

    # Assert correct value capturing (id=1 latest update is 2026-06-01 with score 30)
    target_row = result[result["id"] == 1].iloc[0]
    assert target_row["updated_at"] == "2026-06-01"
    assert target_row["score"] == 30


@pytest.mark.skipif(not HAS_PANDAS, reason="Pandas is required to run this assertion.")
def test_pandas_first_deduplication(pandas_df: pd.DataFrame) -> None:
    """
    Asserts that first() retains the earliest chronological record on Pandas backend.
    """
    result = first(pandas_df, keys=["id", "state"], order_by_col="updated_at")

    assert len(result) == 2
    target_row = result[result["id"] == 1].iloc[0]
    assert target_row["updated_at"] == "2026-01-01"
    assert target_row["score"] == 10


@pytest.mark.skipif(not HAS_PANDAS, reason="Pandas is required to run this assertion.")
def test_pandas_by_order_custom_sort(pandas_df: pd.DataFrame) -> None:
    """
    Asserts that by_order() honors compound multi-column prioritizing arrays in Pandas.
    """
    # Prioritize highest score first (ascending=False)
    result = by_order(pandas_df, keys=["id"], order_by_cols=["score"], ascending=False)

    target_row = result[result["id"] == 1].iloc[0]
    assert target_row["score"] == 30


@pytest.mark.skipif(not HAS_PANDAS, reason="Pandas is required to run this assertion.")
def test_pandas_combined_golden_record(pandas_df: pd.DataFrame) -> None:
    """Asserts that combined() synthesizes golden records ignoring inner nulls in Pandas."""
    result = combined(pandas_df, keys=["id"])

    target_row = result[result["id"] == 1].iloc[0]
    # Should resolve the first non-null record for structural columns
    assert target_row["note"] == "oldest"


# --- SPARK CORE ALGORITHMIC TESTS ---


@pytest.mark.skipif(not HAS_SPARK, reason="PySpark is required to run this assertion.")
def test_spark_latest_deduplication(spark_df: Any) -> None:
    """Asserts that latest() retains the latest chronological record on Spark execution context."""
    result = latest(spark_df, keys=["id", "state"], order_by_col="updated_at")

    assert result.count() == 2
    target_row = result.filter(F.col("id") == 1).collect()[0]
    assert target_row["updated_at"] == "2026-06-01"
    assert target_row["score"] == 30


@pytest.mark.skipif(not HAS_SPARK, reason="PySpark is required to run this assertion.")
def test_spark_first_deduplication(spark_df: Any) -> None:
    """Asserts that first() retains the earliest chronological record on Spark execution context."""
    result = first(spark_df, keys=["id", "state"], order_by_col="updated_at")

    assert result.count() == 2
    target_row = result.filter(F.col("id") == 1).collect()[0]
    assert target_row["updated_at"] == "2026-01-01"
    assert target_row["score"] == 10


@pytest.mark.skipif(not HAS_SPARK, reason="PySpark is required to run this assertion.")
def test_spark_by_order_custom_sort(spark_df: Any) -> None:
    """Asserts that by_order() honors compound multi-column prioritizing arrays in Spark."""
    result = by_order(spark_df, keys=["id"], order_by_cols=["score"], ascending=False)

    target_row = result.filter(F.col("id") == 1).collect()[0]
    assert target_row["score"] == 30


@pytest.mark.skipif(not HAS_SPARK, reason="PySpark is required to run this assertion.")
def test_spark_combined_golden_record(spark_df: Any) -> None:
    """Asserts that combined() synthesizes golden records ignoring inner nulls in Spark."""
    result = combined(spark_df, keys=["id"])

    target_row = result.filter(F.col("id") == 1).collect()[0]
    assert target_row["note"] == "oldest"
