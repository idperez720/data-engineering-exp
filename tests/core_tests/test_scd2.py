"""Unit tests for the flint-core SCD2 engine and facade routing capabilities."""

from __future__ import annotations

from typing import Any, Dict, List

import pytest

# Core architecture imports
from flint_core.core.exceptions import ColumnValidationError
from flint_core.functions.scd2 import process

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


@pytest.fixture
def mock_existing_dim() -> List[Dict[str, Any]]:
    """Provides a baseline historical dimension tracking customer profiles."""
    return [
        {
            "customer_id": 1,
            "email": "alice@alpha.com",
            "tier": "Gold",
            "effective_date": "2026-01-01",
            "end_date": "9999-12-31",
            "is_current": True,
            "input_timestamp": "2026-01-01",
        },
        {
            "customer_id": 2,
            "email": "bob@beta.com",
            "tier": "Silver",
            "effective_date": "2026-01-01",
            "end_date": "9999-12-31",
            "is_current": True,
            "input_timestamp": "2026-01-01",
        },
        {
            "customer_id": 3,
            "email": "charlie@gamma.com",
            "tier": "Bronze",
            "effective_date": "2026-01-01",
            "end_date": "9999-12-31",
            "is_current": True,
            "input_timestamp": "2026-01-01",
        },
    ]


@pytest.fixture
def mock_new_batch() -> List[Dict[str, Any]]:
    """Provides an incoming transaction batch with new and updated profiles."""
    return [
        {"customer_id": 2, "email": "bob@beta.com", "tier": "Gold", "input_timestamp": "2026-06-15"},
        {"customer_id": 4, "email": "david@delta.com", "tier": "Platinum", "input_timestamp": "2026-06-15"},
    ]


@pytest.fixture
def pandas_existing(mock_existing_dim: List[Dict[str, Any]]) -> pd.DataFrame:
    """Converts historical records into a Pandas DataFrame."""
    if not HAS_PANDAS:
        pytest.skip("Pandas environment dependency is absent.")
    return pd.DataFrame(mock_existing_dim)


@pytest.fixture
def pandas_new(mock_new_batch: List[Dict[str, Any]]) -> pd.DataFrame:
    """Converts new batch records into a Pandas DataFrame."""
    if not HAS_PANDAS:
        pytest.skip("Pandas environment dependency is absent.")
    return pd.DataFrame(mock_new_batch)


@pytest.fixture(scope="module")
def spark_session() -> SparkSession:  # type: ignore
    """Initializes a local Spark Session for SCD2 test execution context."""
    if not HAS_SPARK:
        pytest.skip("PySpark environment dependency is absent.")

    session = (
        SparkSession.builder.appName("FlintSCD2UnitTests")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def spark_existing(spark_session: SparkSession, mock_existing_dim: List[Dict[str, Any]]) -> Any:
    """Converts historical records into a PySpark DataFrame schema."""
    return spark_session.createDataFrame(mock_existing_dim)  # type: ignore


@pytest.fixture
def spark_new(spark_session: SparkSession, mock_new_batch: List[Dict[str, Any]]) -> Any:
    """Converts new batch records into a PySpark DataFrame schema."""
    return spark_session.createDataFrame(mock_new_batch)  # type: ignore


def test_empty_compare_columns_raises_error(pandas_new: pd.DataFrame, pandas_existing: pd.DataFrame) -> None:
    """Validates that triggering SCD2 updates without explicit compare targets fails."""
    with pytest.raises(ValueError) as exc_info:
        process(df_new=pandas_new, df_existing=pandas_existing, business_keys=["customer_id"], compare_columns=[])

    assert "compare_columns cannot be empty" in str(exc_info.value)


def test_scd2_column_validation_raises_error(pandas_new: pd.DataFrame, pandas_existing: pd.DataFrame) -> None:
    """Validates that providing invalid business or matrix keys fails schema validation check."""
    with pytest.raises(ColumnValidationError):
        process(
            df_new=pandas_new, df_existing=pandas_existing, business_keys=["missing_id_col"], compare_columns=["tier"]
        )


@pytest.mark.skipif(not HAS_PANDAS, reason="Pandas requirement flag is off.")
def test_pandas_scd2_pipeline(pandas_new: pd.DataFrame, pandas_existing: pd.DataFrame) -> None:
    """Validates full SCD2 pipeline logic correctness under a Pandas processing context."""
    result = process(
        df_new=pandas_new, df_existing=pandas_existing, business_keys=["customer_id"], compare_columns=["tier", "email"]
    )

    assert len(result) == 5

    # Changed 'is True' to '== True' to prevent NumPy boolean identity failure
    c4 = result[result["customer_id"] == 4].iloc[0]
    assert c4["is_current"] == True
    assert c4["effective_date"] == "2026-06-15"
    assert c4["end_date"] == "9999-12-31"

    c2_old = result[(result["customer_id"] == 2) & (result["is_current"] == False)].iloc[0]
    assert c2_old["tier"] == "Silver"
    assert str(c2_old["end_date"]).startswith("2026-06-15")

    c2_new = result[(result["customer_id"] == 2) & (result["is_current"] == True)].iloc[0]
    assert c2_new["tier"] == "Gold"
    assert c2_new["effective_date"] == "2026-06-15"
    assert c2_new["end_date"] == "9999-12-31"

    c3 = result[result["customer_id"] == 3].iloc[0]
    assert c3["is_current"] == True
    assert c3["end_date"] == "9999-12-31"


@pytest.mark.skipif(not HAS_SPARK, reason="PySpark requirement flag is off.")
def test_spark_scd2_pipeline(spark_new: Any, spark_existing: Any) -> None:
    """Validates full SCD2 pipeline logic correctness under a PySpark processing context."""
    result = process(
        df_new=spark_new, df_existing=spark_existing, business_keys=["customer_id"], compare_columns=["tier", "email"]
    )

    assert result.count() == 5

    c4 = result.filter("customer_id = 4").collect()[0]
    assert c4["is_current"] == True
    assert c4["effective_date"] == "2026-06-15"
    assert c4["end_date"] == "9999-12-31"

    c2_old = result.filter("customer_id = 2 AND is_current = false").collect()[0]
    assert c2_old["tier"] == "Silver"
    assert c2_old["end_date"] == "2026-06-15"

    c2_new = result.filter("customer_id = 2 AND is_current = true").collect()[0]
    assert c2_new["tier"] == "Gold"
    assert c2_new["effective_date"] == "2026-06-15"
    assert c2_new["end_date"] == "9999-12-31"
