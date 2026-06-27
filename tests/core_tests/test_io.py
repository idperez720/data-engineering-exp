"""Unit tests for the environment-agnostic metadata-driven DataLoader engine."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Generator

import pytest
import yaml

from flint_core.core.catalog import DataCatalog
from flint_core.core.exceptions import UnsupportedBackendError
from flint_core.core.io import DataLoader


@pytest.fixture
def mock_catalog_environment(
    tmp_path: Path,
) -> Generator[DataCatalog, None, None]:
    """Scaffolds a rigid multi-engine test catalog layout for validation."""
    catalog_dir = tmp_path / "conf" / "catalog"
    data_dir = tmp_path / "data"

    catalog_dir.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)

    # Establish the root anchor
    with open(tmp_path / "pyproject.toml", "w", encoding="utf-8") as f:
        f.write('[project]\nname = "test-io-framework"\n')

    # Drop a physical dummy CSV asset with multiple datatypes
    csv_path = data_dir / "metrics.csv"
    csv_path.write_text(
        "id,value,timestamp_col\n1,100,2026-01-01 12:00:00\n2,200,2026-01-02 12:00:00\n",
        encoding="utf-8",
    )

    catalog_yaml = {
        "local_csv_relative": {
            "engine": "pandas",
            "format": "csv",
            "storage_path": "data/metrics.csv",
            "columns": [
                {"name": "id", "type": "integer"},
                {"name": "value", "type": "string"},
                {"name": "timestamp_col", "type": "timestamp"},
            ],
        },
        "unsupported_engine_asset": {
            "engine": "duckdb",
            "format": "parquet",
            "storage_path": "data/metrics.parquet",
            "columns": [],
        },
        "invalid_pandas_format": {
            "engine": "pandas",
            "format": "xml",
            "storage_path": "data/metrics.xml",
            "columns": [],
        },
        "spark_missing_session_asset": {
            "engine": "spark",
            "format": "csv",
            "storage_path": "data/metrics.csv",
            "columns": [{"name": "id", "type": "integer"}],
        },
        "spark_active_enforcement": {
            "engine": "spark",
            "format": "csv",
            "storage_path": "data/metrics.csv",
            "columns": [
                {"name": "id", "type": "integer"},
                {"name": "value", "type": "string"},
                {"name": "timestamp_col", "type": "timestamp"},
            ],
        },
    }

    with open(catalog_dir / "io_test.yaml", "w", encoding="utf-8") as f:
        yaml.dump(catalog_yaml, f)

    yield DataCatalog(catalog_path=catalog_dir)


def test_data_loader_relative_path_resolution(
    mock_catalog_environment: DataCatalog,
) -> None:
    """Asserts relative paths resolution and read-time type enforcement."""
    pd = pytest.importorskip("pandas")

    loader = DataLoader(catalog=mock_catalog_environment)
    df = loader.load("local_csv_relative")

    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["id", "value", "timestamp_col"]
    assert len(df) == 2

    # Robust library-grade schema enforcement validation
    assert df["id"].dtype == "Int64"
    assert pd.api.types.is_string_dtype(df["value"])
    assert pd.api.types.is_datetime64_any_dtype(df["timestamp_col"])


def test_data_loader_unsupported_engine_raises_backend_error(
    mock_catalog_environment: DataCatalog,
) -> None:
    """Asserts that an unrecognized compute engine triggers UnsupportedBackendError."""
    loader = DataLoader(catalog=mock_catalog_environment)

    with pytest.raises(UnsupportedBackendError) as exc_info:
        loader.load("unsupported_engine_asset")

    assert "No execution engine registered for name: 'duckdb'" in str(exc_info.value)


def test_data_loader_unsupported_format_for_engine_raises_value_error(
    mock_catalog_environment: DataCatalog,
) -> None:
    """Asserts that an invalid engine format raises a semantic ValueError."""
    pytest.importorskip("pandas")
    loader = DataLoader(catalog=mock_catalog_environment)

    with pytest.raises(ValueError) as exc_info:
        loader.load("invalid_pandas_format")

    assert "Unsupported Pandas file layout" in str(exc_info.value)


def test_data_loader_spark_missing_session_raises_value_error(
    mock_catalog_environment: DataCatalog,
) -> None:
    """Asserts that running Spark load without a session raises a ValueError."""
    pytest.importorskip("pyspark")
    loader = DataLoader(catalog=mock_catalog_environment)

    with pytest.raises(ValueError) as exc_info:
        loader.load("spark_missing_session_asset", spark=None)

    assert "No distributed cluster session manager could be resolved" in str(exc_info.value)


def test_data_loader_spark_read_time_enforcement(mock_catalog_environment: DataCatalog, spark_session: Any) -> None:
    """Asserts distributed read-time schema enforcement using active SparkSession."""
    pytest.importorskip("pyspark")
    loader = DataLoader(catalog=mock_catalog_environment)

    df = loader.load("spark_active_enforcement", spark=spark_session)

    # Assert exact schema resolution on Spark DataFrame layout catalogs
    schema_fields = {field.name: field.dataType.simpleString() for field in df.schema}
    assert schema_fields["id"] == "int"
    assert schema_fields["value"] == "string"
    assert schema_fields["timestamp_col"] == "timestamp"
