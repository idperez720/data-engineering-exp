"""Unit tests for the environment-agnostic metadata-driven DataLoader engine."""

from pathlib import Path
from typing import Generator

import pytest
import yaml

from flint_core.core.catalog import DataCatalog
from flint_core.core.io import DataLoader


@pytest.fixture
def mock_catalog_environment(tmp_path: Path) -> Generator[DataCatalog, None, None]:
    """Scaffolds a rigid multi-engine test catalog layout anchored to a virtual project root."""
    catalog_dir = tmp_path / "conf" / "catalog"
    data_dir = tmp_path / "data"

    catalog_dir.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)

    # Establish the root anchor
    with open(tmp_path / "pyproject.toml", "w", encoding="utf-8") as f:
        f.write('[project]\nname = "test-io-framework"\n')

    # Drop a physical dummy CSV asset
    csv_path = data_dir / "metrics.csv"
    csv_path.write_text("id,value\n1,100\n2,200\n", encoding="utf-8")

    catalog_yaml = {
        "local_csv_relative": {
            "engine": "pandas",
            "format": "csv",
            "storage_path": "data/metrics.csv",  # Relative resolution track
            "columns": [{"name": "id", "type": "int"}, {"name": "value", "type": "int"}],
        },
        "unsupported_engine_asset": {
            "engine": "duckdb",  # Unregistered compute engine platform
            "format": "parquet",
            "storage_path": "data/metrics.parquet",
            "columns": [],
        },
        "invalid_pandas_format": {
            "engine": "pandas",
            "format": "xml",  # Unsupported format serialization for Pandas
            "storage_path": "data/metrics.xml",
            "columns": [],
        },
        "spark_missing_session_asset": {
            "engine": "spark",
            "format": "csv",
            "storage_path": "data/metrics.csv",
            "columns": [],
        },
    }

    with open(catalog_dir / "io_test.yaml", "w", encoding="utf-8") as f:
        yaml.dump(catalog_yaml, f)

    yield DataCatalog(catalog_path=catalog_dir)


# =============================================================================
# DATA LOADER ISOLATED BEHAVIORAL TESTS
# =============================================================================


def test_data_loader_relative_path_resolution(mock_catalog_environment: DataCatalog) -> None:
    """Asserts that relative paths in the catalog are correctly anchored to the discovered project root."""
    pd = pytest.importorskip("pandas")

    loader = DataLoader(catalog=mock_catalog_environment)
    df = loader.load("local_csv_relative")

    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["id", "value"]
    assert len(df) == 2


def test_data_loader_unsupported_engine_raises_value_error(mock_catalog_environment: DataCatalog) -> None:
    """Asserts that requesting a dataset mapped to an unrecognized compute engine triggers a clear ValueError."""
    loader = DataLoader(catalog=mock_catalog_environment)

    with pytest.raises(ValueError) as exc_info:
        loader.load("unsupported_engine_asset")

    assert "Unsupported storage processing platform backend: 'duckdb'" in str(exc_info.value)


def test_data_loader_unsupported_format_for_engine_raises_value_error(mock_catalog_environment: DataCatalog) -> None:
    """Asserts that requesting a valid engine but with an invalid format raises a semantic ValueError."""
    pytest.importorskip("pandas")
    loader = DataLoader(catalog=mock_catalog_environment)

    with pytest.raises(ValueError) as exc_info:
        loader.load("invalid_pandas_format")

    assert "Unsupported Pandas file layout standard parsing rule: 'xml'" in str(exc_info.value)


def test_data_loader_spark_missing_session_raises_value_error(mock_catalog_environment: DataCatalog) -> None:
    """Asserts that attempting a Spark load without an active running SparkSession raises an explicit error."""
    pytest.importorskip("pyspark")
    loader = DataLoader(catalog=mock_catalog_environment)

    # Force execution passing spark=None and ensure no active global session exists in this clean process thread
    with pytest.raises(ValueError) as exc_info:
        loader.load("spark_missing_session_asset", spark=None)

    assert "No distributed cluster session manager could be resolved globally" in str(exc_info.value)
