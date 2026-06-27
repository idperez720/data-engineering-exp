"""Unit tests for multi-format DataLoader and DataSaver engine enforcement."""

from __future__ import annotations

import decimal
from pathlib import Path
from typing import Any, Generator

import pytest
import yaml

from flint_core.core.catalog import DataCatalog
from flint_core.core.exceptions import UnsupportedBackendError
from flint_core.core.io import DataLoader, DataSaver


@pytest.fixture
def mock_advanced_io_environment(
    tmp_path: Path,
) -> Generator[DataCatalog, None, None]:
    """Scaffolds a comprehensive environment with nested options for testing."""
    catalog_dir = tmp_path / "conf" / "catalog"
    data_dir = tmp_path / "data"

    catalog_dir.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)

    with open(tmp_path / "pyproject.toml", "w", encoding="utf-8") as f:
        f.write('[project]\nname = "test-advanced-io"\n')

    # Seed baseline semi-colon separated CSV file
    csv_path = data_dir / "dataset.csv"
    csv_content = (
        "id;price;event_date;processed_at\n"
        "1;99.99;25/12/2026;2026-12-25 10:30:00\n"
        "2;150.50;01/01/2027;2027-01-01 14:45:00\n"
    )
    csv_path.write_text(csv_content, encoding="utf-8")

    catalog_yaml = {
        "csv_dataset": {
            "engine": "pandas",
            "format": "csv",
            "storage_path": "data/dataset.csv",
            "options": {"sep": ";", "encoding": "utf-8"},
            "columns": [
                {"name": "id", "type": "integer"},
                {"name": "price", "type": "decimal(10,2)"},
                {"name": "event_date", "type": "date", "format": "%d/%m/%Y"},
                {
                    "name": "processed_at",
                    "type": "timestamp",
                    "format": "%Y-%m-%d %H:%M:%S",
                    "timezone": "UTC",
                },
            ],
        },
        "pandas_strict_save": {
            "engine": "pandas",
            "format": "csv",
            "storage_path": "data/output/strict_pandas.csv",
            "options": {"sep": "|"},
            "columns": [
                {"name": "id", "type": "integer"},
                {"name": "value", "type": "string"},
            ],
        },
        "pandas_schemaless_save": {
            "engine": "pandas",
            "format": "csv",
            "storage_path": "data/output/schemaless_pandas.csv",
            "options": {"sep": "|"},
        },
        "spark_csv_dataset": {
            "engine": "spark",
            "format": "csv",
            "storage_path": "data/dataset.csv",
            "options": {"delimiter": ";"},
            "columns": [
                {"name": "id", "type": "integer"},
                {"name": "price", "type": "decimal(10,2)"},
                {"name": "event_date", "type": "date", "format": "dd/MM/yyyy"},
                {
                    "name": "processed_at",
                    "type": "timestamp",
                    "format": "yyyy-MM-dd HH:mm:ss",
                },
            ],
        },
        "spark_strict_save": {
            "engine": "spark",
            "format": "parquet",
            "storage_path": "data/output/strict_spark.parquet",
            "columns": [
                {"name": "id", "type": "integer"},
                {"name": "value", "type": "string"},
            ],
        },
        "spark_schemaless_save": {
            "engine": "spark",
            "format": "parquet",
            "storage_path": "data/output/schemaless_spark.parquet",
        },
        "unsupported_engine": {
            "engine": "duckdb",
            "format": "parquet",
            "storage_path": "data/dataset.parquet",
        },
    }

    with open(catalog_dir / "advanced_io.yaml", "w", encoding="utf-8") as f:
        yaml.dump(catalog_yaml, f)

    yield DataCatalog(catalog_path=catalog_dir)


def test_pandas_csv_loading_options_and_enforcement(
    mock_advanced_io_environment: DataCatalog,
) -> None:
    """Asserts option pass-through and column-level advanced types for CSV."""
    import datetime

    pd = pytest.importorskip("pandas")

    loader = DataLoader(catalog=mock_advanced_io_environment)
    df = loader.load("csv_dataset")

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert list(df.columns) == ["id", "price", "event_date", "processed_at"]
    assert df["id"].dtype == "Int64"
    assert isinstance(df["price"].iloc[0], decimal.Decimal)
    assert isinstance(df["event_date"].iloc[0], datetime.date)
    assert df["processed_at"].dt.tz is not None


def test_spark_csv_loading_options_and_enforcement(
    mock_advanced_io_environment: DataCatalog, spark_session: Any
) -> None:
    """Asserts options pass-through and column-level execution for Spark CSV."""
    pytest.importorskip("pyspark")

    loader = DataLoader(catalog=mock_advanced_io_environment)
    df = loader.load("spark_csv_dataset", spark=spark_session)

    assert "price" in df.columns
    schema_fields = {f.name: f.dataType.simpleString() for f in df.schema}
    assert schema_fields["id"] == "int"
    assert schema_fields["price"] == "decimal(10,2)"
    assert schema_fields["event_date"] == "date"
    assert schema_fields["processed_at"] == "timestamp"


def test_pandas_saver_strict_drops_extra_columns(
    mock_advanced_io_environment: DataCatalog,
) -> None:
    """Asserts that strict saving filters and truncates extra columns."""
    pd = pytest.importorskip("pandas")

    # DataFrame contains an extra untracked column
    test_df = pd.DataFrame({"id": [1], "value": ["A"], "extra_untracked_field": [999]})

    saver = DataSaver(catalog=mock_advanced_io_environment)
    saver.save(test_df, "pandas_strict_save", mode="overwrite")

    root = mock_advanced_io_environment.project_root
    expected_file = Path(root) / "data/output/strict_pandas.csv"

    # Read back to ensure extra column was cleanly dropped by enforcement
    read_back = pd.read_csv(expected_file, sep="|")
    assert list(read_back.columns) == ["id", "value"]


def test_pandas_saver_schemaless_keeps_all_columns(
    mock_advanced_io_environment: DataCatalog,
) -> None:
    """Asserts that schemaless saving bypasses column truncation filters."""
    pd = pytest.importorskip("pandas")

    test_df = pd.DataFrame({"dynamic_id": [5], "custom_metric": [10.5], "tag": ["alpha"]})

    saver = DataSaver(catalog=mock_advanced_io_environment)
    saver.save(test_df, "pandas_schemaless_save", mode="overwrite")

    root = mock_advanced_io_environment.project_root
    expected_file = Path(root) / "data/output/schemaless_pandas.csv"

    # Read back to ensure all dynamic columns are safely preserved
    read_back = pd.read_csv(expected_file, sep="|")
    assert list(read_back.columns) == ["dynamic_id", "custom_metric", "tag"]


def test_spark_saver_strict_drops_extra_columns(mock_advanced_io_environment: DataCatalog, spark_session: Any) -> None:
    """Asserts that Spark strict saving filters out untracked columns."""
    pytest.importorskip("pyspark")

    test_df = spark_session.createDataFrame([(1, "SparkStrict", 55.4)], ["id", "value", "extra_spark_field"])

    saver = DataSaver(catalog=mock_advanced_io_environment)
    saver.save(test_df, "spark_strict_save", mode="overwrite", spark=spark_session)

    root = mock_advanced_io_environment.project_root
    expected_path = Path(root) / "data/output/strict_spark.parquet"

    # Read back distributed file via raw spark session to verify projection
    read_back = spark_session.read.parquet(str(expected_path))
    assert list(read_back.columns) == ["id", "value"]


def test_spark_saver_schemaless_keeps_all_columns(
    mock_advanced_io_environment: DataCatalog, spark_session: Any
) -> None:
    """Asserts that Spark schemaless saving bypasses projection filters."""
    pytest.importorskip("pyspark")

    test_df = spark_session.createDataFrame([(10, "Gold", True)], ["account_id", "tier", "active_flag"])

    saver = DataSaver(catalog=mock_advanced_io_environment)
    saver.save(
        test_df,
        "spark_schemaless_save",
        mode="overwrite",
        spark=spark_session,
    )

    root = mock_advanced_io_environment.project_root
    expected_path = Path(root) / "data/output/schemaless_spark.parquet"

    read_back = spark_session.read.parquet(str(expected_path))
    assert list(read_back.columns) == ["account_id", "tier", "active_flag"]


def test_pandas_data_saver_collision_error(
    mock_advanced_io_environment: DataCatalog,
) -> None:
    """Asserts that save mode='error' raises on pre-existing paths."""
    pd = pytest.importorskip("pandas")
    test_df = pd.DataFrame({"id": [1]})

    saver = DataSaver(catalog=mock_advanced_io_environment)

    with pytest.raises(FileExistsError):
        saver.save(test_df, "csv_dataset", mode="error")


def test_unsupported_engine_raises_error(
    mock_advanced_io_environment: DataCatalog,
) -> None:
    """Asserts that an unregistered engine throws UnsupportedBackendError."""
    loader = DataLoader(catalog=mock_advanced_io_environment)

    with pytest.raises(UnsupportedBackendError):
        loader.load("unsupported_engine")
