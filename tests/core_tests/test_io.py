"""Unit tests for the advanced multi-format DataLoader and DataSaver engines."""

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

    # Seed baseline JSON metrics file (records orientation)
    json_path = data_dir / "dataset.json"
    json_content = (
        "[\n"
        '  {"id": 1, "price": 99.99, "event_date": "25/12/2026", '
        '"processed_at": "2026-12-25 10:30:00"},\n'
        '  {"id": 2, "price": 150.50, "event_date": "01/01/2027", '
        '"processed_at": "2027-01-01 14:45:00"}\n'
        "]"
    )
    json_path.write_text(json_content, encoding="utf-8")

    catalog_yaml = {
        "csv_dataset": {
            "engine": "pandas",
            "format": "csv",
            "storage_path": "data/dataset.csv",
            "options": {
                "sep": ";",
                "encoding": "utf-8",
            },
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
        "json_dataset": {
            "engine": "pandas",
            "format": "json",
            "storage_path": "data/dataset.json",
            "options": {
                "orient": "records",
            },
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
        "pandas_save_dataset": {
            "engine": "pandas",
            "format": "csv",
            "storage_path": "data/output/saved_pandas.csv",
            "options": {
                "sep": "|",
            },
        },
        "spark_csv_dataset": {
            "engine": "spark",
            "format": "csv",
            "storage_path": "data/dataset.csv",
            "options": {
                "delimiter": ";",
            },
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
        "spark_save_dataset": {
            "engine": "spark",
            "format": "parquet",
            "storage_path": "data/output/saved_spark.parquet",
        },
        "unsupported_engine": {
            "engine": "duckdb",
            "format": "parquet",
            "storage_path": "data/dataset.parquet",
            "columns": [],
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

    # Verify options pass-through (sep=';') worked, resulting in correct columns
    assert list(df.columns) == ["id", "price", "event_date", "processed_at"]

    # Verify advanced column-level type enforcement
    assert df["id"].dtype == "Int64"
    assert isinstance(df["price"].iloc[0], decimal.Decimal)
    assert isinstance(df["event_date"].iloc[0], datetime.date)
    assert df["processed_at"].dt.tz is not None
    assert str(df["processed_at"].dt.tz) == "UTC"


def test_pandas_json_loading_with_nested_options(
    mock_advanced_io_environment: DataCatalog,
) -> None:
    """Asserts advanced column-level execution and options for Pandas JSON."""
    pd = pytest.importorskip("pandas")

    loader = DataLoader(catalog=mock_advanced_io_environment)
    df = loader.load("json_dataset")

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert df["id"].dtype == "Int64"
    assert isinstance(df["price"].iloc[0], decimal.Decimal)


def test_spark_csv_loading_options_and_enforcement(
    mock_advanced_io_environment: DataCatalog, spark_session: Any
) -> None:
    """Asserts options pass-through and column-level execution for Spark CSV."""
    pytest.importorskip("pyspark")

    loader = DataLoader(catalog=mock_advanced_io_environment)
    df = loader.load("spark_csv_dataset", spark=spark_session)

    # Verify options pass-through (delimiter=';') worked
    assert "price" in df.columns

    schema_fields = {f.name: f.dataType.simpleString() for f in df.schema}
    assert schema_fields["id"] == "int"
    assert schema_fields["price"] == "decimal(10,2)"
    assert schema_fields["event_date"] == "date"
    assert schema_fields["processed_at"] == "timestamp"


def test_data_loader_runtime_options_override(
    mock_advanced_io_environment: DataCatalog,
) -> None:
    """Asserts that runtime options dynamically override catalog options."""
    pd = pytest.importorskip("pandas")

    loader = DataLoader(catalog=mock_advanced_io_environment)

    # Pass an intentional wrong separator via runtime options to check override
    df = loader.load("csv_dataset", options={"sep": "|"})

    # Since it didn't split by ';', it should have a single combined column name
    assert "id;price;event_date;processed_at" in df.columns


def test_pandas_data_saver_flow(
    mock_advanced_io_environment: DataCatalog,
) -> None:
    """Asserts polymorphic saving capability for PandasEngine."""
    pd = pytest.importorskip("pandas")

    # Create dummy frame to persist
    test_df = pd.DataFrame({"id": [1, 2], "value": ["X", "Y"]})

    saver = DataSaver(catalog=mock_advanced_io_environment)
    saver.save(test_df, "pandas_save_dataset", mode="overwrite")

    # Resolve paths via project root boundaries using pure pathlib
    expected_file = Path(mock_advanced_io_environment.project_root) / ("data/output/saved_pandas.csv")

    assert expected_file.is_file()

    # Read back to verify file schema and structure
    read_back = pd.read_csv(expected_file, sep="|")
    assert list(read_back.columns) == ["id", "value"]


def test_pandas_data_saver_collision_error(
    mock_advanced_io_environment: DataCatalog,
) -> None:
    """Asserts that save mode='error' raises on pre-existing paths."""
    pd = pytest.importorskip("pandas")
    test_df = pd.DataFrame({"id": [1]})

    saver = DataSaver(catalog=mock_advanced_io_environment)

    # Writing to a path that we know exists (csv_dataset is data/dataset.csv)
    with pytest.raises(FileExistsError):
        saver.save(test_df, "csv_dataset", mode="error")


def test_spark_data_saver_flow(mock_advanced_io_environment: DataCatalog, spark_session: Any) -> None:
    """Asserts polymorphic saving capability for SparkEngine."""
    pytest.importorskip("pyspark")

    # Create dynamic distributed dataframe setup
    test_df = spark_session.createDataFrame(
        [
            (1, "SparkText"),
        ],
        ["id", "value"],
    )

    saver = DataSaver(catalog=mock_advanced_io_environment)
    saver.save(test_df, "spark_save_dataset", mode="overwrite", spark=spark_session)

    expected_path = Path(mock_advanced_io_environment.project_root) / ("data/output/saved_spark.parquet")

    # Spark directories validation check
    assert expected_path.exists()


def test_unsupported_engine_raises_error(
    mock_advanced_io_environment: DataCatalog,
) -> None:
    """Asserts that an unregistered engine throws UnsupportedBackendError."""
    loader = DataLoader(catalog=mock_advanced_io_environment)

    with pytest.raises(UnsupportedBackendError):
        loader.load("unsupported_engine")
