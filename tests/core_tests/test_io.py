"""Unit tests for the metadata-driven DataLoader core utility."""

from pathlib import Path
from typing import Any, Tuple

import pandas as pd
import pytest

from data_engineering_exp.core.catalog import DataCatalog
from data_engineering_exp.core.io import DataLoader


@pytest.fixture(name="loader_setup")
def fixture_loader_setup(tmp_path: Path) -> Tuple[str, Path]:
    """Sets up a mock project environment containing files and catalog schemas.

    Args:
        tmp_path(Path): Pytest built-in temporary directory factory fixture.

    Returns:
        Tuple[str, Path]: Path to the temporary catalog directory and root path.
    """
    conf_dir = tmp_path / "conf" / "catalog"
    conf_dir.mkdir(parents=True)

    toml_file = tmp_path / "pyproject.toml"
    toml_file.write_text("[project]\nname='test'\nversion='0.1.0'", encoding="utf-8")

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    sample_file = data_dir / "mock.csv"

    # Write a real physical file to disk to test Pandas/Spark reading hooks
    df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
    df.to_csv(sample_file, index=False)

    # Cleanly define both execution profiles within the declarative YAML file
    catalog_content = f"""
    mock_pandas:
      description: "Pandas CSV Mock"
      format: "csv"
      engine: "pandas"
      storage_path: "{sample_file.as_posix()}"
      columns:
        - name: "id"
        - name: "name"

    mock_spark:
      description: "PySpark CSV Mock"
      format: "csv"
      engine: "spark"
      storage_path: "{sample_file.as_posix()}"
      columns:
        - name: "id"
        - name: "name"
    """
    (conf_dir / "mock_data.yml").write_text(catalog_content, encoding="utf-8")

    return str(conf_dir), tmp_path


def test_dataloader_pandas_csv_execution(
    loader_setup: Tuple[str, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verifies that the loader correctly reads a dataset using Pandas.

    Args:
        loader_setup(Tuple[str, Path]): Fixture containing config paths.
        monkeypatch(MonkeyPatch): Pytest environment modifier utility.

    Returns:
        None: Test assertions verify system states.
    """
    catalog_path, root_path = loader_setup
    monkeypatch.chdir(str(root_path))

    catalog = DataCatalog(catalog_path=catalog_path)
    loader = DataLoader(catalog=catalog)

    df_res = loader.load("mock_pandas")
    assert isinstance(df_res, pd.DataFrame)
    assert len(df_res) == 2
    assert df_res.iloc[0]["name"] == "Alice"


def test_dataloader_spark_session_fallback_validation(
    loader_setup: Tuple[str, Path], spark_session: Any
) -> None:
    """Verifies that the PySpark dynamic loading router behaves correctly.

    Args:
        loader_setup(Tuple[str, Path]): Fixture containing config paths.
        spark_session(Any): Active PySpark local testing session fixture.

    Returns:
        None: Test assertions verify system states.
    """
    catalog_path, _ = loader_setup
    catalog = DataCatalog(catalog_path=catalog_path)

    loader = DataLoader(catalog=catalog)
    df_spark = loader.load("mock_spark", spark=spark_session)

    assert df_spark.count() == 2
    assert "name" in df_spark.columns
