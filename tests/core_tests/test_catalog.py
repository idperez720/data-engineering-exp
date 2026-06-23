"""Unit tests for the directory-based DataCatalog abstraction layer."""

import os
from pathlib import Path

import pandas as pd
import pytest

from data_engineering_exp.core.catalog import DataCatalog


@pytest.fixture(name="catalog_dir_setup")
def fixture_catalog_dir_setup(tmp_path: Path) -> str:
    """Generates a temporary configuration tree containing multiple mini YAML files.

    Args:
        tmp_path(Path): Pytest built-in temporary directory factory fixture.

    Returns:
        str: System string path pointing to the created catalog directory.
    """
    conf_dir = tmp_path / "conf" / "catalog"
    conf_dir.mkdir(parents=True)

    # Inject a plain, minimal pyproject.toml layout inside the workspace root
    toml_file = tmp_path / "pyproject.toml"
    toml_content = "[project]\nname='user-data-project'\nversion='0.1.0'"
    toml_file.write_text(toml_content, encoding="utf-8")

    cust_content = """
    customers:
      description: "Customer Master Profile"
      format: "parquet"
      columns:
        - name: "id"
        - name: "email"
    """
    (conf_dir / "customers.yml").write_text(cust_content, encoding="utf-8")

    return str(conf_dir)


def test_catalog_folder_recursive_scanning(catalog_dir_setup: str) -> None:
    """Verifies metadata extraction and column list parsing properties.

    Args:
        catalog_dir_setup(str): Path to the temporary catalog directory.

    Returns:
        None: Test assertions verify system states.
    """
    catalog = DataCatalog(catalog_path=catalog_dir_setup)

    cust_meta = catalog.get_dataset_metadata("customers")
    assert cust_meta["format"] == "parquet"
    assert catalog.get_column_names("customers") == ["id", "email"]


def test_catalog_automatic_path_discovery(
    catalog_dir_setup: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verifies the catalog automatically finds configs via pyproject.toml roots.

    Args:
        catalog_dir_setup(str): Path to the temporary catalog directory.
        monkeypatch(MonkeyPatch): Pytest built-in environment patching fixture.

    Returns:
        None: Test assertions verify system states.
    """
    repo_root = os.path.dirname(os.path.dirname(catalog_dir_setup))
    notebooks_dir = os.path.join(repo_root, "src", "notebooks")
    os.makedirs(notebooks_dir, exist_ok=True)

    monkeypatch.chdir(notebooks_dir)

    catalog = DataCatalog()
    assert "customers" in catalog.dataset_names


def test_catalog_missing_pyproject_toml_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ensures a FileNotFoundError triggers if no pyproject.toml exists.

    Args:
        tmp_path(Path): Pytest built-in temporary directory factory fixture.
        monkeypatch(MonkeyPatch): Pytest built-in environment patching fixture.

    Returns:
        None: Test assertions verify error states.
    """
    empty_dir = tmp_path / "empty_project"
    empty_dir.mkdir()
    monkeypatch.chdir(str(empty_dir))

    with pytest.raises(FileNotFoundError, match="Configuration file"):
        DataCatalog()


def test_catalog_dataframe_structural_validation(
    catalog_dir_setup: str,
) -> None:
    """Tests schema structural validation over compliant and corrupted dataframes.

    Args:
        catalog_dir_setup(str): Path to the temporary catalog directory.

    Returns:
        None: Test assertions verify system states.
    """
    catalog = DataCatalog(catalog_path=catalog_dir_setup)

    # 1. Compliant dataframe case
    valid_data = {"id": [1, 2], "email": ["a@test.com", "b@test.com"]}
    df_valid = pd.DataFrame(valid_data)
    assert catalog.validate_schema_presence(df_valid, "customers") is True

    # 2. Corrupted dataframe case (missing email column)
    invalid_data = {"id": [1, 2], "age": [30, 40]}
    df_invalid = pd.DataFrame(invalid_data)
    with pytest.raises(ValueError, match="Missing expected catalog columns"):
        catalog.validate_schema_presence(df_invalid, "customers")
