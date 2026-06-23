"""Unit tests for the directory-based DataCatalog abstraction layer."""

import os

import pandas as pd
import pytest

from data_engineering_exp.core.catalog import DataCatalog


@pytest.fixture(name="catalog_dir_setup")
def fixture_catalog_dir_setup(tmp_path):
    """Generates a temporary configuration tree containing multiple mini YAML files.

    Args:
        tmp_path: Pytest built-in temporary directory factory fixture.

    Returns:
        str: System string path pointing to the created catalog directory.
    """
    conf_dir = tmp_path / "conf" / "catalog"
    conf_dir.mkdir(parents=True)

    cust_content = """
    customers:
      description: "Customer Master Profile"
      format: "parquet"
      columns:
        - name: "id"
        - name: "email"
    """
    (conf_dir / "customers.yml").write_text(cust_content, encoding="utf-8")

    orders_content = """
    orders:
      description: "Transactional Orders Ledger"
      format: "delta"
      columns:
        - name: "order_id"
        - name: "total"
    """
    (conf_dir / "orders.yaml").write_text(orders_content, encoding="utf-8")

    return str(conf_dir)


def test_catalog_folder_recursive_scanning(catalog_dir_setup):
    """Verifies that the engine aggregates definitions across multiple targets.

    Args:
        catalog_dir_setup(str): Path to the temporary catalog directory.

    Returns:
        None: Test assertions verify system states.
    """
    catalog = DataCatalog(catalog_dir_setup)

    cust_meta = catalog.get_dataset_metadata("customers")
    assert cust_meta["format"] == "parquet"

    orders_meta = catalog.get_dataset_metadata("orders")
    assert orders_meta["format"] == "delta"

    assert catalog.get_column_names("customers") == ["id", "email"]
    assert catalog.get_column_names("orders") == ["order_id", "total"]


def test_catalog_single_file_fallback_parsing(catalog_dir_setup):
    """Ensures single file processing behaves reliably when targeting isolated paths.

    Args:
        catalog_dir_setup(str): Path to the temporary catalog directory.

    Returns:
        None: Test assertions verify system states.
    """
    single_file = os.path.join(catalog_dir_setup, "customers.yml")
    catalog = DataCatalog(single_file)

    # Uses the new public property to avoid protected access warnings
    assert "customers" in catalog.dataset_names
    assert "orders" not in catalog.dataset_names


def test_catalog_dataframe_structural_validation(catalog_dir_setup):
    """Tests schema structural validation over dataframes under folder parsing.

    Args:
        catalog_dir_setup(str): Path to the temporary catalog directory.

    Returns:
        None: Test assertions verify system states.
    """
    catalog = DataCatalog(catalog_dir_setup)

    valid_data = {"id": [1, 2], "email": ["a@test.com", "b@test.com"]}
    df_valid = pd.DataFrame(valid_data)

    assert catalog.validate_schema_presence(df_valid, "customers") is True
