"""Unit tests for the flint-core dual-interface data catalog orchestration."""

from pathlib import Path
from typing import Any, Generator, Set
from unittest.mock import patch

import pytest
import yaml

from flint_core.core.catalog import (
    AdapterRegistry,
    BaseAdapter,
    ColumnDefinition,
    DataCatalog,
    DatasetConfiguration,
)
from flint_core.core.exceptions import CatalogParseError


@pytest.fixture
def workspace_setup(tmp_path: Path) -> Generator[Path, None, None]:
    """Scaffolds a mock declarative layout tracking separate configuration."""
    catalog_dir = tmp_path / "conf" / "catalog"
    catalog_dir.mkdir(parents=True, exist_ok=True)

    with open(tmp_path / "pyproject.toml", "w", encoding="utf-8") as f:
        f.write('[project]\nname = "test-flint-pipeline"\n')

    customers_yaml = {
        "customers": {
            "description": "Production golden customers database schema.",
            "format": "csv",
            "engine": "pandas",
            "storage_path": "conf/catalog/customers.yaml",
            "columns": [
                {"name": "customer_id", "type": "string"},
                {"name": "email", "type": "string"},
            ],
        },
        "orders": {
            "description": "High-volume business transactional ingestion track.",
            "format": "csv",
            "engine": "pandas",
            "storage_path": "data/orders.csv",
            "columns": [{"name": "id"}],
        },
    }

    with open(catalog_dir / "customers.yaml", "w", encoding="utf-8") as f:
        yaml.dump(customers_yaml, f)

    yield tmp_path


def test_entities_slots_optimization_enforcement() -> None:
    """Validates memory protection layer prohibiting dynamic dict allocation."""
    column = ColumnDefinition(name="test_col")
    dataset = DatasetConfiguration(
        name="test_ds",
        engine="pandas",
        data_format="csv",
        storage_path="fake/path",
        columns=[],
        metadata={},
    )

    with pytest.raises(AttributeError):
        column.__dict__  # type: ignore[attr-defined]

    with pytest.raises(AttributeError):
        dataset.__dict__  # type: ignore[attr-defined]


def test_adapter_registry_third_party_plug_in_extension() -> None:
    """Validates framework capabilities to incorporate external adapters."""

    class MockCustomDataFrame:
        pass

    class MockecosystemAdapter(BaseAdapter):
        def extract_columns(self, df: Any) -> Set[str]:
            return {"virtual_id", "virtual_metric"}

    mock_df = MockCustomDataFrame()
    mock_df.__class__.__module__ = "mockecosystem.dataframe"

    resolved_adapter = AdapterRegistry.resolve_adapter(mock_df)
    assert isinstance(resolved_adapter, MockecosystemAdapter)


def test_unsupported_dataframe_matrix_raises_type_error() -> None:
    """Validates that scanning unregistered objects triggers TypeError."""
    anonymous_raw_object = ["not", "a", "dataframe"]

    with pytest.raises(TypeError) as exc_info:
        AdapterRegistry.resolve_adapter(anonymous_raw_object)

    assert "No catalog verification adapter registered" in str(exc_info.value)


def test_data_catalog_keys_containment(workspace_setup: Path) -> None:
    """Asserts containment checks and discovery flow of datasets."""
    catalog = DataCatalog(catalog_path=workspace_setup / "conf" / "catalog")
    assert "customers" in catalog
    assert "orders" in catalog
    assert "invalid" not in catalog
    assert sorted(catalog.dataset_names) == ["customers", "orders"]


def test_data_catalog_square_bracket_and_descriptor_access(
    workspace_setup: Path,
) -> None:
    """Validates dynamic descriptor metaprogramming interface pipelines."""
    catalog = DataCatalog(catalog_path=workspace_setup / "conf" / "catalog")
    ds_via_bracket = catalog["customers"]
    ds_via_descriptor = catalog.customers  # type: ignore[attr-defined]

    assert ds_via_bracket is ds_via_descriptor
    assert ds_via_descriptor.engine == "pandas"


def test_malformed_yaml_syntax_raises_parse_error(tmp_path: Path) -> None:
    """Asserts tracking corrupted syntax configurations safely aborts."""
    broken_dir = tmp_path / "broken_catalog"
    broken_dir.mkdir()

    with open(broken_dir / "corrupt.yaml", "w", encoding="utf-8") as f:
        f.write("customers: \n  [unbalanced bracket syntax breaking parser: ")

    with pytest.raises(CatalogParseError):
        DataCatalog(catalog_path=broken_dir)


def test_missing_mandatory_keys_raises_key_error(tmp_path: Path) -> None:
    """Asserts missing structural tokens raises formal KeyError states."""
    invalid_dir = tmp_path / "invalid_catalog"
    invalid_dir.mkdir()
    incomplete = {"incomplete": {"format": "csv", "storage_path": "a.csv"}}

    with open(invalid_dir / "incomplete.yaml", "w", encoding="utf-8") as f:
        yaml.dump(incomplete, f)

    with pytest.raises(KeyError):
        DataCatalog(catalog_path=invalid_dir)


@pytest.mark.asyncio
async def test_async_catalog_reloading_loop(workspace_setup: Path) -> None:
    """Asserts non-blocking concurrent updating routines."""
    catalog = DataCatalog(catalog_path=workspace_setup / "conf" / "catalog")
    await catalog.reload_catalog_async(workspace_setup / "conf" / "catalog")
    assert "customers" in catalog


def test_pandas_dual_interface_loading_flow(workspace_setup: Path) -> None:
    """Asserts symmetrical loading configurations via traditional facades."""
    pd = pytest.importorskip("pandas")
    catalog = DataCatalog(catalog_path=workspace_setup / "conf" / "catalog")

    with patch("flint_core.core.io.DataLoader.load") as mock_load:
        mock_reader = pd.DataFrame()
        mock_load.return_value = mock_reader

        df_facade = catalog.load("customers")
        df_fluent = catalog.customers.load()  # type: ignore[attr-defined]

        assert df_facade is mock_reader
        assert df_fluent is mock_reader


def test_detached_configuration_entity_raises_runtime_error() -> None:
    """Validates that invoking detached entities load protocols fails."""
    detached = DatasetConfiguration(
        name="orphan",
        engine="pandas",
        data_format="csv",
        storage_path="a.csv",
        columns=[],
        metadata={},
        catalog_ref=None,
    )
    with pytest.raises(RuntimeError):
        detached.load()


def test_get_spark_configuration_extraction(workspace_setup: Path) -> None:
    """Asserts processing capabilities extracting custom runtime options."""
    spark_file = workspace_setup / "conf" / "spark.yml"
    spark_opts = {"spark.sql.shuffle.partitions": "10", "spark.custom.key": True}

    with open(spark_file, "w", encoding="utf-8") as f:
        yaml.dump(spark_opts, f)

    catalog = DataCatalog(catalog_path=workspace_setup / "conf" / "catalog")
    configs = catalog.get_spark_configuration()

    assert configs["spark.sql.shuffle.partitions"] == "10"
    assert configs["spark.custom.key"] is True


def test_get_spark_configuration_missing_fallback(workspace_setup: Path) -> None:
    """Asserts missing yml environment targets gracefully degrade to empty dict."""
    catalog = DataCatalog(catalog_path=workspace_setup / "conf" / "catalog")
    assert catalog.get_spark_configuration() == {}


def test_get_spark_configuration_yaml_fallback(workspace_setup: Path) -> None:
    """Asserts alternate file extension fallback discovery verification."""
    spark_file = workspace_setup / "conf" / "spark.yaml"
    spark_opts = {"spark.sql.shuffle.partitions": "45"}

    with open(spark_file, "w", encoding="utf-8") as f:
        yaml.dump(spark_opts, f)

    catalog = DataCatalog(catalog_path=workspace_setup / "conf" / "catalog")
    assert catalog.get_spark_configuration()["spark.sql.shuffle.partitions"] == "45"


def test_get_spark_configuration_non_dict_payload(workspace_setup: Path) -> None:
    """Asserts invalid root structures gracefully degrade to empty dictionaries."""
    spark_file = workspace_setup / "conf" / "spark.yml"
    invalid_payload = ["spark.sql.shuffle.partitions", "20"]

    with open(spark_file, "w", encoding="utf-8") as f:
        yaml.dump(invalid_payload, f)

    catalog = DataCatalog(catalog_path=workspace_setup / "conf" / "catalog")
    assert catalog.get_spark_configuration() == {}
