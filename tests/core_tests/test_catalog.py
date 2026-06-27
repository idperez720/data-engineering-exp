"""Unit tests for the flint-core modular data catalog engine orchestration layers."""

from pathlib import Path
from typing import Any, Generator, Set

import pytest
import yaml

from flint_core.core.catalog import (
    AdapterRegistry,
    BaseAdapter,
    ColumnDefinition,
    DataCatalog,
    DatasetConfiguration,
)
from flint_core.core.exceptions import CatalogParseError, ColumnValidationError

# =============================================================================
# PYTEST FIXTURES UTILITIES
# =============================================================================


@pytest.fixture
def workspace_setup(tmp_path: Path) -> Generator[Path, None, None]:
    """Scaffolds a mock declarative layout tracking separate configuration targets.

    Args:
        tmp_path: Core pytest temporary directory tracker injection.

    Yields:
        Path: Operational temporary root layout anchored path reference.
    """
    catalog_dir = tmp_path / "conf" / "catalog"
    catalog_dir.mkdir(parents=True, exist_ok=True)

    # Inject baseline pyproject.toml configuration anchor to support discovery
    with open(tmp_path / "pyproject.toml", "w", encoding="utf-8") as f:
        f.write('[project]\nname = "test-flint-pipeline"\n')

    # Construct split modular micro-configurations sheets targets
    customers_yaml = {
        "customers": {
            "description": "Production golden customers database schema.",
            "format": "parquet",
            "engine": "pandas",
            "storage_path": "data/customers.parquet",
            "columns": [{"name": "customer_id", "type": "string"}, {"name": "email", "type": "string"}],
        }
    }

    orders_yaml = {
        "orders": {
            "description": "High-volume business transactional ingestion track.",
            "format": "csv",
            "engine": "spark",
            "storage_path": "data/orders.csv",
            "columns": [{"name": "order_id", "type": "long"}, {"name": "amount", "type": "double"}],
        }
    }

    with open(catalog_dir / "customers.yaml", "w", encoding="utf-8") as f:
        yaml.dump(customers_yaml, f)

    with open(catalog_dir / "orders.yaml", "w", encoding="utf-8") as f:
        yaml.dump(orders_yaml, f)

    yield tmp_path


# =============================================================================
# METAPROGRAMMING & SLOTS EFFICIENCY ASSERTIONS
# =============================================================================


def test_entities_slots_optimization_enforcement() -> None:
    """Validates memory protection layer prohibiting dynamic internal dictionary allocation."""
    column = ColumnDefinition(name="test_col")
    dataset = DatasetConfiguration(
        name="test_ds", engine="pandas", data_format="csv", storage_path="fake/path", columns=[], metadata={}
    )

    with pytest.raises(AttributeError):
        column.__dict__  # type: ignore[attr-defined]

    with pytest.raises(AttributeError):
        dataset.__dict__  # type: ignore[attr-defined]

    with pytest.raises(AttributeError):
        column.arbitrary_runtime_attribute = "corrupt"  # type: ignore[attr-defined]


# =============================================================================
# INVERSION OF CONTROL EXTENSIBILITY POOLS TESTS
# =============================================================================


def test_adapter_registry_third_party_plug_in_extension() -> None:
    """Validates framework capabilities to incorporate external analytical adapters targets."""

    class MockPolarsDataFrame:
        def __init__(self) -> None:
            self.columns = ["polars_id", "polars_metric"]

    # Naming the class with 'Core_Tests' matches this test module's namespace string
    class Core_TestsAdapter(BaseAdapter):
        def extract_columns(self, df: Any) -> Set[str]:
            return set(df.columns)

    mock_df = MockPolarsDataFrame()
    resolved_adapter = AdapterRegistry.resolve_adapter(mock_df)

    assert isinstance(resolved_adapter, Core_TestsAdapter)
    assert resolved_adapter.extract_columns(mock_df) == {"polars_id", "polars_metric"}


def test_unsupported_dataframe_matrix_raises_type_error() -> None:
    """Validates that scanning unregistered objects structure triggers explicit TypeError exceptions."""
    anonymous_raw_object = ["not", "a", "dataframe"]

    with pytest.raises(TypeError) as exc_info:
        AdapterRegistry.resolve_adapter(anonymous_raw_object)

    assert "No catalog verification adapter registered" in str(exc_info.value)


# =============================================================================
# CATALOG STRUCTURAL PARSING AND CORE OPERATIONS TESTS
# =============================================================================


def test_data_catalog_modular_discovery_and_containment(workspace_setup: Path) -> None:
    """Asserts capabilities resolving multi-file declarative datasets clusters maps."""
    catalog = DataCatalog(catalog_path=workspace_setup / "conf" / "catalog")

    assert "customers" in catalog
    assert "orders" in catalog
    assert "non_existent" not in catalog

    assert sorted(catalog.dataset_names) == ["customers", "orders"]


def test_data_catalog_square_bracket_and_descriptor_access(workspace_setup: Path) -> None:
    """Validates dynamic descriptor metaprogramming and bracket index parameters fetching lookups."""
    catalog = DataCatalog(catalog_path=workspace_setup / "conf" / "catalog")

    # Assert square bracket getter routing interface pipeline
    ds_via_bracket = catalog["customers"]

    # Assert dynamic descriptor proxy fluid dot-notation lookups
    ds_via_descriptor = catalog.customers  # type: ignore[attr-defined]

    assert ds_via_bracket is ds_via_descriptor
    assert isinstance(ds_via_descriptor, DatasetConfiguration)
    assert ds_via_descriptor.engine == "pandas"
    assert ds_via_descriptor.format == "parquet"
    assert ds_via_descriptor.column_names == ["customer_id", "email"]


def test_malformed_yaml_syntax_raises_parse_error(tmp_path: Path) -> None:
    """Asserts tracking corrupted syntax configurations parameters safely aborts execution."""
    broken_dir = tmp_path / "broken_catalog"
    broken_dir.mkdir()

    with open(broken_dir / "corrupt.yaml", "w", encoding="utf-8") as f:
        f.write("customers: \n  [unbalanced bracket syntax breaking parser: ")

    with pytest.raises(CatalogParseError):
        DataCatalog(catalog_path=broken_dir)


def test_missing_mandatory_keys_raises_key_error(tmp_path: Path) -> None:
    """Asserts omitting baseline engine parameter flags raises descriptive KeyError states."""
    invalid_dir = tmp_path / "invalid_catalog"
    invalid_dir.mkdir()

    incomplete_yaml = {
        "incomplete_dataset": {
            "format": "csv",
            "storage_path": "data/isolated.csv",
            # Missing mandatory 'engine' mapping flag parameters
        }
    }

    with open(invalid_dir / "incomplete.yaml", "w", encoding="utf-8") as f:
        yaml.dump(incomplete_yaml, f)

    with pytest.raises(KeyError) as exc_info:
        DataCatalog(catalog_path=invalid_dir)

    assert "metadata must contain 'engine'" in str(exc_info.value)


# =============================================================================
# CONCURRENT AND ASYNC NON-BLOCKING EXECUTION TESTS
# =============================================================================


@pytest.mark.asyncio
async def test_async_catalog_reloading_loop(workspace_setup: Path) -> None:
    """Asserts non-blocking concurrent updating routines dispatch on background execution threads pools."""
    catalog = DataCatalog(catalog_path=workspace_setup / "conf" / "catalog")
    target_path = workspace_setup / "conf" / "catalog"

    # Dispatch non-blocking asynchronous executor state synchronization pipeline
    await catalog.reload_catalog_async(target_path)

    assert "orders" in catalog
    assert catalog.orders.engine == "spark"  # type: ignore[attr-defined]


# =============================================================================
# REAL CONCRETE ADAPTER VALIDATION TESTING PLUGS (Zero global state flags)
# =============================================================================


def test_pandas_adapter_validation_flow(workspace_setup: Path) -> None:
    """Asserts that schema verification routines interact natively with real local Pandas DataFrames."""
    # Elite approach: Lazy import and clean auto-skip if dependency is uninstalled
    pd = pytest.importorskip("pandas")

    catalog = DataCatalog(catalog_path=workspace_setup / "conf" / "catalog")

    valid_pandas_data = pd.DataFrame({"customer_id": ["C1"], "email": ["info@alpha.com"]})
    invalid_pandas_data = pd.DataFrame({"wrong_id_key": ["C1"], "email": ["info@alpha.com"]})

    assert catalog.customers.validate_schema(valid_pandas_data) is True  # type: ignore[attr-defined]

    with pytest.raises(ColumnValidationError) as exc_info:
        catalog.customers.validate_schema(invalid_pandas_data)  # type: ignore[attr-defined]

    assert "Missing expected catalog columns" in str(exc_info.value)


def test_pyspark_adapter_validation_flow(workspace_setup: Path, spark_session: Any) -> None:
    """Asserts that schema verification routines interact natively with distributed Spark plans."""
    # Elite approach: Lazy skip validation tracking contextual session injections
    pytest.importorskip("pyspark")

    catalog = DataCatalog(catalog_path=workspace_setup / "conf" / "catalog")

    valid_spark_records = [{"order_id": 101, "amount": 250.50}]
    invalid_spark_records = [{"wrong_order_column": 101, "amount": 250.50}]

    valid_spark_df = spark_session.createDataFrame(valid_spark_records)
    invalid_spark_df = spark_session.createDataFrame(invalid_spark_records)

    assert catalog.orders.validate_schema(valid_spark_df) is True  # type: ignore[attr-defined]

    with pytest.raises(ColumnValidationError):
        catalog.orders.validate_schema(invalid_spark_df)  # type: ignore[attr-defined]
