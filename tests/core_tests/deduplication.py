"""Unit tests for the extended Deduplicator core class."""

import pandas as pd
import pytest

from data_engineering_exp.core.deduplication import Deduplicator


@pytest.fixture(name="deduplicator")
def fixture_deduplicator():
    """Initializes the core deduplicator utility."""
    return Deduplicator()


def test_pandas_advanced_deduplication(deduplicator):
    """Tests by_order and combined methods using Pandas."""
    # Data for custom sorting sorting test
    sort_data = {
        "id": [1, 1],
        "is_verified": [False, True],
        "score": [10, 5],
    }
    df_sort = pd.DataFrame(sort_data)
    df_res = deduplicator.by_order(
        df_sort, keys=["id"], order_by_cols=["is_verified"], ascending=False
    )
    assert df_res.iloc[0]["is_verified"]

    # Data for row stitching test (combining nulls)
    stitch_data = {
        "id": [2, 2],
        "email": ["target@test.com", None],
        "phone": [None, "555-1234"],
    }
    df_stitch = pd.DataFrame(stitch_data)
    df_comb = deduplicator.combined(df_stitch, keys=["id"])
    assert df_comb.iloc[0]["email"] == "target@test.com"
    assert df_comb.iloc[0]["phone"] == "555-1234"


def test_spark_advanced_deduplication(spark_session, deduplicator):
    """Tests by_order and combined methods using PySpark."""
    # Test by_order multi-sorting criteria
    sort_data = [(1, False, 10), (1, True, 5)]
    schema = ["id", "is_verified", "score"]
    df_spark = spark_session.createDataFrame(sort_data, schema=schema)

    res = deduplicator.by_order(
        df_spark, keys=["id"], order_by_cols=["is_verified"], ascending=False
    ).collect()
    assert res[0]["is_verified"]

    # Test row stitching consolidation
    stitch_data = [(2, "target@test.com", None), (2, None, "555-1234")]
    schema_stitch = ["id", "email", "phone"]
    df_stitch = spark_session.createDataFrame(stitch_data, schema=schema_stitch)

    res_comb = deduplicator.combined(df_stitch, keys=["id"]).collect()
    assert res_comb[0]["email"] == "target@test.com"
    assert res_comb[0]["phone"] == "555-1234"
