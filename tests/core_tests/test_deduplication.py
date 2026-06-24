"""Unit tests for all five methods of the Deduplicator core class."""

import pandas as pd
import pytest

from flint_core.core.deduplication import Deduplicator


@pytest.fixture(name="deduplicator")
def fixture_deduplicator():
    """Initializes the core deduplicator utility.

    Args:

    Returns:
        Deduplicator: An instance of the Deduplicator class.
    """
    return Deduplicator()


def test_pandas_deduplication_suite(deduplicator):
    """Tests all 5 deduplication methods using the Pandas engine."""
    # Base dataset for chronological and structural drops
    base_data = {
        "id": [1, 1, 2, 2],
        "val": ["A", "B", "C", "C"],
        "ts": ["2026-01-01", "2026-01-02", "2026-01-01", "2026-01-01"],
    }
    df_base = pd.DataFrame(base_data)

    # 1. Test latest
    df_latest = deduplicator.latest(df_base, keys=["id"], order_by_col="ts")
    assert df_latest[df_latest["id"] == 1].iloc[0]["val"] == "B"

    # 2. Test first
    df_first = deduplicator.first(df_base, keys=["id"], order_by_col="ts")
    assert df_first[df_first["id"] == 1].iloc[0]["val"] == "A"

    # 3. Test distinct
    df_distinct = deduplicator.distinct(df_base)
    assert len(df_distinct) == 3

    # 4. Test by_order (Advanced multi-sorting)
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

    # 5. Test combined (Row stitching/consolidation)
    stitch_data = {
        "id": [3, 3],
        "email": ["target@test.com", None],
        "phone": [None, "555-1234"],
    }
    df_stitch = pd.DataFrame(stitch_data)
    df_comb = deduplicator.combined(df_stitch, keys=["id"])
    assert df_comb.iloc[0]["email"] == "target@test.com"
    assert df_comb.iloc[0]["phone"] == "555-1234"


def test_spark_deduplication_suite(spark_session, deduplicator):
    """Tests all 5 deduplication methods using the PySpark engine."""
    # Base dataset for chronological and structural drops
    base_data = [
        (1, "A", "2026-01-01"),
        (1, "B", "2026-01-02"),
        (2, "C", "2026-01-01"),
        (2, "C", "2026-01-01"),
    ]
    schema_base = ["id", "val", "ts"]
    df_base = spark_session.createDataFrame(base_data, schema=schema_base)

    # 1. Test latest
    res_latest = deduplicator.latest(df_base, keys=["id"], order_by_col="ts").collect()
    row_1_l = [r for r in res_latest if r["id"] == 1][0]
    assert row_1_l["val"] == "B"

    # 2. Test first
    res_first = deduplicator.first(df_base, keys=["id"], order_by_col="ts").collect()
    row_1_f = [r for r in res_first if r["id"] == 1][0]
    assert row_1_f["val"] == "A"

    # 3. Test distinct
    res_distinct = deduplicator.distinct(df_base).collect()
    assert len(res_distinct) == 3

    # 4. Test by_order (Advanced multi-sorting)
    sort_data = [(1, False, 10), (1, True, 5)]
    schema_sort = ["id", "is_verified", "score"]
    df_sort = spark_session.createDataFrame(sort_data, schema=schema_sort)
    res_sort = deduplicator.by_order(
        df_sort, keys=["id"], order_by_cols=["is_verified"], ascending=False
    ).collect()
    assert res_sort[0]["is_verified"]

    # 5. Test combined (Row stitching/consolidation)
    stitch_data = [(3, "target@test.com", None), (3, None, "555-1234")]
    schema_stitch = ["id", "email", "phone"]
    df_stitch = spark_session.createDataFrame(stitch_data, schema=schema_stitch)
    res_comb = deduplicator.combined(df_stitch, keys=["id"]).collect()
    assert res_comb[0]["email"] == "target@test.com"
    assert res_comb[0]["phone"] == "555-1234"
