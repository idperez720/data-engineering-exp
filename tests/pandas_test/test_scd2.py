"""Unit tests for the PandasSCD2Processor class."""

import pandas as pd
import pytest

from flint_core.pandas_core.scd2 import PandasSCD2Processor


@pytest.fixture(name="pandas_scd2_processor")
def fixture_pandas_scd2_processor():
    """Initializes the processor with standard metadata configurations."""
    return PandasSCD2Processor(
        business_keys=["id"],
        compare_columns=["email"],
        effective_date_col="start_date",
        end_date_col="end_date",
        current_flag_col="is_current",
        input_timestamp_col="updated_at",
    )


def test_pandas_scd2_process_pipeline(pandas_scd2_processor):
    """Tests the full Pandas SCD2 pipeline handling multiple state changes."""
    existing_data = {
        "id": [1, 2],
        "email": ["user1@test.com", "old_email@test.com"],
        "updated_at": ["2026-01-01", "2026-01-01"],
        "start_date": ["2026-01-01", "2026-01-01"],
        "end_date": ["9999-12-31", "9999-12-31"],
        "is_current": [True, True],
    }
    df_existing = pd.DataFrame(existing_data)

    new_data = {
        "id": [1, 2, 3],
        "email": ["user1@test.com", "new_email@test.com", "user3@test.com"],
        "updated_at": ["2026-02-01", "2026-02-01", "2026-02-01"],
    }
    df_new = pd.DataFrame(new_data)

    df_result = pandas_scd2_processor.process(df_new, df_existing)

    assert len(df_result) == 4

    row_3 = df_result[df_result["id"] == 3].iloc[0]
    assert row_3["is_current"]
    assert row_3["start_date"] == "2026-02-01"
    assert row_3["end_date"] == "9999-12-31"

    row_2_old = df_result[(df_result["id"] == 2) & (~df_result["is_current"])].iloc[0]
    assert row_2_old["email"] == "old_email@test.com"
    assert row_2_old["end_date"] == "2026-02-01"

    row_2_new = df_result[(df_result["id"] == 2) & (df_result["is_current"])].iloc[0]
    assert row_2_new["email"] == "new_email@test.com"
    assert row_2_new["start_date"] == "2026-02-01"
