"""Test for inserting a new record in SCD Type 2 implementation."""

from data_engineering_exp.spark.scd2 import apply_scd2_changes


def test_insert_new_record(spark):
    """
    Test the insertion of a new record into an SCD2 table.

    This test verifies that when a new record (with a unique business key) is provided,
    the SCD2 logic correctly inserts it as a current record, while preserving existing records.

    Args:
        spark: A SparkSession fixture for creating test DataFrames.

    Asserts:
        - The resulting DataFrame contains both the existing and new records.
        - The new record is marked as current (`is_current` is True).
    """
    existing_df = spark.createDataFrame(
        [
            ("1", "Alice", "2023-01-01", "9999-12-31", True),
        ],
        ["id", "name", "effective_date", "end_date", "is_current"],
    )

    new_df = spark.createDataFrame(
        [
            ("2", "Bob", "2025-06-08"),
        ],
        ["id", "name", "input_date"],
    )

    result_df = apply_scd2_changes(
        spark_df_new=new_df,
        spark_df_existing=existing_df,
        business_keys=["id"],
        compare_columns=["name"],
        effective_date_col="effective_date",
        end_date_col="end_date",
        current_flag_col="is_current",
        input_timestamp_col="input_date",
    )

    result = result_df.collect()
    assert len(result) == 2
    assert any(r.id == "2" and r.is_current for r in result)
