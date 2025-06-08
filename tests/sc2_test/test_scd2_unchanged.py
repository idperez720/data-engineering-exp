"""Test cases for SCD Type 2 where the record remains unchanged."""

from pyspark.sql.session import SparkSession

from data_engineering_exp.spark.scd2 import apply_scd2_changes


def test_keep_unchanged_record(spark: SparkSession):
    """
    Test that an unchanged record is correctly retained as current in the SCD2 implementation.

    This test verifies that when a new record matches an existing record on both business keys
    and compare columns, the SCD2 logic does not create a new record or update the existing one.
    The existing record should remain current, with its 'is_current' flag set to True and its
    'end_date' unchanged.

    Args:
        spark (SparkSession): The Spark session fixture used to create test DataFrames.

    Asserts:
        - Only one record exists in the result.
        - The record remains current ('is_current' is True).
        - The 'end_date' remains as the default open-ended value ("9999-12-31").
    """
    existing_df = spark.createDataFrame(
        [
            ("1", "Alice", "2023-01-01", "9999-12-31", True),
        ],
        ["id", "name", "effective_date", "end_date", "is_current"],
    )

    new_df = spark.createDataFrame(
        [
            ("1", "Alice", "2025-06-08"),
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
    assert len(result) == 1
    assert result[0].is_current is True
    assert result[0].end_date == "9999-12-31"
