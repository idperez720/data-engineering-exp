"""Test cases for SCD Type 2 updates in PySpark."""

from pyspark.sql import SparkSession

from data_engineering_exp.spark.scd2 import apply_scd2_changes


def test_update_changed_record(spark: SparkSession):
    """
    Test the SCD2 update logic when a record with the same business key but a changed compare
    column is provided.

    This test verifies that:
    - The existing record is marked as not current and its end_date is set to the input_date of
    the new record.
    - A new record is created with the updated value, effective from the input_date, and
    marked as current.

    Args:
        spark (SparkSession): A SparkSession fixture for creating DataFrames.

    Asserts:
        - Only one current and one past record exist after the update.
        - The current record reflects the new value.
        - The past record's end_date matches the input_date of the new record.
    """
    existing_df = spark.createDataFrame(
        [
            ("1", "Alice", "2023-01-01", "9999-12-31", True),
        ],
        ["id", "name", "effective_date", "end_date", "is_current"],
    )

    new_df = spark.createDataFrame(
        [
            ("1", "Alicia", "2025-06-08"),
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

    current = result_df.filter("is_current = true").collect()
    past = result_df.filter("is_current = false").collect()

    assert len(current) == 1
    assert len(past) == 1
    assert current[0].name == "Alicia"
    assert past[0].end_date == "2025-06-08"
