"""This module implements Slowly Changing Dimension Type 2 (SCD2) logic using PySpark."""

from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def apply_scd2_changes(
    spark_df_new: DataFrame,
    spark_df_existing: DataFrame,
    business_keys: List[str],
    compare_columns: List[str],
    effective_date_col: str,
    end_date_col: str,
    current_flag_col: str,
    input_timestamp_col: str,
) -> DataFrame:
    """
    Applies Slowly Changing Dimension Type 2 (SCD2) logic to detect and manage changes
    in a dimension table using PySpark.

    Parameters
    ----------
    spark_df_new : DataFrame
        Incoming snapshot DataFrame that contains a timestamp column with record extraction/update
        time.

    spark_df_existing : DataFrame
        Existing dimension table containing historical records with SCD2 fields.

    business_keys : List[str]
        Columns that uniquely identify a dimension record.

    compare_columns : List[str]
        Columns used to detect changes (excluding technical fields like dates or flags).

    effective_date_col : str
        Column that stores the start date of the current version of a record.

    end_date_col : str
        Column that stores the end date of the current version of a record.

    current_flag_col : str
        Column that indicates whether a record is the current one (Boolean or int).

    input_timestamp_col : str
        Column in `spark_df_new` representing the record's processing timestamp.

    Returns
    -------
    DataFrame
        A DataFrame with new and updated records according to SCD2 logic.
    """
    MAX_DATE = "9999-12-31"

    # Prepare the new DataFrame with SCD2 metadata
    df_new = (
        spark_df_new.withColumn(effective_date_col, F.col(input_timestamp_col))
        .withColumn(end_date_col, F.lit(MAX_DATE))
        .withColumn(current_flag_col, F.lit(True))
    )

    # Aliases for joining
    new_alias = "new"
    existing_alias = "exist"

    # Build join condition using business keys
    join_condition = [
        F.col(f"{new_alias}.{col}") == F.col(f"{existing_alias}.{col}")
        for col in business_keys
    ]

    # Join new data with existing data
    df_joined = df_new.alias(new_alias).join(
        spark_df_existing.alias(existing_alias), on=join_condition, how="left"
    )

    # Build condition to detect any changes in compare_columns
    change_condition = None
    for col in compare_columns:
        condition = F.col(f"{new_alias}.{col}") != F.col(f"{existing_alias}.{col}")
        change_condition = (
            condition if change_condition is None else change_condition | condition
        )

    # Rows where the key exists and the data has changed → need to expire old and insert new
    df_changed = (
        df_joined.filter(F.col(f"{existing_alias}.{current_flag_col}"))
        .filter(change_condition)  # type: ignore
        .select(
            [f"{new_alias}.{c}" for c in df_new.columns]
            + [
                f"{new_alias}.{effective_date_col}",
                f"{new_alias}.{end_date_col}",
                f"{new_alias}.{current_flag_col}",
            ]  # type: ignore
        )
    )  # type: ignore

    # Rows that are new (no matching business key in existing table)
    df_new_only = df_joined.filter(
        F.col(f"{existing_alias}.{business_keys[0]}").isNull()
    ).select(
        [f"{new_alias}.{c}" for c in df_new.columns]
        + [
            F.col(f"{new_alias}.{effective_date_col}"),
            F.col(f"{new_alias}.{end_date_col}"),
            F.col(f"{new_alias}.{current_flag_col}"),
        ]  # type: ignore
    )  # type: ignore

    # Rows that already exist and have not changed → keep as-is
    unchanged_condition = F.lit(True)
    for col in compare_columns:
        unchanged_condition &= F.col(f"{new_alias}.{col}") == F.col(
            f"{existing_alias}.{col}"
        )

    df_unchanged = (
        df_joined.filter(F.col(f"{existing_alias}.{current_flag_col}"))
        .filter(unchanged_condition)
        .select([f"{existing_alias}.{c}" for c in spark_df_existing.columns])
    )

    # Expire previous versions by updating end_date and marking them as not current
    df_expired = (
        df_joined.filter(F.col(f"{existing_alias}.{current_flag_col}"))
        .filter(change_condition)  # type: ignore
        .select([F.col(f"{existing_alias}.{c}") for c in spark_df_existing.columns])
        .withColumn(end_date_col, F.col(input_timestamp_col))
        .withColumn(current_flag_col, F.lit(False))
    )

    # Final output: combine unchanged + expired + new records (changed and new)
    final_df = (
        df_new_only.unionByName(df_changed)
        .unionByName(df_expired)
        .unionByName(df_unchanged)
    )

    return final_df
