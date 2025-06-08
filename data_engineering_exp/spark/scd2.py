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
    """
    MAX_DATE = "9999-12-31"

    # Prepare the new DataFrame with SCD2 metadata
    df_new = (
        spark_df_new.withColumn(effective_date_col, F.col(input_timestamp_col))
        .withColumn(end_date_col, F.lit(MAX_DATE))
        .withColumn(current_flag_col, F.lit(True))
    )

    new_alias = "new"
    existing_alias = "exist"

    join_condition = [
        F.col(f"{new_alias}.{col}") == F.col(f"{existing_alias}.{col}")
        for col in business_keys
    ]

    df_joined = df_new.alias(new_alias).join(
        spark_df_existing.alias(existing_alias), on=join_condition, how="left"
    )

    # Detect changes in compare columns
    change_condition = None
    for col in compare_columns:
        condition = F.col(f"{new_alias}.{col}") != F.col(f"{existing_alias}.{col}")
        change_condition = (
            condition if change_condition is None else change_condition | condition
        )

    # Changed records (new version to insert)
    df_changed = (
        df_joined.filter(F.col(f"{existing_alias}.{current_flag_col}"))
        .filter(change_condition)
        .select([F.col(f"{new_alias}.{c}").alias(c) for c in spark_df_existing.columns])
    )

    # New records (not found in existing)
    df_new_only = df_joined.filter(
        F.col(f"{existing_alias}.{business_keys[0]}").isNull()
    ).select([F.col(f"{new_alias}.{c}").alias(c) for c in spark_df_existing.columns])

    # Unchanged records (keep as-is)
    unchanged_condition = F.lit(True)
    for col in compare_columns:
        unchanged_condition &= F.col(f"{new_alias}.{col}") == F.col(
            f"{existing_alias}.{col}"
        )

    df_unchanged = (
        df_joined.filter(F.col(f"{existing_alias}.{current_flag_col}"))
        .filter(unchanged_condition)
        .select(
            [F.col(f"{existing_alias}.{c}").alias(c) for c in spark_df_existing.columns]
        )
    )

    # Expire previous records
    df_expired = (
        df_joined.filter(F.col(f"{existing_alias}.{current_flag_col}"))
        .filter(change_condition)
        .select(
            [
                (
                    F.col(f"{existing_alias}.{c}").alias(c)
                    if c not in [end_date_col, current_flag_col]
                    else (
                        F.col(f"{new_alias}.{input_timestamp_col}").alias(end_date_col)
                        if c == end_date_col
                        else F.lit(False).alias(current_flag_col)
                    )
                )
                for c in spark_df_existing.columns
            ]
        )
    )
    # Records from existing table that are still current and not in new_df at all (preserve them)
    df_preserved = (
        spark_df_existing.alias(existing_alias)
        .join(spark_df_new.alias(new_alias), on=business_keys, how="left_anti")
        .filter(F.col(f"{existing_alias}.{current_flag_col}"))
        .select(
            [F.col(f"{existing_alias}.{c}").alias(c) for c in spark_df_existing.columns]
        )
    )

    return (
        df_new_only.unionByName(df_changed)
        .unionByName(df_expired)
        .unionByName(df_preserved)
        .unionByName(df_unchanged)
    )
