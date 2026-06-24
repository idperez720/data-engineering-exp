"""
This module implements Slowly Changing Dimension Type 2 (SCD2) logic using
PySpark.
"""

from typing import List

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


class SparkSCD2Processor:
    """Handles Slowly Changing Dimension Type 2 (SCD2) logic using PySpark.

    This class encapsulates configuration parameters such as keys and metadata
    columns, allowing clean and repeatable executions of SCD2 updates.
    """

    def __init__(
        self,
        business_keys: List[str],
        compare_columns: List[str],
        effective_date_col: str = "effective_date",
        end_date_col: str = "end_date",
        current_flag_col: str = "is_current",
        input_timestamp_col: str = "input_timestamp",
        max_date: str = "9999-12-31",
    ):
        """Initializes the SparkSCD2Processor with configuration parameters.

        Args:
            business_keys(List[str]): List of columns that uniquely identify
                a record.
            compare_columns(List[str]): List of columns used to detect
                changes in the data.
            effective_date_col(str): Name of the effective start date column.
                Defaults to "effective_date".
            end_date_col(str): Name of the effective end date column.
                Defaults to "end_date".
            current_flag_col(str): Name of the boolean flag column
                indicating the active record. Defaults to "is_current".
            input_timestamp_col(str): Name of the column containing the
                ingestion/mutation timestamp. Defaults to "input_timestamp".
            max_date(str): The maximum date string used to populate active
                records. Defaults to "9999-12-31".

        Returns:
            None: This method initializes the class instance.
        """
        self.business_keys = business_keys
        self.compare_columns = compare_columns
        self.effective_date_col = effective_date_col
        self.end_date_col = end_date_col
        self.current_flag_col = current_flag_col
        self.input_timestamp_col = input_timestamp_col
        self.max_date = max_date

        self._new_alias = "new"
        self._exist_alias = "exist"

    def process(
        self, spark_df_new: DataFrame, spark_df_existing: DataFrame
    ) -> DataFrame:
        """Orchestrates the SCD2 pipeline process and returns result.

        Args:
            spark_df_new(DataFrame): Incoming batch of data.
            spark_df_existing(DataFrame): Current state of dimension.

        Returns:
            DataFrame: Unified DataFrame with all processed records.
        """
        target_columns = spark_df_existing.columns

        df_new_prepared = self._prepare_new_df(spark_df_new)
        df_joined = self._join_datasets(df_new_prepared, spark_df_existing)

        change_cond = self._build_change_condition()
        unchanged_cond = self._build_unchanged_condition()

        df_new_only = self._get_new_records(df_joined, target_columns)
        df_changed = self._get_changed_records(df_joined, change_cond, target_columns)
        df_unchanged = self._get_unchanged_records(
            df_joined, unchanged_cond, target_columns
        )
        df_expired = self._get_expired_records(df_joined, change_cond, target_columns)
        df_preserved = self._get_preserved_records(
            spark_df_new, spark_df_existing, target_columns
        )

        return (
            df_new_only.unionByName(df_changed)
            .unionByName(df_expired)
            .unionByName(df_preserved)
            .unionByName(df_unchanged)
        )

    def _prepare_new_df(self, df: DataFrame) -> DataFrame:
        """Prepares the incoming new dataset by injecting SCD2 metadata.

        Args:
            df(DataFrame): The raw incoming PySpark DataFrame.

        Returns:
            DataFrame: PySpark DataFrame with SCD2 columns added.
        """
        return (
            df.withColumn(self.effective_date_col, F.col(self.input_timestamp_col))
            .withColumn(self.end_date_col, F.lit(self.max_date))
            .withColumn(self.current_flag_col, F.lit(True))
        )

    def _join_datasets(self, df_new: DataFrame, df_existing: DataFrame) -> DataFrame:
        """Performs a left join between new data and historical dimension.

        Args:
            df_new(DataFrame): Prepared incoming DataFrame.
            df_existing(DataFrame): Historical dimension table.

        Returns:
            DataFrame: Joined PySpark DataFrame.
        """
        join_condition = [
            F.col(f"{self._new_alias}.{col}") == F.col(f"{self._exist_alias}.{col}")
            for col in self.business_keys
        ]
        return df_new.alias(self._new_alias).join(
            df_existing.alias(self._exist_alias),
            on=join_condition,
            how="left",
        )

    def _build_change_condition(self) -> Column:
        """Builds expression to identify mismatches across comparison columns.

        Args:

        Returns:
            Column: PySpark Column representing the logical OR condition.
        """
        if not self.compare_columns:
            raise ValueError("compare_columns cannot be empty for SCD2 processing.")

        # Inicializamos con la primera columna para asegurar un tipo Column
        first_col = self.compare_columns[0]
        change_condition = F.col(f"{self._new_alias}.{first_col}") != F.col(
            f"{self._exist_alias}.{first_col}"
        )

        # Iteramos sobre el resto de las columnas usando el operador |= (OR)
        for col in self.compare_columns[1:]:
            condition = F.col(f"{self._new_alias}.{col}") != F.col(
                f"{self._exist_alias}.{col}"
            )
            change_condition |= condition

        return change_condition

    def _build_unchanged_condition(self) -> Column:
        """Builds expression to verify that columns are strictly identical.

        Args:

        Returns:
            Column: PySpark Column representing the logical AND condition.
        """
        unchanged_condition = F.lit(True)
        for col in self.compare_columns:
            unchanged_condition &= F.col(f"{self._new_alias}.{col}") == F.col(
                f"{self._exist_alias}.{col}"
            )
        return unchanged_condition

    def _get_new_records(self, df_joined: DataFrame, columns: List[str]) -> DataFrame:
        """Extracts records whose business keys do not exist in dimension.

        Args:
            df_joined(DataFrame): The combined/joined PySpark DataFrame.
            columns(List[str]): List of output columns required.

        Returns:
            DataFrame: PySpark DataFrame containing brand new rows.
        """
        key_col = f"{self._exist_alias}.{self.business_keys[0]}"
        select_exprs = [F.col(f"{self._new_alias}.{c}").alias(c) for c in columns]
        return df_joined.filter(F.col(key_col).isNull()).select(select_exprs)

    def _get_changed_records(
        self, df_joined: DataFrame, change_cond: Column, columns: List[str]
    ) -> DataFrame:
        """Extracts newest version of records that suffered changes.

        Args:
            df_joined(DataFrame): The combined/joined PySpark DataFrame.
            change_cond(Column): PySpark Column condition for changes.
            columns(List[str]): List of output columns required.

        Returns:
            DataFrame: PySpark DataFrame containing active updated records.
        """
        is_current_cond = F.col(f"{self._exist_alias}.{self.current_flag_col}")
        select_exprs = [F.col(f"{self._new_alias}.{c}").alias(c) for c in columns]
        return (
            df_joined.filter(is_current_cond).filter(change_cond).select(select_exprs)
        )

    def _get_unchanged_records(
        self, df_joined: DataFrame, unchanged_cond: Column, columns: List[str]
    ) -> DataFrame:
        """Extracts existing active records that have no changes.

        Args:
            df_joined(DataFrame): The combined/joined PySpark DataFrame.
            unchanged_cond(Column): PySpark Column condition for identity.
            columns(List[str]): List of output columns required.

        Returns:
            DataFrame: PySpark DataFrame containing untouched active rows.
        """
        is_current_cond = F.col(f"{self._exist_alias}.{self.current_flag_col}")
        select_exprs = [F.col(f"{self._exist_alias}.{c}").alias(c) for c in columns]
        return (
            df_joined.filter(is_current_cond)
            .filter(unchanged_cond)
            .select(select_exprs)
        )

    def _get_expired_records(
        self, df_joined: DataFrame, change_cond: Column, columns: List[str]
    ) -> DataFrame:
        """Transforms existing active rows into historically closed versions.

        Args:
            df_joined(DataFrame): The combined/joined PySpark DataFrame.
            change_cond(Column): PySpark Column condition for changes.
            columns(List[str]): List of output columns required.

        Returns:
            DataFrame: PySpark DataFrame containing expired records.
        """
        is_current_cond = F.col(f"{self._exist_alias}.{self.current_flag_col}")

        select_exprs = []
        for c in columns:
            if c not in [self.end_date_col, self.current_flag_col]:
                select_exprs.append(F.col(f"{self._exist_alias}.{c}").alias(c))
            elif c == self.end_date_col:
                new_ts_col = f"{self._new_alias}.{self.input_timestamp_col}"
                select_exprs.append(F.col(new_ts_col).alias(self.end_date_col))
            else:
                select_exprs.append(F.lit(False).alias(self.current_flag_col))

        return (
            df_joined.filter(is_current_cond).filter(change_cond).select(select_exprs)
        )

    def _get_preserved_records(
        self, df_new: DataFrame, df_existing: DataFrame, columns: List[str]
    ) -> DataFrame:
        """Preserves active records missing from the new batch.

        Args:
            df_new(DataFrame): The raw incoming PySpark DataFrame.
            df_existing(DataFrame): The existing dimension table.
            columns(List[str]): List of output columns required.

        Returns:
            DataFrame: PySpark DataFrame containing unaffected rows.
        """
        is_current_cond = F.col(f"{self._exist_alias}.{self.current_flag_col}")
        select_exprs = [F.col(f"{self._exist_alias}.{c}").alias(c) for c in columns]
        return (
            df_existing.alias(self._exist_alias)
            .join(
                df_new.alias(self._new_alias),
                on=self.business_keys,
                how="left_anti",
            )
            .filter(is_current_cond)
            .select(select_exprs)
        )
