"""This module implements Slowly Changing Dimension Type 2 (SCD2) logic using Pandas."""

from typing import List

import pandas as pd


class PandasSCD2Processor:
    """Handles Slowly Changing Dimension Type 2 (SCD2) logic using Pandas.

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
        """Initializes the PandasSCD2Processor with configuration parameters.

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

    def process(self, df_new: pd.DataFrame, df_existing: pd.DataFrame) -> pd.DataFrame:
        """Orchestrates the SCD2 pipeline process and returns result.

        Args:
            df_new(pd.DataFrame): Incoming batch of data.
            df_existing(pd.DataFrame): Current state of dimension.

        Returns:
            pd.DataFrame: Unified DataFrame with all processed records.
        """
        if not self.compare_columns:
            raise ValueError("compare_columns cannot be empty for SCD2 processing.")

        # Filtrar solo los registros activos de la dimensión histórica
        # Avoid equality comparison to True; use truthiness check instead
        df_active = df_existing[df_existing[self.current_flag_col]]

        # Indexar por llaves de negocio para aprovechar alineación de Pandas
        df_exist_idx = df_active.set_index(self.business_keys)
        df_new_idx = df_new.set_index(self.business_keys)

        # Identificar segmentos por llaves utilizando operaciones de conjuntos
        new_keys = df_new_idx.index.difference(df_exist_idx.index)
        common_keys = df_new_idx.index.intersection(df_exist_idx.index)
        preserved_keys = df_exist_idx.index.difference(df_new_idx.index)

        # Separar registros comunes entre cambiados y no cambiados
        changed_keys, unchanged_keys = self._split_common_keys(
            df_new_idx, df_exist_idx, common_keys
        )

        # Construir cada segmento del SCD2
        df_new_only = self._get_new_records(df_new_idx, new_keys)
        df_changed = self._get_changed_records(df_new_idx, changed_keys)
        df_expired = self._get_expired_records(df_new_idx, df_exist_idx, changed_keys)
        df_unchanged = self._get_unchanged_records(df_exist_idx, unchanged_keys)
        df_preserved = self._get_preserved_records(df_exist_idx, preserved_keys)

        # Consolidar y ordenar columnas según el esquema original
        target_cols = df_existing.columns.tolist()
        return pd.concat(
            [df_new_only, df_changed, df_expired, df_unchanged, df_preserved],
            ignore_index=True,
        )[target_cols]

    def _split_common_keys(
        self,
        df_new_idx: pd.DataFrame,
        df_exist_idx: pd.DataFrame,
        common_keys: pd.Index,
    ) -> tuple:
        """Splits common keys into changed and unchanged categories.

        Args:
            df_new_idx(pd.DataFrame): New data indexed by business keys.
            df_exist_idx(pd.DataFrame): Active existing data indexed by keys.
            common_keys(pd.Index): Intersection of business keys.

        Returns:
            tuple: A tuple containing (changed_keys, unchanged_keys).
        """
        df_new_common = df_new_idx.loc[common_keys]
        df_exist_common = df_exist_idx.loc[common_keys]

        change_mask = pd.Series(False, index=common_keys)
        for col in self.compare_columns:
            change_mask |= df_new_common[col] != df_exist_common[col]

        return common_keys[change_mask], common_keys[~change_mask]

    def _get_new_records(
        self, df_new_idx: pd.DataFrame, new_keys: pd.Index
    ) -> pd.DataFrame:
        """Extracts and formats brand new records.

        Args:
            df_new_idx(pd.DataFrame): New data indexed by business keys.
            new_keys(pd.Index): Keys that only exist in the new batch.

        Returns:
            pd.DataFrame: Formatted dataframe for new insertions.
        """
        df_new_only = df_new_idx.loc[new_keys].reset_index()
        df_new_only[self.effective_date_col] = df_new_only[self.input_timestamp_col]
        df_new_only[self.end_date_col] = self.max_date
        df_new_only[self.current_flag_col] = True
        return df_new_only

    def _get_changed_records(
        self, df_new_idx: pd.DataFrame, changed_keys: pd.Index
    ) -> pd.DataFrame:
        """Extracts and formats new active versions of changed records.

        Args:
            df_new_idx(pd.DataFrame): New data indexed by business keys.
            changed_keys(pd.Index): Keys with detected attribute modifications.

        Returns:
            pd.DataFrame: Formatted dataframe for updated active rows.
        """
        df_changed = df_new_idx.loc[changed_keys].reset_index()
        df_changed[self.effective_date_col] = df_changed[self.input_timestamp_col]
        df_changed[self.end_date_col] = self.max_date
        df_changed[self.current_flag_col] = True
        return df_changed

    def _get_expired_records(
        self,
        df_new_idx: pd.DataFrame,
        df_exist_idx: pd.DataFrame,
        changed_keys: pd.Index,
    ) -> pd.DataFrame:
        """Closes historical records by updating end date and flag.

        Args:
            df_new_idx(pd.DataFrame): New data indexed by business keys.
            df_exist_idx(pd.DataFrame): Active existing data indexed by keys.
            changed_keys(pd.Index): Keys with detected attribute modifications.

        Returns:
            pd.DataFrame: Formatted dataframe for expired historical rows.
        """
        df_expired = df_exist_idx.loc[changed_keys].reset_index()
        closing_timestamps = df_new_idx.loc[
            changed_keys, self.input_timestamp_col
        ].values

        df_expired[self.end_date_col] = closing_timestamps
        df_expired[self.current_flag_col] = False
        return df_expired

    def _get_unchanged_records(
        self, df_exist_idx: pd.DataFrame, unchanged_keys: pd.Index
    ) -> pd.DataFrame:
        """Extracts active records that remain identical.

        Args:
            df_exist_idx(pd.DataFrame): Active existing data indexed by keys.
            unchanged_keys(pd.Index): Keys with no attribute modifications.

        Returns:
            pd.DataFrame: Existing active rows without modifications.
        """
        return df_exist_idx.loc[unchanged_keys].reset_index()

    def _get_preserved_records(
        self, df_exist_idx: pd.DataFrame, preserved_keys: pd.Index
    ) -> pd.DataFrame:
        """Extracts active records completely missing from the incoming batch.

        Args:
            df_exist_idx(pd.DataFrame): Active existing data indexed by keys.
            preserved_keys(pd.Index): Keys missing from the new batch.

        Returns:
            pd.DataFrame: Existing active rows that must be preserved.
        """
        return df_exist_idx.loc[preserved_keys].reset_index()
