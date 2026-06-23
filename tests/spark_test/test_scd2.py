"""Unit tests for the SparkSCD2Processor class."""

import pytest
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from data_engineering_exp.spark_core.scd2 import SparkSCD2Processor


@pytest.fixture(name="scd2_processor")
def fixture_scd2_processor():
    """Initializes the processor with standard metadata configurations."""
    return SparkSCD2Processor(
        business_keys=["id"],
        compare_columns=["email"],
        effective_date_col="start_date",
        end_date_col="end_date",
        current_flag_col="is_current",
        input_timestamp_col="updated_at",
    )


def test_scd2_process_pipeline(spark_session, scd2_processor):
    """Tests the full SCD2 pipeline handling inserts, updates and unchanges."""
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("email", StringType(), True),
            StructField("updated_at", StringType(), True),
            StructField("start_date", StringType(), True),
            StructField("end_date", StringType(), True),
            StructField("is_current", BooleanType(), True),
        ]
    )

    # 1. Datos existentes en la dimensión
    existing_data = [
        # Registro que no cambia
        (1, "user1@test.com", "2026-01-01", "2026-01-01", "9999-12-31", True),
        # Registro que va a cambiar (SCD2 Update)
        (2, "old_email@test.com", "2026-01-01", "2026-01-01", "9999-12-31", True),
    ]
    df_existing = spark_session.createDataFrame(existing_data, schema=schema)

    # 2. Nuevos datos que llegan del ETL (Solo traen id, email y updated_at)
    new_schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("email", StringType(), True),
            StructField("updated_at", StringType(), True),
        ]
    )
    new_data = [
        (1, "user1@test.com", "2026-02-01"),  # Unchanged
        (2, "new_email@test.com", "2026-02-01"),  # Changed
        (3, "user3@test.com", "2026-02-01"),  # New insert
    ]
    df_new = spark_session.createDataFrame(new_data, schema=new_schema)

    # 3. Ejecución del procesador POO
    df_result = scd2_processor.process(df_new, df_existing)
    results = df_result.collect()

    # 4. Aseveraciones (Assertions)
    # Deberíamos terminar con 5 registros en total:
    # 1 (unchanged) + 1 (old expired) + 1 (new active) + 1 (brand new insert) + 1
    # preserved (0 en este caso)
    assert len(results) == 4

    # Validar el registro nuevo (ID 3)
    row_3 = [r for r in results if r["id"] == 3][0]
    assert row_3["is_current"] is True
    assert row_3["start_date"] == "2026-02-01"
    assert row_3["end_date"] == "9999-12-31"

    # Validar la expiración del ID 2 viejo
    row_2_old = [
        r for r in results if r["id"] == 2 and r["email"] == "old_email@test.com"
    ][0]
    assert row_2_old["is_current"] is False
    assert row_2_old["end_date"] == "2026-02-01"

    # Validar la activación del ID 2 nuevo
    row_2_new = [
        r for r in results if r["id"] == 2 and r["email"] == "new_email@test.com"
    ][0]
    assert row_2_new["is_current"] is True
    assert row_2_new["start_date"] == "2026-02-01"
    assert row_2_new["end_date"] == "9999-12-31"
