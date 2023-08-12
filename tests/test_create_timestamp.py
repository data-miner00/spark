from labs.create_df_for_timestamp_column import (
    create_datetime,
    create_single_timestamp_array_column_dataframe,
    create_single_timestamp_column_dataframe,
    schema_with_single_timestamp_array_column,
    schema_with_single_timestamp_column,
)
from tests.steps import assert_frame_equal
from pyspark.sql import SparkSession
from datetime import datetime
import pytest


@pytest.mark.parametrize(
    "datetime_str, datetime_obj",
    [
        ("2023-04-19 10:34:11", datetime(2023, 4, 19, 10, 34, 11)),
        ("2023-04-20 10:34:11", datetime(2023, 4, 20, 10, 34, 11)),
    ],
)
def test_create_datetime(datetime_str: str, datetime_obj: datetime):
    assert create_datetime(datetime_str) == datetime_obj


def test_create_single_timestamp_column(spark: SparkSession):
    data = [
        [create_datetime("2023-04-19 10:34:11")],
        [create_datetime("2023-04-20 10:34:11")],
        [create_datetime("2023-04-20 20:20:00")],
    ]

    expected_data = [
        (datetime(2023, 4, 19, 10, 34, 11),),
        (datetime(2023, 4, 20, 10, 34, 11),),
        (datetime(2023, 4, 20, 20, 20, 00),),
    ]

    df = create_single_timestamp_column_dataframe(spark, data)

    expected_df = spark.createDataFrame(
        expected_data, schema_with_single_timestamp_column
    )

    assert_frame_equal(df, expected_df)


def test_create_single_timestamp_array_column(spark: SparkSession):
    data = [
        [
            [
                create_datetime("2023-04-19 10:34:11"),
                create_datetime("2023-04-19 10:34:11"),
                create_datetime("2023-04-20 20:20:00"),
            ]
        ],
        [[create_datetime("2022-04-20 20:20:00")]],
    ]

    expected_data = [
        (
            [
                datetime(2023, 4, 19, 10, 34, 11),
                datetime(2023, 4, 19, 10, 34, 11),
                datetime(2023, 4, 20, 20, 20, 00),
            ],
        ),
        ([datetime(2022, 4, 20, 20, 20, 0)],),
    ]

    actual_df = create_single_timestamp_array_column_dataframe(spark, data)
    expected_df = spark.createDataFrame(
        expected_data, schema_with_single_timestamp_array_column
    )

    assert_frame_equal(actual_df, expected_df)
