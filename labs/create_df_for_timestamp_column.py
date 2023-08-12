"""
Experiment: Create a dataframe that only have 1 column that holds Timestamp type
or an array of Timestamp type.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructField, StructType, TimestampType, ArrayType
from datetime import datetime
from typing import List


schema_with_single_timestamp_column = StructType(
    [StructField("time", TimestampType(), True)]
)

schema_with_single_timestamp_array_column = StructType(
    [StructField("times", ArrayType(TimestampType()), True)]
)


def create_datetime(date_str: str) -> datetime:
    return datetime.fromisoformat(date_str)


def create_single_timestamp_column_dataframe(
    spark: SparkSession, data: List[List[datetime]]
) -> DataFrame:
    return spark.createDataFrame(data, schema_with_single_timestamp_column)


def create_single_timestamp_array_column_dataframe(
    spark: SparkSession, data: List[List[datetime]]
) -> DataFrame:
    return spark.createDataFrame(data, schema_with_single_timestamp_array_column)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("sandbox").getOrCreate()

    time = [
        [create_datetime("2023-04-19 10:34:11")],
        [create_datetime("2023-04-20 10:34:11")],
    ]

    times = [
        [
            [
                create_datetime("2023-04-19 10:34:11"),
                create_datetime("2023-04-19 10:34:11"),
            ]
        ]
    ]

    time_df = create_single_timestamp_column_dataframe(spark, time)
    times_df = create_single_timestamp_array_column_dataframe(spark, times)

    time_df.show()
    times_df.show()
