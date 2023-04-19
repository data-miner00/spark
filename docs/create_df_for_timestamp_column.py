from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, TimestampType, ArrayType
from datetime import datetime


spark = SparkSession.builder.appName("sandbox").getOrCreate()


def create_datetime(date_str: str) -> datetime:
    return datetime.fromisoformat(date_str)


schema_with_single_timestamp_column = StructType(
    [StructField("time", TimestampType(), True)]
)

schema_with_single_timestamp_array_column = StructType(
    [StructField("times", ArrayType(TimestampType()), True)]
)


def create_single_timestamp_column_dataframe():
    time = [
        [create_datetime("2023-04-19 10:34:11")],
        [create_datetime("2023-04-20 10:34:11")],
    ]

    df = spark.createDataFrame(time, schema_with_single_timestamp_column)

    df.show()


def create_single_timestamp_array_column_dataframe():
    times = [
        [
            [
                create_datetime("2023-04-19 10:34:11"),
                create_datetime("2023-04-19 10:34:11"),
            ]
        ]
    ]

    df = spark.createDataFrame(times, schema_with_single_timestamp_array_column)

    df.show()


if __name__ == "__main__":
    create_single_timestamp_column_dataframe()
    create_single_timestamp_array_column_dataframe()
