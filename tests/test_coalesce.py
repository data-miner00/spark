from datetime import datetime
from pyspark.sql import SparkSession
from labs.coalesce import transform, car_schema, country_schema
from tests.steps import assert_frame_equal


def test_coalesce(spark: SparkSession):
    car_data = [
        (1, "Audi", datetime(2020, 4, 5), "China", 123456),
        (2, "BMW", datetime(2015, 6, 7), "US", 324256),
        (3, "Chevrolet", datetime(2013, 1, 25), "US", 224206),
        (4, "Chevrolet", datetime(2013, 1, 25), "Austria", 594206),
    ]

    country_data = [
        ("US", 1, 1312345),
        ("China", 2, 1643345),
        ("Japan", 3, 1312345),
        ("Belarus", 4, 666445),
        ("Canada", 5, 26445),
        ("Czech", 6, 25565),
    ]

    expected_data = [
        (1, "Audi", datetime(2020, 4, 5), "China", 123456),
        (2, "BMW", datetime(2015, 6, 7), "US", 324256),
        (3, "Chevrolet", datetime(2013, 1, 25), "US", 224206),
        (4, "Chevrolet", datetime(2013, 1, 25), "Austria", 594206),
        (-1, "Unknown", datetime(2020, 1, 1), "Belarus", 0),
        (-1, "Unknown", datetime(2020, 1, 1), "Canada", 0),
        (-1, "Unknown", datetime(2020, 1, 1), "Czech", 0),
        (-1, "Unknown", datetime(2020, 1, 1), "Japan", 0),
    ]

    car_df = spark.createDataFrame(car_data, car_schema)
    country_df = spark.createDataFrame(country_data, country_schema)
    expected_df = spark.createDataFrame(expected_data, car_schema)

    actual_df = transform(car_df, country_df)

    assert_frame_equal(actual_df, expected_df)
