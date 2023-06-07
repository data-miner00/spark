from pyspark.sql.functions import coalesce, col, lit
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType
)

spark = SparkSession.builder.appName("sandbox").getOrCreate()

car_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("produced", TimestampType(), False),
    StructField("country", StringType(), False),
    StructField("distance", IntegerType(), False),
])

country_schema = StructType([
    StructField("name", StringType(), False),
    StructField("id", IntegerType(), False),
    StructField("car_count", IntegerType(), False)
])

car_df = spark.createDataFrame([
    (1, "Audi", datetime(2020, 4, 5), "China", 123456),
    (2, "BMW", datetime(2015, 6, 7), "US", 324256),
    (3, "Chevrolet", datetime(2013, 1, 25), "US", 224206),
    (4, "Chevrolet", datetime(2013, 1, 25), "Austria", 594206),
], schema=car_schema)

country_df = spark.createDataFrame([
    ("US", 1, 1312345),
    ("China", 2, 1643345),
    ("Japan", 3, 1312345),
    ("Belarus", 4, 666445),
    ("Canada", 5, 26445),
], schema=country_schema)

merged_df = car_df.alias("car").join(
    country_df.alias("country"),
    on=[col("car.country") == col("country.name")],
    how="outer"
).select(
    coalesce(col("car.id"), lit(-1)).alias("id"),
    coalesce(col("car.name"), lit("Unknown")).alias("name"),
    coalesce(col("car.produced"), lit(datetime(2020, 1, 1))).alias("produced"),
    coalesce(col("car.country"), col("country.name")).alias("country")
)

merged_df.show()