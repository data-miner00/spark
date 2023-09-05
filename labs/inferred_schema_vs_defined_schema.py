"""
Experiment: To investigate the differences between inferred schema and hardcoded schema.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
    IntegerType,
)
from pyspark.sql import SparkSession


schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("age", StringType(), True),
        StructField(
            "address",
            ArrayType(
                StructType(
                    [
                        StructField("postcode", IntegerType(), True),
                        StructField("line1", StringType(), True),
                        StructField("abc", StringType(), True),
                    ]
                )
            ),
            True,
        ),
    ]
)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("sandbox").getOrCreate()

    # create empty df with schema
    empty_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema=schema)

    # infer schema from data
    df = spark.read.json("tracked_data/data.json")

    df.printSchema()  # inferred data everything in alphabetical order
    empty_df.printSchema()
