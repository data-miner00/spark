from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructField,
    StringType,
    StructType,
    IntegerType,
    BooleanType,
    ArrayType,
)
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("sandbox").getOrCreate()

schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("aliases", ArrayType(StringType()), True),
        StructField(
            "houses",
            ArrayType(
                StructType(
                    [
                        StructField("country", StringType(), True),
                        StructField("location", StringType(), True),
                        StructField("year", IntegerType(), True),
                        StructField("available", BooleanType(), True),
                    ]
                )
            ),
            True,
        ),
    ]
)

data = [
    (
        "id_001",
        "Randy",
        ["Rando", "Ron", "Don"],
        [("Germany", "Berlin", 2020, False), ("Belgium", "Brussels", 2023, True)],
    )
]

df = spark.createDataFrame(data, schema=schema)


df = df.select(
    "id",
    "name",
    F.array_join("aliases", "|").alias("aliases"),
    F.explode("houses").alias("houses"),
)

df = df.groupBy("id", "name", "aliases").agg(
    F.array_join(
        F.collect_list(
            F.concat(
                F.lit("country="),
                F.col("houses.country"),
                F.lit("|Location="),
                F.col("houses.location"),
                F.lit("|year="),
                F.col("houses.year").cast("string"),
                F.lit("|available="),
                F.col("houses.available").cast("string"),
            )
        ),
        ";",
    ).alias("houses")
)

print(df.first()["houses"])
