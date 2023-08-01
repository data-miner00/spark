from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StringType,
    IntegerType,
    StructField,
    TimestampType,
    BooleanType,
)
from pyspark.sql.functions import col
from datetime import datetime

spark = SparkSession.builder.appName("sandbox").getOrCreate()

person_schema = StructType(
    [
        StructField("id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("birthday", TimestampType(), False),
        StructField("blood_type", StringType(), False),
    ]
)

property_schema = StructType(
    [
        StructField("id", StringType(), False),
        StructField("owner_id", StringType(), False),
        StructField("location", StringType(), False),
        StructField("is_premium", BooleanType(), False),
    ]
)

person_df = spark.createDataFrame(
    [
        ("1", "Marvin", datetime(2000, 10, 1), "O"),
        ("2", "Conan", datetime(1998, 9, 8), "AB"),
        ("3", "Jake", datetime(2000, 11, 12), "A"),
        ("4", "Sam", datetime(2000, 8, 26), "B"),
    ],
    schema=person_schema,
)


property_df = spark.createDataFrame(
    [
        ("h1", "1", "Anchorage", True),
        ("h2", "1", "Budapest", False),
        ("h3", "1", "Christchurch", False),
        ("h4", "3", "Vientine", True),
    ],
    schema=property_schema,
)

merge_df = person_df.join(
    property_df,
    [
        (person_df.id == property_df.owner_id),
    ],
    how="left",
).select(person_df.name, property_df.location, property_df.is_premium)

merge_df.show()


# With alias
merge_df2 = (
    person_df.alias("person")
    .join(
        property_df.alias("property"),
        [
            col("person.id") == col("property.owner_id"),
        ],
        how="left",
    )
    .select(col("person.name"), col("property.location"), col("property.is_premium"))
)

merge_df2.show()
