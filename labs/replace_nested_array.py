from pyspark.sql import SparkSession, Row
from pyspark.sql.types import (
    StructType,
    StructField,
    ArrayType,
    MapType,
    StringType,
    IntegerType,
)
from pyspark.sql.functions import (
    col,
    when,
    udf,
    expr,
    array,
    lit,
    size,
    coalesce,
    struct,
)

# define struct with array
schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField(
            "item",
            ArrayType(
                StructType(
                    [
                        StructField("mood", StringType(), True),
                        StructField("money", IntegerType(), True),
                    ]
                )
            ),
            True,
        ),
    ]
)

# create a SparkSession
spark = SparkSession.builder.appName("sandbox").getOrCreate()

# dummy data with empty array
data = [
    ("123", "Kelly", [("happy", 100), ("normal", 20)]),
    ("124", "Mandy", []),
]

df = spark.createDataFrame(data=data, schema=schema)
df.show()

# replace empty array with default value
df.select(
    "id",
    "name",
    when(
        size(col("item")) == 0,
        array(struct(lit("sad").alias("mood"), lit(5).alias("money"))),
    )
    .otherwise(col("item"))
    .alias("item"),
).show()
