from pyspark.sql.types import StringType, StructField, StructType, MapType, IntegerType
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F


spark = SparkSession.builder.appName("sandbox").getOrCreate()

schema = StructType(
    [
        StructField("Id", IntegerType(), False),
        StructField("Name", StringType(), False),
        StructField("Houses", MapType(StringType(), IntegerType(), False), False),
        StructField("Year", IntegerType(), False),
    ]
)

data = [
    (1, "first", {"a": 1, "b": 2}, 2020),
    (2, "second", {"a": 2, "b": 3}, 2020),
    (1, "first", {"a": 1, "b": 2}, 2020),
]

df = spark.createDataFrame(data, schema=schema)

df = df.select("*", F.explode("Houses"))

df = (
    df.groupBy("Id", "Name", "key", "value")
    .agg(F.sum(F.col("Year")).alias("SumYears"))
    .select("*")
    .groupBy(
        "Id",
        "Name",
        "SumYears",
    )
    .agg(F.collect_list(F.create_map(F.col("key"), F.col("value"))).alias("Houses"))
    .select("*")
)

df.show()
