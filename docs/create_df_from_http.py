import requests
from dataclasses import dataclass
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


spark: SparkSession = SparkSession.builder.appName("sandbox").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


@dataclass
class Post:
    userId: int
    id: int
    title: str
    body: str


schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("userId", IntegerType(), True),
        StructField("title", StringType(), True),
        StructField("body", StringType(), True),
    ]
)

url = "https://jsonplaceholder.typicode.com/posts"

res = requests.get(url)


df = spark.createDataFrame((Row(**x) for x in res.json()), schema=schema)

print("Row count: %d" % df.count())

df.show()
