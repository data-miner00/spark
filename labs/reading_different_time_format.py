from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, TimestampType


spark = SparkSession.builder.appName("sandbox").getOrCreate()

schema_with_single_timestamp_column = StructType(
    [StructField("time", TimestampType(), True)]
)

df = (
    spark.read.option("header", "true")
    .schema(schema_with_single_timestamp_column)
    .csv("./time.csv")
)

df.show()

time_list = df.rdd.collect()

for time in time_list:
    print(time)
