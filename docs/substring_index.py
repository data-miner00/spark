from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring_index

spark = SparkSession.builder.appName("sandbox").getOrCreate()

df = spark.createDataFrame([("a.b.c.d",)], ["s"])
df.select(substring_index(df.s, ".", 2).alias("s")).show()
df.select(substring_index(df.s, ".", -3).alias("s")).show()
df.select(substring_index(df.s, "-", -2).alias("s")).show()
