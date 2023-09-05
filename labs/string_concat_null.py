"""
Experiment: To investigate what happens when null is concatenated with a value
"""

from pyspark.sql import SparkSession
from pyspark.sql import types as t
from pyspark.sql import functions as f

schema = t.StructType(
    [
        t.StructField("id", t.StringType()),
        t.StructField("age", t.IntegerType()),
    ]
)

data = [
    ("1", 16),
    (None, 16),
    ("", 16),
]


if __name__ == "__main__":
    spark = SparkSession.builder.appName("sandbox").getOrCreate()
    df = spark.createDataFrame(data, schema=schema)

    df.select(f.concat(f.lit("hello_"), f.col("id"))).show()
