"""
Experiment: To investigate what happens when nulls are aggregated with sum
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

data_allnums = [
    ("1", 16),
    ("1", 16),
    ("1", 16),
]

data_gotnull = [
    ("1", None),
    ("1", 16),
    ("1", 16),
]

data_allnull = [
    ("1", None),
    ("1", None),
    ("1", None),
]

data_nullzero = [
    ("1", 0),
    ("1", None),
    ("1", None),
]

if __name__ == "__main__":
    spark = SparkSession.builder.appName("sandbox").getOrCreate()

    df_1 = spark.createDataFrame(data_allnums, schema=schema)
    df_1.groupBy("id").agg(f.sum("age")).show()

    df_2 = spark.createDataFrame(data_gotnull, schema=schema)
    df_2.groupBy("id").agg(f.sum("age")).show()

    df_3 = spark.createDataFrame(data_allnull, schema=schema)
    df_3.groupBy("id").agg(f.sum("age")).show()

    df_4 = spark.createDataFrame(data_nullzero, schema=schema)
    df_4.groupBy("id").agg(f.sum("age")).show()
