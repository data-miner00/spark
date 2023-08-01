"""
Experiment: Merging dataframes with different number of columns
1. Merge from dataframes with minimal columns
2. Merge with dataframes with even more columns
"""

from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("sandbox").getOrCreate()

schema = StructType(
    [
        StructField("col1", StringType(), True),
        StructField("col2", StringType(), True),
    ]
)

schema2 = StructType(
    [
        StructField("col1", StringType(), True),
        StructField("col2", StringType(), True),
        StructField("col3", StringType(), True),
    ]
)


data = [("val1", "val2")]

data2 = [
    (
        "v2al1",
        "v2al2",
        "v2al3",
    )
]

df = spark.createDataFrame(data=data, schema=schema)
df2 = spark.createDataFrame(data=data2, schema=schema2)

final = df.unionByName(df2, allowMissingColumns=True)

final.show()

# Merging dataframes with even more columns
schema3 = StructType(
    [
        StructField("col1", StringType(), True),
        StructField("col2", StringType(), True),
        StructField("col3", StringType(), True),
        StructField("col4", StringType(), True),
        StructField("col5", StringType(), True),
        StructField("col6", StringType(), True),
    ]
)

data3 = [
    (
        "v3al1",
        "v3al2",
        "v3al3",
        "v3al4",
        "v3al5",
        "v3al6",
    )
]

df3 = spark.createDataFrame(data=data3, schema=schema3)

merged = final.unionByName(df3, allowMissingColumns=True)

merged.show()
