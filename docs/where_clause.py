from pyspark.sql.functions import col, to_timestamp, lit
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructField,
    StructType,
    TimestampType,
    ArrayType,
    StringType,
)
from datetime import datetime
from dateutil import parser

spark = SparkSession.builder.appName("sandbox").getOrCreate()

schema = StructType(
    [
        StructField("id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("expiry_date", TimestampType(), False),
        StructField("type", StringType(), False),
    ]
)

df = spark.createDataFrame(
    [
        ("1", "Sandra", datetime(2023, 1, 1), "admin"),
        ("2", "Eko", datetime(2023, 1, 2), "user"),
        ("3", "May", datetime(2023, 2, 2), "read_only"),
        ("4", "Josh", datetime(2023, 2, 2), "anonymous"),
    ],
    schema=schema,
)

## Conditions
one_of = ["admin", "superadmin", "root"]
date = datetime(2023, 1, 1)

## Query
df.filter((col("expiry_date") == date) & (col("type").isin(one_of))).show()
