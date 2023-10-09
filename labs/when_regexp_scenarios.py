"""
Experiment: regexp_extract will ignore null
"""

from pyspark.sql.functions import when, regexp_extract, lit, col
from pyspark.sql import SparkSession
import pyspark.sql.types as t

spark = SparkSession.builder.appName("sandbox").getOrCreate()

schema = t.StructType([t.StructField("field", t.StringType())])

df = spark.createDataFrame(
    [
        ("12345",),
        ("12345",),
        (None,),
        ("random string",),
    ],
    schema=schema,
)

df.select(
    when(regexp_extract("field", r"(\d+)", 0) == "", lit(0))
    .otherwise(lit(1))
    .alias("calculated")
).show()
