from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, MapType, IntegerType
from pyspark.sql.functions import to_json, col

spark = SparkSession.builder.appName("sandbox").getOrCreate()

schema = StructType(
    [
        StructField("Id", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("Scores", MapType(StringType(), IntegerType()), True),
        StructField("Age", IntegerType(), True),
    ]
)

df = spark.createDataFrame(
    [
        ("id_1", "chong", {"english": 50, "chinese": 10}, 15),
        ("id_1", "chong", {"english": 50, "chinese": 10}, 15),
        ("id_2", "tan", {"english": 20, "chinese": 20, "malay": 40}, 17),
    ],
    schema=schema,
)

df.select("Id", "Name", to_json(col("Scores")).alias("Scores"), "Age").distinct().show()
