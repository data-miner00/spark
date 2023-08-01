from datetime import datetime
from pyspark.sql import SparkSession

from pyspark.sql.types import (
    StructField,
    StringType,
    StructType,
    TimestampType,
    IntegerType,
)

spark = SparkSession.builder.appName("sandbox").getOrCreate()

left_schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("username", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("date_of_birth", TimestampType(), True),
        StructField("country", StringType(), True),
        StructField("user_type", StringType(), True),
        StructField("credits", IntegerType(), True),
        StructField("free", IntegerType(), True),
    ]
)

right_schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("username", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
    ]
)

# Left, right equal items
print("Equal items")
left_df = spark.createDataFrame(
    [
        (
            "id_1",
            "shein",
            "Shein",
            "Ann",
            datetime(1999, 4, 3),
            "Bulgaria",
            "normal",
            1000,
            1000,
        ),
        (
            "id_2",
            "ceddy",
            "Ceddy",
            "Kann",
            datetime(1999, 5, 4),
            "Croatia",
            "admin",
            1111,
            3000,
        ),
        (
            "id_3",
            "manda",
            "Manda",
            "Fann",
            datetime(1999, 6, 5),
            "Ireland",
            "contract",
            2222,
            7777,
        ),
        (
            "id_4",
            "lucas",
            "Lucas",
            "Lann",
            datetime(1999, 7, 6),
            "Australia",
            "payg",
            1333,
            8888,
        ),
    ],
    schema=left_schema,
)

right_df = spark.createDataFrame(
    [
        ("id_1", "shein", "Shein", "Ann"),
        ("id_2", "ceddy", "Ceddy", "Kann"),
        ("id_3", "manda", "Manda", "Fann"),
        ("id_4", "lucas", "Lucas", "Lann"),
    ],
    schema=right_schema,
)

merged = left_df.join(
    right_df, ["id", "username", "first_name", "last_name"], "right"
).select(
    right_df.id,
    right_df.username,
    right_df.first_name,
    right_df.last_name,
    left_df.credits,
    left_df.free,
)

merged.show()

# Right less item
print("Right missing item")
left_df2 = spark.createDataFrame(
    [
        (
            "id_1",
            "shein",
            "Shein",
            "Ann",
            datetime(1999, 4, 3),
            "Bulgaria",
            "normal",
            1000,
            1000,
        ),
        (
            "id_2",
            "ceddy",
            "Ceddy",
            "Kann",
            datetime(1999, 5, 4),
            "Croatia",
            "admin",
            1111,
            3000,
        ),
        (
            "id_3",
            "manda",
            "Manda",
            "Fann",
            datetime(1999, 6, 5),
            "Ireland",
            "contract",
            2222,
            7777,
        ),
        (
            "id_4",
            "lucas",
            "Lucas",
            "Lann",
            datetime(1999, 7, 6),
            "Australia",
            "payg",
            1333,
            8888,
        ),
    ],
    schema=left_schema,
)

right_df2 = spark.createDataFrame(
    [
        ("id_1", "shein", "Shein", "Ann"),
        ("id_2", "ceddy", "Ceddy", "Kann"),
        ("id_3", "manda", "Manda", "Fann"),
    ],
    schema=right_schema,
)

merged2 = left_df2.join(
    right_df2, ["id", "username", "first_name", "last_name"], "right"
).select(
    right_df2.id,
    right_df2.username,
    right_df2.first_name,
    right_df2.last_name,
    left_df2.credits,
    left_df2.free,
)

merged2.show()

# Left less items
print("Left missing item")
left_df3 = spark.createDataFrame(
    [
        (
            "id_1",
            "shein",
            "Shein",
            "Ann",
            datetime(1999, 4, 3),
            "Bulgaria",
            "normal",
            1000,
            1000,
        ),
        (
            "id_2",
            "ceddy",
            "Ceddy",
            "Kann",
            datetime(1999, 5, 4),
            "Croatia",
            "admin",
            1111,
            3000,
        ),
        (
            "id_3",
            "manda",
            "Manda",
            "Fann",
            datetime(1999, 6, 5),
            "Ireland",
            "contract",
            2222,
            7777,
        ),
    ],
    schema=left_schema,
)

right_df3 = spark.createDataFrame(
    [
        ("id_1", "shein", "Shein", "Ann"),
        ("id_2", "ceddy", "Ceddy", "Kann"),
        ("id_3", "manda", "Manda", "Fann"),
        ("id_4", "lucas", "Lucas", "Lann"),
    ],
    schema=right_schema,
)

merged3 = left_df3.join(
    right_df3, ["id", "username", "first_name", "last_name"], "right"
).select(
    right_df3.id,
    right_df3.username,
    right_df3.first_name,
    right_df3.last_name,
    left_df3.credits,
    left_df3.free,
)

merged3.show()
