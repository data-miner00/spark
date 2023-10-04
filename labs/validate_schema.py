"""
Experiment: To discover the proper way of doing schema validation in PySpark.
"""
from pyspark.sql.avro.functions import to_avro
from pyspark.sql.types import StructField, StructType, StringType, ArrayType
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import array, col
import pandera.pyspark as pa
from pandera.pyspark import DataFrameModel
import json


class AfterSchema(DataFrameModel):
    name: StringType() = pa.Field()
    itemId: StringType() = pa.Field(str_contains="Id")


schema_ori = StructType(
    [
        StructField("name", StringType(), True),
        StructField("itemId", StringType(), True),
    ]
)

schema = StructType(
    [
        StructField("name", StringType(), True),
        StructField("items", ArrayType(StringType(), True), False),
    ]
)


def select_statement(df: DataFrame) -> DataFrame:
    return df.select("name", array(col("itemId")).alias("items"))


def print_errors(df_out_errors):
    print(json.dumps(dict(df_out_errors), indent=4))


if __name__ == "__main__":
    spark = SparkSession.builder.appName("sandbox").getOrCreate()

    df = spark.createDataFrame([("item", "itemId1")], schema=schema_ori)
    empRDD = spark.sparkContext.emptyRDD()
    df_emp = spark.createDataFrame(empRDD, schema=schema_ori)

    df = select_statement(df)
    df.show()
    df.printSchema()
    validated = AfterSchema.validate(check_obj=df)
    validated.show()
    print_errors(validated.pandera.errors)

    df_emp = select_statement(df_emp)
    df_emp.show()
    df_emp.printSchema()
    validated = AfterSchema.validate(check_obj=df_emp)
    validated.show()
    print_errors(validated.pandera.errors)
