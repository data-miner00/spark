"""
Experiment: Compatibility issues when merging dataframes with different data types
"""

from pyspark.sql.functions import factorial
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructField,
    StringType,
    StructType,
    MapType,
    ArrayType,
    IntegerType,
    BooleanType,
    FloatType,
)
import pytest
import pandas as pd

schema_with_stringtype = StructType(
    [
        StructField("id", StringType(), True),
        StructField("col1", StringType(), True),
    ]
)

schema_with_inttype = StructType(
    [
        StructField("id", StringType(), True),
        StructField("col1", IntegerType(), True),
    ]
)

schema_with_booltype = StructType(
    [
        StructField("id", StringType(), True),
        StructField("col1", BooleanType(), True),
    ]
)

schema_with_maptype = StructType(
    [
        StructField("id", StringType(), True),
        StructField("col1", MapType(StringType(), StringType()), True),
    ]
)

schema_with_arraytype = StructType(
    [
        StructField("id", StringType(), True),
        StructField("col1", ArrayType(StringType()), True),
    ]
)

schema_with_floattype = StructType(
    [
        StructField("id", StringType(), True),
        StructField("col1", FloatType(), True),
    ]
)


def test_mergeWithNormalDataframes_returnsExpectedDataframe(spark: SparkSession):
    data_1 = [("id_1", "string")]

    data_2 = [("id_2", "string")]

    expected_data = [("id_1", "string"), ("id_2", "string")]

    df_1 = spark.createDataFrame(data_1, schema=schema_with_stringtype)
    df_2 = spark.createDataFrame(data_2, schema=schema_with_stringtype)
    df_expected = spark.createDataFrame(expected_data, schema=schema_with_stringtype)

    df_actual = df_1.unionByName(df_2)

    __assert_frame_equals(df_expected, df_actual)


def test_mergeStringWithInt_returnStringColumn(spark: SparkSession):
    data_1 = [("id_1", "string")]

    data_2 = [("id_2", 10_000)]

    expected_data = [("id_1", "string"), ("id_2", "10000")]

    df_1 = spark.createDataFrame(data_1, schema=schema_with_stringtype)
    df_2 = spark.createDataFrame(data_2, schema=schema_with_inttype)
    df_expected = spark.createDataFrame(expected_data, schema=schema_with_stringtype)

    df_actual = df_1.unionByName(df_2)

    __assert_frame_equals(df_expected, df_actual)


def test_mergeStringWithFloat_returnStringColumn(spark: SparkSession):
    data_1 = [("id_1", "string")]

    data_2 = [("id_2", 3.14159)]

    expected_data = [("id_1", "string"), ("id_2", "3.14159")]

    df_1 = spark.createDataFrame(data_1, schema=schema_with_stringtype)
    df_2 = spark.createDataFrame(data_2, schema=schema_with_floattype)
    df_expected = spark.createDataFrame(expected_data, schema=schema_with_stringtype)

    df_actual = df_1.unionByName(df_2)

    __assert_frame_equals(df_expected, df_actual)


@pytest.mark.parametrize(
    "target_data, target_schema",
    [
        (("id_2", ["string"]), schema_with_arraytype),
        (("id_2", False), schema_with_booltype),
        (("id_2", {"key": "value"}), schema_with_maptype),
    ],
)
def test_incompatibleMerging_throwsException(
    target_data, target_schema, spark: SparkSession
):
    data_1 = [("id_1", "string")]

    df_1 = spark.createDataFrame(data_1, schema=schema_with_stringtype)

    with pytest.raises(TypeError):
        df_2 = spark.createDataFrame(target_data, schema=target_schema)
        df_1.unionByName(df_2)


def __assert_frame_equals(df1: DataFrame, df2: DataFrame):
    return pd.testing.assert_frame_equal(
        left=df1.toPandas(), right=df2.toPandas(), check_exact=True
    )
