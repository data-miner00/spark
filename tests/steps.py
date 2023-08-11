from pyspark.sql import DataFrame
import pandas as pd


def assert_frame_equal(left: DataFrame, right: DataFrame):
    left = left.sort(left.schema.fieldNames())
    right = right.sort(right.schema.fieldNames())

    pd.testing.assert_frame_equal(
        left=left.toPandas(), right=right.toPandas(), check_exact=True
    )
