import pytest
from pyspark.sql import SparkSession
from labs.calculate_pi import generate_pi


@pytest.mark.slow
def test_calculate_pi(spark: SparkSession):
    sc = spark.sparkContext
    pi = generate_pi(sc)

    assert pi == pytest.approx(3.1415926535, 0.0001)
