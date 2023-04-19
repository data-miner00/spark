import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark(request: pytest.FixtureRequest) -> SparkSession:
    spark = SparkSession.builder.master("local").getOrCreate()
    request.addfinalizer(lambda: spark.sparkContext.stop())
    return spark
