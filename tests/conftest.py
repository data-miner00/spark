import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark(request: pytest.FixtureRequest) -> SparkSession:
    spark = (
        SparkSession.builder.master("local[1]")
        .config(
            "spark.driver.extraJavaOptions",
            " ".join(["-XX:+CMSClassUnloadingEnabled", "-XX:+UseCompressedOops"]),
        )
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.dagGraph.retainedRootRDDs", "1")
        .config("spark.ui.retainedStages", "1")
        .config("spark.ui.retainedTasks", "1")
        .config("spark.sql.ui.retainedExecutions", "1")
        .config("spark.worker.ui.retainedExecutors", "1")
        .config("spark.worker.ui.retainedDrivers", "1")
        .config("spark.driver.memory", "2g")
        .config("spark.rdd.compress", "false")
        .config("spark.shuffle.compress", "false")
        .config("spark.default.parallelism", "1")
        .getOrCreate()
    )

    request.addfinalizer(lambda: spark.sparkContext.stop())
    return spark
