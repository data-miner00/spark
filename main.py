from pyspark.sql import SparkSession

# create a SparkSession
spark = SparkSession.builder.appName("sandbox").getOrCreate()

# create a DataFrame with null values
df = spark.createDataFrame(
    [(1, None, "a"), (2, "b", None), (3, "c", "d"), (4, None, None)],
    ["id", "col1", "col2"],
)

# drop rows that have at least 2 null values
df_thresh = df.na.drop(thresh=2)

# show the results
df_thresh.show()

# jvm get version
print(spark._sc._jvm.org.apache.hadoop.util.VersionInfo.getVersion())
