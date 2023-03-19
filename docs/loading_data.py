from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("sandbox").getOrCreate()

schema = 'Age INTEGER, Sex STRING, Name STRING'

df = spark.read.csv('random_data.csv', schema=schema, header=True)
df = spark.read.csv('random_data.csv', inferSchema=True, header=True)
df = spark.read.csv('random_data.csv', nullValue='NA')

df.write.format("csv").save('random_data.csv')
df.write.format("csv").mode("overwrite").save('random_data.csv')
