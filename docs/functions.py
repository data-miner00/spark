from pyspark.sql.functions import expr, desc
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("sandbox").getOrCreate()

df: DataFrame = spark.read.csv('random_data.csv', inferSchema=True, header=True)

# expr
exp = 'age + 0.2 * AgeFixed'
df.withColumn('new_col', expr(exp))

# desc
df.orderBy(desc("age"))

# Use SQL Syntax
df.createOrReplaceTempView("df")
spark.sql("""SELECT sex from df""").show(2)

# SQL select Expr
df.selectExpr("age >= 40 as older", "age").show(2)

# Pivot
df.groupBy('age').pivot('sex').count()
df.groupBy('age').pivot('sex', ("M", "F"))

df.selectExpr("age >= 40 as older", "age", "sex").groupBy("sex")\
    .pivot("older", ("true", "false")).count().show()
