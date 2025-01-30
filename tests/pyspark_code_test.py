import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Pyspark unit test").getOrCreate()

df1 = spark.createDataFrame(data=[("1", 1000), ("2", 3000)], schema=["id", "amount"])
df2 = spark.createDataFrame(data=[("1", 1000), ("2", 3000)], schema=["id", "amount"])
assert sorted(df1.collect()) == sorted(df2.collect())

df1.write.csv("result/expected/df1.csv", header=True)
df2.write.csv("result/output/df2.csv", header=True)
