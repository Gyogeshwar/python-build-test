import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.testing.utils import assertDataFrameEqual

spark = SparkSession.builder.appName("Pyspark unit test").getOrCreate()

df1 = spark.createDataFrame(data=[("1", 1000), ("2", 3000)], schema=["id", "amount"])
df2 = spark.createDataFrame(data=[("1", 1000), ("2", 3000), ("3", 4000)], schema=["id", "amount"])
assertDataFrameEqual(df1, df2) 
