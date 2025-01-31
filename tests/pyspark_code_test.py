import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def create_spark_session():
  spark = SparkSession.builder.appName("Pyspark unit test").getOrCreate()
  return spark

def create_data_frames(spark):
  df1 = spark.createDataFrame(data=[("1", 1000), ("2", 3000)], schema=["id", "amount"])
  df2 = spark.createDataFrame(data=[("1", 1000), ("2", 3000)], schema=["id", "amount"])
  return df1, df2

def compare_dataframes_publish(spark, df1, df2):
  assert sorted(df1.collect()) == sorted(df2.collect())


def main():
  spark = create_spark_session()
  df1, df2 = create_data_frames(spark)
  compare_dataframes_publish(spark, df1, df2)
  df1.write.csv("result/expected/df1.csv", header=True)
  df2.write.csv("result/output/df2.csv", header=True)
  
 
