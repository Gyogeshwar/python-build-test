import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for testing."""
    spark_session = SparkSession.builder.appName("Pyspark unit test").getOrCreate()
    yield spark_session
    spark_session.stop()

@pytest.fixture
def data_frames(spark):
    """Create sample DataFrames for testing."""
    df1 = spark.createDataFrame(data=[("1", 1000), ("2", 3000)], schema=["id", "amount"])
    df2 = spark.createDataFrame(data=[("1", 1000), ("2", 3000)], schema=["id", "amount"])
    return df1, df2

def compare_dataframes_publish(df1, df2):
    """Compare two DataFrames."""
    assert sorted(df1.collect()) == sorted(df2.collect())

def test_compare_dataframes(spark, data_frames):
    """Test case for comparing DataFrames."""
    df1, df2 = data_frames
    compare_dataframes_publish(df1, df2)

def main():
    spark = create_spark_session()
    df1, df2 = create_data_frames(spark)
    
    # Compare DataFrames
    compare_dataframes_publish(df1, df2)
    
    # Ensure output directories exist
    os.makedirs("/home/runner/work/python-build-test/result/expected", exist_ok=True)
    os.makedirs("/home/runner/work/python-build-test/result/output", exist_ok=True)
    
    # Write DataFrames to CSV
    df1.write.csv("/home/runner/work/python-build-test/result/expected/df1.csv", header=True)
    df2.write.csv("/home/runner/work/python-build-test/result/output/df2.csv", header=True)

if __name__ == "__main__":
    main()
