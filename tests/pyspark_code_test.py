import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
import shutil
from pathlib import Path

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
    df1.show()
    df2.show()
    return df1, df2

def compare_dataframes_publish(df1, df2):
    """Compare two DataFrames and write expected/output CSV folders."""
    assert sorted(df1.collect()) == sorted(df2.collect())

    # Prefer CI-provided workspace; fall back to repo root (project folder)
    ws = os.environ.get("GITHUB_WORKSPACE")
    if ws:
        workspace = Path(ws)
    else:
        # file is tests/<file>; parent 1 is project root
        workspace = Path(__file__).resolve().parents[1]

    workspace = workspace.expanduser().resolve()

    result_dir = workspace /"tests" / "result"
    expected_dir = result_dir / "expected"
    output_dir = result_dir / "output"

    # Remove previous result folder if any, then recreate directories
    if result_dir.exists():
        shutil.rmtree(result_dir)
        print("Removed existing result directory")
    result_dir.mkdir(parents=True, exist_ok=True)
    expected_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Spark writes into directories — use directory paths and cast to str
    df1.coalesce(1).write.mode("overwrite").csv(str(expected_dir / "df1"), header=True)
    df2.coalesce(1).write.mode("overwrite").csv(str(output_dir / "df2"), header=True)


def test_compare_dataframes(spark, data_frames):
    """Test case for comparing DataFrames."""
    df1, df2 = data_frames
    compare_dataframes_publish(df1, df2)

def main():
    spark = spark()
    df1, df2 = data_frames(spark)
    
    # Compare DataFrames
    compare_dataframes_publish(df1, df2)

if __name__ == "__main__":
    main()
