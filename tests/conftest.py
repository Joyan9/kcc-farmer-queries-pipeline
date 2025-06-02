# tests/conftest.py
import pytest
import os
import sys

# Add source directories to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'ingestion'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'processing'))

@pytest.fixture(scope="session")
def spark_session():
    """Create a Spark session for testing."""
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName("KCC_Tests") \
        .master("local[1]") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .getOrCreate()
    
    yield spark
    
    spark.stop()

