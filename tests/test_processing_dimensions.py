# tests/test_processing_dimensions.py
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import sys
sys.path.append('../processing')
from helpers.dimensions import generate_dim


class TestDimensionGeneration:
    """Test dimension table generation."""
    
    @classmethod
    def setup_class(cls):
        """Setup Spark session for testing."""
        cls.spark = SparkSession.builder \
            .appName("TestDimensionGeneration") \
            .master("local[1]") \
            .getOrCreate()
    
    @classmethod
    def teardown_class(cls):
        """Tear down Spark session."""
        cls.spark.stop()
    
    def test_generate_dim(self):
        """Test dimension table generation with surrogate keys."""
        schema = StructType([
            StructField("category", StringType(), True),
            StructField("other_col", StringType(), True)
        ])
        
        data = [
            ("Agriculture", "data1"),
            ("Livestock", "data2"),
            ("Agriculture", "data3"),  # Duplicate
            ("Horticulture", "data4")
        ]
        
        df = self.spark.createDataFrame(data, schema)
        dim_table = generate_dim(df, "category", "category_id")
        result = dim_table.collect()
        
        # Should have 3 unique categories
        assert len(result) == 3
        
        # Check if all categories are present
        categories = [row["category"] for row in result]
        assert "Agriculture" in categories
        assert "Livestock" in categories
        assert "Horticulture" in categories
        
        # Check if surrogate keys are assigned
        category_ids = [row["category_id"] for row in result]
        assert len(set(category_ids)) == 3  # All IDs should be unique
        assert min(category_ids) == 1       # Should start from 1
