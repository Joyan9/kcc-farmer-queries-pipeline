# tests/test_processing_cleaning.py
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import sys
sys.path.append('../processing')
from helpers.cleaning import clean_categorical_columns, clean_regex_columns, mask_pii


class TestDataCleaning:
    """Test data cleaning functions."""
    
    @classmethod
    def setup_class(cls):
        """Setup Spark session for testing."""
        cls.spark = SparkSession.builder \
            .appName("TestDataCleaning") \
            .master("local[1]") \
            .getOrCreate()
    
    @classmethod
    def teardown_class(cls):
        """Tear down Spark session."""
        cls.spark.stop()
    
    def test_clean_categorical_columns(self):
        """Test categorical column cleaning."""
        # Create test data
        schema = StructType([
            StructField("state_name", StringType(), True),
            StructField("category", StringType(), True)
        ])
        
        data = [
            ("Valid State", "Valid Category"),
            ("NA", "0"),
            (None, None),
            ("Another State", "Another Category")
        ]
        
        df = self.spark.createDataFrame(data, schema)
        
        invalid_values = {
            "state_name": ["NA", "0"],
            "category": ["0"]
        }
        
        cleaned_df = clean_categorical_columns(df, invalid_values)
        result = cleaned_df.collect()
        
        # Check results
        assert result[0]["state_name"] == "Valid State"
        assert result[0]["category"] == "Valid Category"
        assert result[1]["state_name"] == "Not Available"  # "NA" replaced
        assert result[1]["category"] == "Not Available"    # "0" replaced
        assert result[2]["state_name"] == "Not Available"  # None replaced
        assert result[2]["category"] == "Not Available"    # None replaced
    
    def test_clean_regex_columns(self):
        """Test regex-based column cleaning."""
        schema = StructType([
            StructField("sector", StringType(), True),
            StructField("crop", StringType(), True)
        ])
        
        data = [
            ("Agriculture", "Wheat"),
            ("123", "NA"),
            ("0", "456"),
            (None, None)
        ]
        
        df = self.spark.createDataFrame(data, schema)
        regex_cols = ["sector", "crop"]
        
        cleaned_df = clean_regex_columns(df, regex_cols)
        result = cleaned_df.collect()
        
        # Check results
        assert result[0]["sector"] == "Agriculture"
        assert result[0]["crop"] == "Wheat"
        assert result[1]["sector"] == "Not Available"  # "123" is numeric
        assert result[1]["crop"] == "Not Available"    # "NA"
        assert result[2]["sector"] == "Not Available"  # "0"
        assert result[2]["crop"] == "Not Available"    # "456" is numeric
    
    def test_mask_pii(self):
        """Test PII masking functionality."""
        schema = StructType([
            StructField("kcc_ans", StringType(), True)
        ])
        
        data = [
            ("Contact farmer at 9876543210 for details",),
            ("Email: farmer@example.com for more info",),
            ("Account number 123456789012 should be protected",),
            ("No PII in this text",)
        ]
        
        df = self.spark.createDataFrame(data, schema)
        
        patterns = [
            (r"(\+91[\-\s]?\d{10})|(\b\d{10}\b)", "[PHONE]"),
            (r"[a-zA-Z0-9.\-_]+@[a-zA-Z0-9\-_]+\.[a-zA-Z.]+", "[EMAIL]"),
            (r"\b\d{9,18}\b", "[ACCOUNT]")
        ]
        
        masked_df = mask_pii(df, "kcc_ans", patterns)
        result = masked_df.collect()
        
        assert "[PHONE]" in result[0]["kcc_ans"]
        assert "[EMAIL]" in result[1]["kcc_ans"]
        assert "[ACCOUNT]" in result[2]["kcc_ans"]
        assert result[3]["kcc_ans"] == "No PII in this text"
