# tests/test_config.py
import pytest
import sys
sys.path.append('../processing')
from config import INVALID_VALUES, REGEX_INVALID_COLS, PII_PATTERNS, RAW_SCHEMA


class TestConfiguration:
    """Test configuration values and schema."""
    
    def test_invalid_values_structure(self):
        """Test invalid values configuration structure."""
        assert isinstance(INVALID_VALUES, dict)
        assert "state_name" in INVALID_VALUES
        assert "category" in INVALID_VALUES
        assert isinstance(INVALID_VALUES["state_name"], list)
    
    def test_regex_invalid_cols(self):
        """Test regex columns configuration."""
        assert isinstance(REGEX_INVALID_COLS, list)
        assert "sector" in REGEX_INVALID_COLS
        assert "crop" in REGEX_INVALID_COLS
    
    def test_pii_patterns_structure(self):
        """Test PII patterns configuration."""
        assert isinstance(PII_PATTERNS, list)
        assert len(PII_PATTERNS) > 0
        
        # Each pattern should be a tuple of (pattern, replacement)
        for pattern_tuple in PII_PATTERNS:
            assert isinstance(pattern_tuple, tuple)
            assert len(pattern_tuple) == 2
            assert isinstance(pattern_tuple[0], str)  # Regex pattern
            assert isinstance(pattern_tuple[1], str)  # Replacement
    
    def test_raw_schema_structure(self):
        """Test raw data schema structure."""
        assert RAW_SCHEMA is not None
        
        # Get field names
        field_names = [field.name for field in RAW_SCHEMA.fields]
        
        # Check required fields
        required_fields = [
            "state_name", "district_name", "block_name", 
            "category", "sector", "crop", "query_text", 
            "kcc_ans", "created_on", "_dlt_id"
        ]
        
        for field in required_fields:
            assert field in field_names
