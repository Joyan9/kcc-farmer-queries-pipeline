# tests/test_integration.py
import pytest
import tempfile
import shutil
import os
from unittest.mock import patch, MagicMock
import duckdb


class TestIntegrationScenarios:
    """Integration tests for end-to-end scenarios."""
    
    def setup_method(self):
        """Setup temporary directories for each test."""
        self.test_dir = tempfile.mkdtemp()
        self.raw_data_dir = os.path.join(self.test_dir, "raw_data")
        self.processed_data_dir = os.path.join(self.test_dir, "processed_data")
        os.makedirs(self.raw_data_dir)
        os.makedirs(self.processed_data_dir)
    
    def teardown_method(self):
        """Clean up temporary directories."""
        shutil.rmtree(self.test_dir)
    
    def test_duckdb_table_creation(self):
        """Test DuckDB table creation and operations."""
        from helpers.io import ensure_duckdb_tables
        
        db_path = os.path.join(self.test_dir, "test.duckdb")
        conn = duckdb.connect(db_path)
        
        # Test table creation
        ensure_duckdb_tables(conn)
        
        # Verify tables were created
        tables = conn.execute("SHOW TABLES").fetchall()
        table_names = [table[0] for table in tables]
        
        expected_tables = ["dim_category", "dim_sector", "dim_demography", "fct_queries"]
        for table in expected_tables:
            assert table in table_names
        
        conn.close()
    
    @patch.dict(os.environ, {'KCC_API_KEY': 'test_key'})
    def test_pipeline_configuration(self):
        """Test pipeline configuration without actual API calls."""
        from ingestion.main import get_kcc_source
        
        with patch('ingestion.main.rest_api_source') as mock_source:
            mock_source.return_value = MagicMock()
            
            # Test source creation
            source = get_kcc_source(2024, 5, max_offset=100)
            
            # Verify configuration
            assert mock_source.called
            config = mock_source.call_args[0][0]
            
            # Verify authentication is configured
            assert 'client' in config
            assert config['client']['auth'] is not None
            
            # Verify pagination is configured
            assert config['client']['paginator'] is not None
