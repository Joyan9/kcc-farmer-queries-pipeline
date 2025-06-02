# tests/test_ingestion.py
import pytest
import unittest.mock as mock
from datetime import datetime
import os
import tempfile
import shutil
from unittest.mock import patch, MagicMock

# Import functions from ingestion module
import sys
sys.path.append('../ingestion')
import ingestion.main as ingestion_main
from ingestion.main import (
    get_last_month, get_month_year_range, get_kcc_source,
    parse_args, EARLIEST_YEAR, EARLIEST_MONTH
)



class TestIngestionHelpers:
    """Test helper functions in ingestion module."""
    
    def test_get_last_month_january(self):
        """Test get_last_month when current month is January."""
        with patch('ingestion.main.datetime') as mock_datetime:
            mock_datetime.now.return_value = datetime(2024, 1, 15)
            year, month = get_last_month()
            assert year == 2023
            assert month == 12
    
    def test_get_last_month_other_months(self):
        """Test get_last_month for non-January months."""
        with patch('ingestion.main.datetime') as mock_datetime:
            mock_datetime.now.return_value = datetime(2024, 6, 15)
            year, month = get_last_month()
            assert year == 2024
            assert month == 5
    
    def test_get_month_year_range_same_year(self):
        """Test month range generation within same year."""
        result = get_month_year_range(2024, 3, 2024, 6)
        expected = [(2024, 3), (2024, 4), (2024, 5), (2024, 6)]
        assert result == expected
    
    def test_get_month_year_range_across_years(self):
        """Test month range generation across multiple years."""
        result = get_month_year_range(2023, 11, 2024, 2)
        expected = [(2023, 11), (2023, 12), (2024, 1), (2024, 2)]
        assert result == expected
    
    def test_get_month_year_range_single_month(self):
        """Test month range generation for single month."""
        result = get_month_year_range(2024, 5, 2024, 5)
        expected = [(2024, 5)]
        assert result == expected


class TestKCCSource:
    """Test KCC source configuration."""
    
    @patch.dict(os.environ, {'KCC_API_KEY': 'test_key'})
    def test_get_kcc_source_configuration(self):
        """Test KCC source creates proper configuration."""
        # Mock the rest_api_source to avoid actual API calls
        with patch('ingestion.main.rest_api_source') as mock_source:
            mock_source.return_value = MagicMock()
            
            source = get_kcc_source(2024, 5, max_offset=1000)
            
            # Verify rest_api_source was called
            mock_source.assert_called_once()
            
            # Check the configuration structure
            config = mock_source.call_args[0][0]
            assert 'client' in config
            assert 'resources' in config
            assert config['client']['base_url'] == 'https://api.data.gov.in/'
            
            # Check resource configuration
            resource = config['resources'][0]
            assert resource['name'] == 'kcc_data_2024_05'
            assert resource['endpoint']['params']['filters[year]'] == '2024'
            assert resource['endpoint']['params']['filters[month]'] == '5'


class TestArgumentParsing:
    """Test command line argument parsing."""
    
    def test_parse_args_default(self):
        """Test default argument parsing."""
        with patch('sys.argv', ['main.py']):
            args = parse_args()
            assert args.backfill is False
            assert args.max_offset == 50000
    
    def test_parse_args_backfill(self):
        """Test backfill argument parsing."""
        with patch('sys.argv', ['main.py', '--backfill', '--max-offset', '1000']):
            args = parse_args()
            assert args.backfill is True
            assert args.max_offset == 1000
