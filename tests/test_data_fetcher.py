"""
Tests for async data fetcher.
"""

import pytest
import asyncio
from datetime import datetime, timedelta
from unittest.mock import patch, AsyncMock, MagicMock
import pandas as pd

from data_fetcher import DataFetcher, fetch_data


class TestDataFetcher:
    """Test the DataFetcher class."""
    
    @pytest.fixture
    def fetcher(self):
        """Create a DataFetcher instance for testing."""
        return DataFetcher()
    
    @pytest.fixture
    def mock_data_sources(self):
        """Mock data sources configuration."""
        return {
            'series': [
                {
                    'name': 'Test Series 1',
                    'source': 'fred',
                    'id': 'TEST1',
                    'freq': 'D'
                },
                {
                    'name': 'Test Series 2',
                    'source': 'yahoo',
                    'ticker': 'TEST2',
                    'freq': 'D'
                }
            ],
            'defaults': {
                'lookback_days': 365,
                'max_lag': 10,
                'top_n': 5
            }
        }
    
    def test_load_data_sources(self, fetcher, mock_data_sources):
        """Test loading data sources from YAML."""
        with patch('builtins.open', create=True) as mock_open:
            mock_open.return_value.__enter__.return_value.read.return_value = """
            series:
              - name: Test Series 1
                source: fred
                id: TEST1
                freq: D
            defaults:
              lookback_days: 365
            """
            
            # Mock yaml.safe_load
            with patch('yaml.safe_load') as mock_yaml:
                mock_yaml.return_value = mock_data_sources
                fetcher._load_data_sources()
                
                assert fetcher.data_sources == mock_data_sources
    
    @pytest.mark.asyncio
    async def test_fetch_all_series(self, fetcher, mock_data_sources):
        """Test fetching all series."""
        fetcher.data_sources = mock_data_sources
        
        # Mock the fetcher registry
        mock_fred_fetcher = AsyncMock()
        mock_fred_fetcher.fetch.return_value = pd.Series(
            [1.0, 2.0, 3.0],
            index=pd.date_range('2023-01-01', periods=3, freq='D')
        )
        
        mock_yahoo_fetcher = AsyncMock()
        mock_yahoo_fetcher.fetch.return_value = pd.Series(
            [4.0, 5.0, 6.0],
            index=pd.date_range('2023-01-01', periods=3, freq='D')
        )
        
        with patch('fetchers.fetcher_registry', {
            'fred': mock_fred_fetcher,
            'yahoo': mock_yahoo_fetcher
        }):
            start = datetime(2023, 1, 1)
            end = datetime(2023, 1, 3)
            
            result = await fetcher.fetch_all_series(start, end)
            
            assert not result.empty
            assert len(result.columns) == 2
            assert 'Test Series 1' in result.columns
            assert 'Test Series 2' in result.columns
            assert len(result) == 3
    
    @pytest.mark.asyncio
    async def test_fetch_single_series(self, fetcher):
        """Test fetching a single series."""
        series_config = {
            'name': 'Test Series',
            'source': 'fred',
            'id': 'TEST',
            'freq': 'D'
        }
        
        mock_fetcher = AsyncMock()
        mock_fetcher.fetch.return_value = pd.Series(
            [1.0, 2.0, 3.0],
            index=pd.date_range('2023-01-01', periods=3, freq='D')
        )
        
        with patch('fetchers.fetcher_registry', {'fred': mock_fetcher}):
            start = datetime(2023, 1, 1)
            end = datetime(2023, 1, 3)
            
            result = await fetcher._fetch_single_series(start, end, series_config)
            
            assert not result.empty
            assert len(result) == 3
            mock_fetcher.fetch.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_fetch_single_series_error(self, fetcher):
        """Test error handling in single series fetch."""
        series_config = {
            'name': 'Test Series',
            'source': 'unknown',
            'id': 'TEST',
            'freq': 'D'
        }
        
        with patch('fetchers.fetcher_registry', {}):
            start = datetime(2023, 1, 1)
            end = datetime(2023, 1, 3)
            
            result = await fetcher._fetch_single_series(start, end, series_config)
            
            assert result.empty
    
    def test_align_dataframe(self, fetcher):
        """Test DataFrame alignment."""
        # Create test data with different lengths
        series1 = pd.Series(
            [1.0, 2.0, 3.0, 4.0],
            index=pd.date_range('2023-01-01', periods=4, freq='D')
        )
        series2 = pd.Series(
            [5.0, 6.0, 7.0],
            index=pd.date_range('2023-01-02', periods=3, freq='D')
        )
        
        df = pd.DataFrame({
            'Series 1': series1,
            'Series 2': series2
        })
        
        aligned = fetcher._align_dataframe(df)
        
        # Should have same length for all series
        assert len(aligned) > 0
        assert not aligned.isna().all().any()
    
    def test_get_series_info(self, fetcher, mock_data_sources):
        """Test getting series information."""
        fetcher.data_sources = mock_data_sources
        
        info = fetcher.get_series_info()
        
        assert len(info) == 2
        assert info[0]['name'] == 'Test Series 1'
        assert info[1]['name'] == 'Test Series 2'
    
    def test_get_defaults(self, fetcher, mock_data_sources):
        """Test getting default parameters."""
        fetcher.data_sources = mock_data_sources
        
        defaults = fetcher.get_defaults()
        
        assert defaults['lookback_days'] == 365
        assert defaults['max_lag'] == 10
        assert defaults['top_n'] == 5


class TestFetchData:
    """Test the convenience fetch_data function."""
    
    @pytest.mark.asyncio
    async def test_fetch_data(self):
        """Test the fetch_data convenience function."""
        with patch('data_fetcher.DataFetcher') as mock_fetcher_class:
            mock_fetcher = AsyncMock()
            mock_fetcher.fetch_all_series.return_value = pd.DataFrame({
                'Test': [1.0, 2.0, 3.0]
            })
            mock_fetcher_class.return_value = mock_fetcher
            
            start = datetime(2023, 1, 1)
            end = datetime(2023, 1, 3)
            
            result = await fetch_data(start, end)
            
            assert not result.empty
            mock_fetcher.fetch_all_series.assert_called_once_with(start, end, None) 