"""
Tests for data fetchers.
"""

import pytest
import asyncio
import pandas as pd
from datetime import datetime, timedelta
from unittest.mock import patch, AsyncMock, MagicMock

from fetchers import BaseFetcher, fetcher_registry
from fetchers.fred import FredFetcher
from fetchers.yahoo import YahooFetcher
from fetchers.fng import FearGreedFetcher
from fetchers.trends import TrendsFetcher
from fetchers.binance import BinanceFetcher


class TestBaseFetcher:
    """Test the base fetcher class."""
    
    def test_init(self):
        """Test fetcher initialization."""
        fetcher = BaseFetcher(max_retries=5, base_delay=2.0)
        
        assert fetcher.max_retries == 5
        assert fetcher.base_delay == 2.0
    
    def test_validate_date_range_valid(self):
        """Test valid date range validation."""
        fetcher = BaseFetcher()
        start = datetime(2023, 1, 1)
        end = datetime(2023, 1, 2)
        
        # Should not raise an exception
        fetcher._validate_date_range(start, end)
    
    def test_validate_date_range_invalid(self):
        """Test invalid date range validation."""
        fetcher = BaseFetcher()
        start = datetime(2023, 1, 2)
        end = datetime(2023, 1, 1)
        
        with pytest.raises(ValueError, match="Start date must be before end date"):
            fetcher._validate_date_range(start, end)
    
    def test_validate_date_range_future(self):
        """Test future date validation."""
        fetcher = BaseFetcher()
        start = datetime.now() + timedelta(days=1)
        end = datetime.now() + timedelta(days=2)
        
        with pytest.raises(ValueError, match="Start date cannot be in the future"):
            fetcher._validate_date_range(start, end)
    
    def test_align_series(self):
        """Test series alignment."""
        fetcher = BaseFetcher()
        
        # Create test series
        dates = pd.date_range('2023-01-01', periods=5, freq='D')
        series = pd.Series([1, 2, 3, 4, 5], index=dates)
        
        aligned = fetcher._align_series(series, 'D')
        
        assert isinstance(aligned, pd.Series)
        assert len(aligned) == 5
        assert isinstance(aligned.index, pd.DatetimeIndex)


class TestFredFetcher:
    """Test the FRED fetcher."""
    
    @pytest.fixture
    def fetcher(self):
        """Create a FRED fetcher instance."""
        return FredFetcher()
    
    @pytest.mark.asyncio
    async def test_fetch_success(self, fetcher):
        """Test successful FRED data fetch."""
        # Mock API response
        mock_response = {
            'observations': [
                {'date': '2023-01-01', 'value': '100.0'},
                {'date': '2023-01-02', 'value': '101.0'},
                {'date': '2023-01-03', 'value': '102.0'}
            ]
        }
        
        with patch('aiohttp.ClientSession') as mock_session:
            mock_session.return_value.__aenter__.return_value.get.return_value.__aenter__.return_value.json.return_value = mock_response
            mock_session.return_value.__aenter__.return_value.get.return_value.__aenter__.return_value.raise_for_status.return_value = None
            
            start = datetime(2023, 1, 1)
            end = datetime(2023, 1, 3)
            
            result = await fetcher.fetch(start, end, series_id='TEST')
            
            assert not result.empty
            assert len(result) == 3
            assert isinstance(result.index, pd.DatetimeIndex)
    
    @pytest.mark.asyncio
    async def test_fetch_no_data(self, fetcher):
        """Test FRED fetch with no data."""
        mock_response = {'observations': []}
        
        with patch('aiohttp.ClientSession') as mock_session:
            mock_session.return_value.__aenter__.return_value.get.return_value.__aenter__.return_value.json.return_value = mock_response
            mock_session.return_value.__aenter__.return_value.get.return_value.__aenter__.return_value.raise_for_status.return_value = None
            
            start = datetime(2023, 1, 1)
            end = datetime(2023, 1, 3)
            
            result = await fetcher.fetch(start, end, series_id='TEST')
            
            assert result.empty


class TestYahooFetcher:
    """Test the Yahoo Finance fetcher."""
    
    @pytest.fixture
    def fetcher(self):
        """Create a Yahoo fetcher instance."""
        return YahooFetcher()
    
    @pytest.mark.asyncio
    async def test_fetch_success(self, fetcher):
        """Test successful Yahoo data fetch."""
        # Mock API response
        mock_response = {
            'chart': {
                'result': [{
                    'timestamp': [1672531200, 1672617600, 1672704000],  # Unix timestamps
                    'indicators': {
                        'quote': [{
                            'close': [100.0, 101.0, 102.0]
                        }]
                    }
                }]
            }
        }
        
        with patch('aiohttp.ClientSession') as mock_session:
            mock_session.return_value.__aenter__.return_value.get.return_value.__aenter__.return_value.json.return_value = mock_response
            mock_session.return_value.__aenter__.return_value.get.return_value.__aenter__.return_value.raise_for_status.return_value = None
            
            start = datetime(2023, 1, 1)
            end = datetime(2023, 1, 3)
            
            result = await fetcher.fetch(start, end, ticker='TEST')
            
            assert not result.empty
            assert len(result) == 3
            assert isinstance(result.index, pd.DatetimeIndex)


class TestFearGreedFetcher:
    """Test the Fear & Greed fetcher."""
    
    @pytest.fixture
    def fetcher(self):
        """Create a Fear & Greed fetcher instance."""
        return FearGreedFetcher()
    
    @pytest.mark.asyncio
    async def test_fetch_success(self, fetcher):
        """Test successful Fear & Greed data fetch."""
        # Mock API response
        mock_response = {
            'data': [
                {'timestamp': '2023-01-01T00:00:00Z', 'value': '50'},
                {'timestamp': '2023-01-02T00:00:00Z', 'value': '60'},
                {'timestamp': '2023-01-03T00:00:00Z', 'value': '40'}
            ]
        }
        
        with patch('aiohttp.ClientSession') as mock_session:
            mock_session.return_value.__aenter__.return_value.get.return_value.__aenter__.return_value.json.return_value = mock_response
            mock_session.return_value.__aenter__.return_value.get.return_value.__aenter__.return_value.raise_for_status.return_value = None
            
            start = datetime(2023, 1, 1)
            end = datetime(2023, 1, 3)
            
            result = await fetcher.fetch(start, end)
            
            assert not result.empty
            assert len(result) == 3
            assert isinstance(result.index, pd.DatetimeIndex)


class TestTrendsFetcher:
    """Test the Google Trends fetcher."""
    
    @pytest.fixture
    def fetcher(self):
        """Create a Trends fetcher instance."""
        return TrendsFetcher()
    
    @pytest.mark.asyncio
    async def test_fetch_with_cache(self, fetcher, tmp_path):
        """Test trends fetch with caching."""
        # Mock cache directory
        with patch.object(fetcher, 'cache_dir', tmp_path):
            # Mock pytrends
            mock_pytrends = MagicMock()
            mock_interest_df = pd.DataFrame({
                'bitcoin': [50, 60, 70]
            }, index=pd.date_range('2023-01-01', periods=3, freq='D'))
            mock_pytrends.interest_over_time.return_value = mock_interest_df
            
            with patch.object(fetcher, 'pytrends', mock_pytrends):
                start = datetime(2023, 1, 1)
                end = datetime(2023, 1, 3)
                
                result = await fetcher.fetch(start, end, keyword='bitcoin')
                
                assert not result.empty
                assert len(result) == 3
                assert isinstance(result.index, pd.DatetimeIndex)


class TestBinanceFetcher:
    """Test the Binance fetcher."""
    
    @pytest.fixture
    def fetcher(self):
        """Create a Binance fetcher instance."""
        return BinanceFetcher()
    
    @pytest.mark.asyncio
    async def test_fetch_from_database(self, fetcher, tmp_path):
        """Test fetching from local database."""
        # Mock database path
        with patch.object(fetcher, 'db_path', tmp_path / 'test.db'):
            # Create test database
            import sqlite3
            conn = sqlite3.connect(fetcher.db_path)
            conn.execute('''
                CREATE TABLE klines (
                    open_time INTEGER PRIMARY KEY,
                    close_price REAL
                )
            ''')
            conn.execute('INSERT INTO klines VALUES (?, ?)', (1672531200000, 100.0))
            conn.execute('INSERT INTO klines VALUES (?, ?)', (1672617600000, 101.0))
            conn.commit()
            conn.close()
            
            start = datetime(2023, 1, 1)
            end = datetime(2023, 1, 2)
            
            result = await fetcher.fetch(start, end)
            
            assert not result.empty
            assert len(result) == 2


class TestFetcherRegistry:
    """Test the fetcher registry."""
    
    def test_registry_populated(self):
        """Test that the registry is populated with fetchers."""
        assert 'fred' in fetcher_registry
        assert 'yahoo' in fetcher_registry
        assert 'fng' in fetcher_registry
        assert 'trends' in fetcher_registry
        assert 'binance' in fetcher_registry
        
        # Check that all fetchers are instances of BaseFetcher
        for fetcher in fetcher_registry.values():
            assert isinstance(fetcher, BaseFetcher) 