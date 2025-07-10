"""
Tests for vectorized signal scanner.
"""

import pytest
import asyncio
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from unittest.mock import patch, AsyncMock, MagicMock

from signal_scanner import SignalScanner, scan_signals


class TestSignalScanner:
    """Test the SignalScanner class."""
    
    @pytest.fixture
    def scanner(self):
        """Create a SignalScanner instance for testing."""
        return SignalScanner(use_numba=False)
    
    @pytest.fixture
    def sample_data(self):
        """Create sample data for testing."""
        dates = pd.date_range('2023-01-01', periods=100, freq='D')
        
        # Create correlated series
        np.random.seed(42)
        base = np.cumsum(np.random.randn(100))
        series1 = pd.Series(base, index=dates, name='Series 1')
        series2 = pd.Series(base[5:] + np.random.randn(95) * 0.1, 
                           index=dates[:-5], name='Series 2')
        
        return pd.DataFrame({
            'Series 1': series1,
            'Series 2': series2
        })
    
    def test_init(self, scanner):
        """Test scanner initialization."""
        assert scanner.use_numba is False
        assert scanner.max_lag is not None
        assert scanner.top_n is not None
        assert scanner.lookback_days is not None
    
    def test_calculate_correlations(self, scanner, sample_data):
        """Test correlation calculation."""
        correlations = scanner._calculate_correlations(sample_data, max_lag=5)
        
        assert not correlations.empty
        assert 'lead_series' in correlations.columns
        assert 'lag_series' in correlations.columns
        assert 'correlation' in correlations.columns
        assert 'lag' in correlations.columns
        assert 'abs_correlation' in correlations.columns
        
        # Should have correlations for each pair
        expected_pairs = len(sample_data.columns) * (len(sample_data.columns) - 1)
        assert len(correlations) == expected_pairs
    
    def test_correlate_numpy(self, scanner):
        """Test numpy correlation calculation."""
        # Create test data with known correlation
        x = np.array([1, 2, 3, 4, 5])
        y = np.array([2, 4, 6, 8, 10])  # Perfect positive correlation
        
        correlations = scanner._correlate_numpy(x, y, max_lag=2)
        
        assert len(correlations) == 5  # 2*max_lag + 1
        assert correlations[2] == pytest.approx(1.0, abs=1e-10)  # No lag correlation
    
    def test_get_top_correlations(self, scanner):
        """Test getting top correlations."""
        # Create sample correlations
        correlations = pd.DataFrame({
            'lead_series': ['A', 'B', 'C'],
            'lag_series': ['B', 'C', 'A'],
            'correlation': [0.8, 0.6, 0.9],
            'lag': [1, -1, 0],
            'abs_correlation': [0.8, 0.6, 0.9]
        })
        
        top_corr = scanner._get_top_correlations(correlations, top_n=2)
        
        assert len(top_corr) == 2
        assert top_corr.iloc[0]['abs_correlation'] >= top_corr.iloc[1]['abs_correlation']
        assert 'z_score' in top_corr.columns
    
    def test_calculate_z_scores(self, scanner):
        """Test z-score calculation."""
        all_correlations = pd.Series([0.1, 0.2, 0.3, 0.4, 0.5])
        selected_correlations = pd.Series([0.4, 0.5])
        
        z_scores = scanner._calculate_z_scores(all_correlations, selected_correlations)
        
        assert len(z_scores) == 2
        assert isinstance(z_scores, pd.Series)
    
    def test_build_composite_signal(self, scanner, sample_data):
        """Test composite signal building."""
        # Create sample top correlations
        top_correlations = pd.DataFrame({
            'lead_series': ['Series 1'],
            'lag_series': ['Series 2'],
            'correlation': [0.8],
            'lag': [1],
            'abs_correlation': [0.8],
            'z_score': [2.0]
        })
        
        composite = scanner._build_composite_signal(sample_data, top_correlations)
        
        assert not composite.empty
        assert isinstance(composite, pd.Series)
        assert len(composite) > 0
    
    @pytest.mark.asyncio
    async def test_scan_signals(self, scanner, sample_data):
        """Test full signal scanning."""
        # Mock the data fetcher
        with patch.object(scanner.data_fetcher, 'fetch_all_series') as mock_fetch:
            mock_fetch.return_value = sample_data
            
            start = datetime(2023, 1, 1)
            end = datetime(2023, 4, 10)
            
            results = await scanner.scan_signals(start, end)
            
            assert 'error' not in results
            assert 'start_date' in results
            assert 'end_date' in results
            assert 'series_count' in results
            assert 'data_points' in results
            assert 'max_lag' in results
            assert 'top_correlations' in results
            assert 'composite_signal' in results
            assert 'all_correlations' in results
    
    @pytest.mark.asyncio
    async def test_scan_signals_no_data(self, scanner):
        """Test scanning with no data."""
        # Mock empty data
        with patch.object(scanner.data_fetcher, 'fetch_all_series') as mock_fetch:
            mock_fetch.return_value = pd.DataFrame()
            
            start = datetime(2023, 1, 1)
            end = datetime(2023, 4, 10)
            
            results = await scanner.scan_signals(start, end)
            
            assert 'error' in results
            assert results['error'] == 'No data available'
    
    def test_save_results(self, scanner, tmp_path):
        """Test saving results to files."""
        # Create sample results
        results = {
            'top_correlations': pd.DataFrame({
                'lead_series': ['A'],
                'lag_series': ['B'],
                'correlation': [0.8],
                'lag': [1],
                'abs_correlation': [0.8]
            }),
            'composite_signal': pd.Series([1.0, 2.0, 3.0], 
                                        index=pd.date_range('2023-01-01', periods=3))
        }
        
        scanner.save_results(results, tmp_path)
        
        # Check that files were created
        assert (tmp_path / 'results.csv').exists()
        assert (tmp_path / 'composite_signal.csv').exists()


class TestScanSignals:
    """Test the convenience scan_signals function."""
    
    @pytest.mark.asyncio
    async def test_scan_signals_convenience(self):
        """Test the scan_signals convenience function."""
        with patch('signal_scanner.SignalScanner') as mock_scanner_class:
            mock_scanner = AsyncMock()
            mock_scanner.scan_signals.return_value = {
                'start_date': datetime(2023, 1, 1),
                'end_date': datetime(2023, 4, 10),
                'series_count': 2,
                'data_points': 100,
                'max_lag': 5,
                'top_correlations': pd.DataFrame(),
                'composite_signal': pd.Series(),
                'all_correlations': pd.DataFrame()
            }
            mock_scanner_class.return_value = mock_scanner
            
            start = datetime(2023, 1, 1)
            end = datetime(2023, 4, 10)
            
            result = await scan_signals(start, end)
            
            assert 'start_date' in result
            mock_scanner.scan_signals.assert_called_once_with(
                start, end, None, None, None
            ) 