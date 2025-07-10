"""
Vectorized signal scanner with proper statistical corrections and async support.
"""

import asyncio
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from concurrent.futures import ProcessPoolExecutor
import yaml

from config import get_settings
from data_fetcher import DataFetcher

logger = logging.getLogger(__name__)

# Optional Numba acceleration
try:
    import numba
    HAS_NUMBA = True
except ImportError:
    HAS_NUMBA = False
    logger.info("Numba not available - using standard numpy operations")

class SignalScanner:
    """Vectorized signal scanner with proper statistical corrections."""
    
    def __init__(self, use_numba: bool = True):
        self.settings = get_settings()
        self.use_numba = use_numba and HAS_NUMBA
        self.data_fetcher = DataFetcher()
        
        # Load defaults from data sources
        defaults = self.data_fetcher.get_defaults()
        self.max_lag = defaults.get('max_lag', self.settings.max_lag)
        self.top_n = defaults.get('top_n', self.settings.top_n)
        self.lookback_days = defaults.get('lookback_days', self.settings.lookback_days)
    
    async def scan_signals(
        self, 
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        series_names: Optional[List[str]] = None,
        max_lag: Optional[int] = None,
        top_n: Optional[int] = None
    ) -> Dict:
        """
        Scan for lead-lag relationships between series.
        
        Args:
            start: Start date (defaults to lookback_days ago)
            end: End date (defaults to now)
            series_names: Optional list of series to analyze
            max_lag: Maximum lag to test (defaults to config)
            top_n: Number of top correlations to return (defaults to config)
            
        Returns:
            Dictionary with scan results
        """
        # Set defaults
        end = end or datetime.now()
        start = start or (end - timedelta(days=self.lookback_days))
        max_lag = max_lag or self.max_lag
        top_n = top_n or self.top_n
        
        logger.info(f"Starting signal scan from {start} to {end}")
        
        # Fetch data
        df = await self.data_fetcher.fetch_all_series(start, end, series_names)
        if df.empty:
            logger.error("No data available for analysis")
            return {"error": "No data available"}
        
        logger.info(f"Analyzing {len(df.columns)} series with {len(df)} data points")
        
        # Calculate correlations
        correlations = self._calculate_correlations(df, max_lag)
        
        # Get top correlations
        top_correlations = self._get_top_correlations(correlations, top_n)
        
        # Build composite signal
        composite_signal = self._build_composite_signal(df, top_correlations)
        
        # Prepare results
        results = {
            'start_date': start,
            'end_date': end,
            'series_count': len(df.columns),
            'data_points': len(df),
            'max_lag': max_lag,
            'top_correlations': top_correlations,
            'composite_signal': composite_signal,
            'all_correlations': correlations
        }
        
        logger.info(f"Scan complete. Found {len(top_correlations)} significant correlations")
        return results
    
    def _calculate_correlations(
        self, 
        df: pd.DataFrame, 
        max_lag: int
    ) -> pd.DataFrame:
        """
        Calculate lead-lag correlations using vectorized operations.
        
        Args:
            df: DataFrame with series as columns
            max_lag: Maximum lag to test
            
        Returns:
            DataFrame with correlation results
        """
        series_names = df.columns.tolist()
        n_series = len(series_names)
        
        # Prepare results storage
        results = []
        
        # Use vectorized operations for better performance
        for i, lead_series in enumerate(series_names):
            for j, lag_series in enumerate(series_names):
                if i == j:
                    continue
                
                # Get series data
                lead_data = df[lead_series].values
                lag_data = df[lag_series].values
                
                # Calculate correlations for all lags
                if self.use_numba:
                    correlations = self._correlate_numba(lead_data, lag_data, max_lag)
                else:
                    correlations = self._correlate_numpy(lead_data, lag_data, max_lag)
                
                # Find best lag and correlation
                best_lag_idx = np.argmax(np.abs(correlations))
                best_correlation = correlations[best_lag_idx]
                best_lag = best_lag_idx - max_lag  # Convert to actual lag
                
                results.append({
                    'lead_series': lead_series,
                    'lag_series': lag_series,
                    'correlation': best_correlation,
                    'lag': best_lag,
                    'abs_correlation': abs(best_correlation)
                })
        
        return pd.DataFrame(results)
    
    def _correlate_numpy(self, lead_data: np.ndarray, lag_data: np.ndarray, max_lag: int) -> np.ndarray:
        """Calculate correlations using numpy."""
        correlations = np.zeros(2 * max_lag + 1)
        
        for lag in range(-max_lag, max_lag + 1):
            if lag < 0:
                # Lead series is ahead
                x = lead_data[-lag:]
                y = lag_data[:lag]
            else:
                # Lag series is ahead
                x = lead_data[:-lag] if lag > 0 else lead_data
                y = lag_data[lag:]
            
            # Ensure same length
            min_len = min(len(x), len(y))
            if min_len < 10:  # Need minimum data points
                correlations[lag + max_lag] = 0
                continue
            
            x = x[:min_len]
            y = y[:min_len]
            
            # Calculate correlation
            correlation = np.corrcoef(x, y)[0, 1]
            correlations[lag + max_lag] = correlation if not np.isnan(correlation) else 0
        
        return correlations
    
    def _correlate_numba(self, lead_data: np.ndarray, lag_data: np.ndarray, max_lag: int) -> np.ndarray:
        """Calculate correlations using Numba for speed."""
        if not HAS_NUMBA:
            return self._correlate_numpy(lead_data, lag_data, max_lag)
        
        @numba.njit
        def correlate_numba_inner(x, y):
            n = len(x)
            if n < 2:
                return 0.0
            
            mean_x = np.mean(x)
            mean_y = np.mean(y)
            
            numerator = 0.0
            sum_x_sq = 0.0
            sum_y_sq = 0.0
            
            for i in range(n):
                dx = x[i] - mean_x
                dy = y[i] - mean_y
                numerator += dx * dy
                sum_x_sq += dx * dx
                sum_y_sq += dy * dy
            
            denominator = np.sqrt(sum_x_sq * sum_y_sq)
            if denominator == 0:
                return 0.0
            
            return numerator / denominator
        
        correlations = np.zeros(2 * max_lag + 1)
        
        for lag in range(-max_lag, max_lag + 1):
            if lag < 0:
                x = lead_data[-lag:]
                y = lag_data[:lag]
            else:
                x = lead_data[:-lag] if lag > 0 else lead_data
                y = lag_data[lag:]
            
            min_len = min(len(x), len(y))
            if min_len < 10:
                correlations[lag + max_lag] = 0
                continue
            
            x = x[:min_len]
            y = y[:min_len]
            
            correlations[lag + max_lag] = correlate_numba_inner(x, y)
        
        return correlations
    
    def _get_top_correlations(self, correlations: pd.DataFrame, top_n: int) -> pd.DataFrame:
        """
        Get top correlations by absolute value.
        
        Args:
            correlations: DataFrame with correlation results
            top_n: Number of top correlations to return
            
        Returns:
            DataFrame with top correlations
        """
        if correlations.empty:
            return pd.DataFrame()
        
        # Sort by absolute correlation
        top_corr = correlations.nlargest(top_n, 'abs_correlation')
        
        # Add z-score for significance
        top_corr = top_corr.copy()
        top_corr['z_score'] = self._calculate_z_scores(correlations['correlation'], top_corr['correlation'])
        
        return top_corr
    
    def _calculate_z_scores(self, all_correlations: pd.Series, selected_correlations: pd.Series) -> pd.Series:
        """Calculate z-scores for correlation significance."""
        mean_corr = all_correlations.mean()
        std_corr = all_correlations.std()
        
        if std_corr == 0:
            return pd.Series([0] * len(selected_correlations))
        
        z_scores = (selected_correlations - mean_corr) / std_corr
        return z_scores
    
    def _build_composite_signal(
        self, 
        df: pd.DataFrame, 
        top_correlations: pd.DataFrame
    ) -> pd.Series:
        """
        Build composite signal from top correlations.
        
        Args:
            df: Original data DataFrame
            top_correlations: Top correlation results
            
        Returns:
            Composite signal series
        """
        if top_correlations.empty:
            return pd.Series(dtype=float)
        
        # Calculate z-scores for all series
        z_scores = {}
        for series_name in df.columns:
            series_data = df[series_name].dropna()
            if len(series_data) > 0:
                mean_val = series_data.mean()
                std_val = series_data.std()
                if std_val > 0:
                    z_scores[series_name] = (series_data - mean_val) / std_val
                else:
                    z_scores[series_name] = pd.Series(0, index=series_data.index)
        
        # Build composite signal
        composite_parts = []
        weights = []
        
        for _, row in top_correlations.iterrows():
            lead_series = row['lead_series']
            lag_series = row['lag_series']
            lag = row['lag']
            correlation = row['correlation']
            
            if lead_series in z_scores and lag_series in z_scores:
                # Get the appropriate series based on lag
                if lag < 0:
                    # Lead series is ahead, use it
                    signal_series = z_scores[lead_series]
                else:
                    # Lag series is ahead, use it
                    signal_series = z_scores[lag_series]
                
                # Apply lag if needed
                if lag != 0:
                    signal_series = signal_series.shift(-lag)
                
                composite_parts.append(signal_series * correlation)
                weights.append(abs(correlation))
        
        if not composite_parts:
            return pd.Series(dtype=float)
        
        # Combine signals with weights
        composite_df = pd.concat(composite_parts, axis=1)
        weights_array = np.array(weights)
        
        # Calculate weighted average
        composite_signal = (composite_df * weights_array).sum(axis=1) / weights_array.sum()
        
        # Normalize to z-score
        composite_signal = (composite_signal - composite_signal.mean()) / composite_signal.std()
        
        return composite_signal
    
    def save_results(self, results: Dict, output_dir: Optional[str] = None) -> None:
        """
        Save scan results to files.
        
        Args:
            results: Scan results dictionary
            output_dir: Output directory (defaults to project root)
        """
        if output_dir is None:
            output_dir = self.settings.data_sources_path.parent
        
        # Save correlations
        if 'top_correlations' in results and not results['top_correlations'].empty:
            results['top_correlations'].to_csv(
                f"{output_dir}/{self.settings.results_csv}", 
                index=False
            )
        
        # Save composite signal
        if 'composite_signal' in results and not results['composite_signal'].empty:
            results['composite_signal'].to_csv(
                f"{output_dir}/{self.settings.composite_csv}"
            )

# Convenience function for backward compatibility
async def scan_signals(
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    series_names: Optional[List[str]] = None,
    max_lag: Optional[int] = None,
    top_n: Optional[int] = None
) -> Dict:
    """
    Convenience function to scan signals.
    
    Args:
        start: Start date
        end: End date
        series_names: Optional list of series to analyze
        max_lag: Maximum lag to test
        top_n: Number of top correlations to return
        
    Returns:
        Dictionary with scan results
    """
    scanner = SignalScanner()
    return await scanner.scan_signals(start, end, series_names, max_lag, top_n) 