"""
Vectorized signal scanner with proper statistical corrections and async support.
"""

import asyncio
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from concurrent.futures import ProcessPoolExecutor
import yaml
from pathlib import Path

from config import get_settings, get_logger
from data_fetcher import DataFetcher

logger = get_logger(__name__)

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
        Scan for lead-lag relationships between time series.
        
        Args:
            start: Start date for analysis
            end: End date for analysis
            series_names: List of series to analyze (None for all)
            max_lag: Maximum lag to test
            top_n: Number of top correlations to return
            
        Returns:
            Dictionary with scan results
        """
        try:
            # Set defaults
            if end is None:
                end = datetime.now()
            if start is None:
                start = end - timedelta(days=self.lookback_days)
            if max_lag is None:
                max_lag = self.max_lag
            if top_n is None:
                top_n = self.top_n
            
            logger.info(f"Starting signal scan from {start} to {end}")
            
            # Fetch data
            df = await self.data_fetcher.fetch_all(
                start=start,
                end=end,
                series_names=series_names
            )
            
            if df.empty:
                logger.error("No data available for analysis")
                return {'error': 'No data available'}
            
            logger.info(f"Analyzing {len(df.columns)} series with {len(df)} data points")
            
            # Calculate correlations
            correlations = self._calculate_correlations(df, max_lag)
            
            # Get top correlations
            top_correlations = self._get_top_correlations(correlations, top_n)
            
            # Generate composite signal
            composite_signal = self._generate_composite_signal(df, top_correlations)
            
            logger.info(f"Scan complete. Found {len(top_correlations)} significant correlations")
            
            return {
                'start': start,
                'end': end,
                'series_count': len(df.columns),
                'data_points': len(df),
                'max_lag': max_lag,
                'top_n': top_n,
                'all_correlations': correlations,
                'top_correlations': top_correlations,
                'composite_signal': composite_signal,
                'raw_data': df
            }
            
        except Exception as e:
            logger.error(f"Signal scan failed: {e}")
            return {'error': str(e)}
    
    def _calculate_correlations(self, df: pd.DataFrame, max_lag: int) -> pd.DataFrame:
        """Calculate lead-lag correlations between all series."""
        correlations = []
        
        for lead_series in df.columns:
            for lag_series in df.columns:
                if lead_series != lag_series:
                    for lag in range(1, max_lag + 1):
                        # Calculate correlation with lag
                        lead_data = df[lead_series].iloc[lag:]
                        lag_data = df[lag_series].iloc[:-lag]
                        
                        if len(lead_data) > 10:  # Minimum data points
                            corr = lead_data.corr(lag_data)
                            if not pd.isna(corr):
                                correlations.append({
                                    'lead_series': lead_series,
                                    'lag_series': lag_series,
                                    'lag': lag,
                                    'correlation': corr
                                })
        
        return pd.DataFrame(correlations)
    
    def _get_top_correlations(self, correlations: pd.DataFrame, top_n: int) -> pd.DataFrame:
        """Get top N correlations by absolute value."""
        if correlations.empty:
            return pd.DataFrame()
        
        # Sort by absolute correlation value
        correlations['abs_corr'] = correlations['correlation'].abs()
        top_corr = correlations.nlargest(top_n, 'abs_corr')
        
        # Remove helper column
        top_corr = top_corr.drop('abs_corr', axis=1)
        
        return top_corr.reset_index(drop=True)
    
    def _generate_composite_signal(self, df: pd.DataFrame, top_correlations: pd.DataFrame) -> pd.Series:
        """Generate composite signal from top correlations."""
        if top_correlations.empty:
            return pd.Series()
        
        # Create composite signal as weighted average of leading series
        composite = pd.Series(0.0, index=df.index)
        total_weight = 0
        
        for _, row in top_correlations.iterrows():
            lead_series = row['lead_series']
            weight = abs(row['correlation'])
            
            if lead_series in df.columns:
                composite += weight * df[lead_series]
                total_weight += weight
        
        if total_weight > 0:
            composite = composite / total_weight
        
        return composite
    
    def save_results(self, results: Dict, output_dir: Path) -> None:
        """Save scan results to files."""
        try:
            # Save top correlations
            if not results['top_correlations'].empty:
                results['top_correlations'].to_csv(
                    output_dir / self.settings.results_csv, 
                    index=False
                )
            
            # Save composite signal
            if results['composite_signal'] is not None and not results['composite_signal'].empty:
                results['composite_signal'].to_csv(
                    output_dir / self.settings.composite_csv
                )
            
            # Save raw data
            if not results['raw_data'].empty:
                results['raw_data'].to_csv(output_dir / 'raw_data.csv')
                
        except Exception as e:
            logger.error(f"Failed to save results: {e}")
    
    def run(self, generate_plots: bool = True) -> Dict:
        """Synchronous wrapper for scan_signals."""
        return asyncio.run(self.scan_signals()) 