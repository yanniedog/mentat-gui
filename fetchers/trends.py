"""
Google Trends fetcher with caching.
"""

import logging
import pickle
from pathlib import Path
from typing import Dict, Any
from datetime import datetime, timedelta
import pandas as pd
from pytrends.request import TrendReq
from .base import BaseFetcher, fetcher_registry
from config import get_settings

logger = logging.getLogger(__name__)

class TrendsFetcher(BaseFetcher):
    """Fetcher for Google Trends data with caching."""
    
    def __init__(self):
        super().__init__()
        self.settings = get_settings()
        self.cache_dir = self.settings.log_path / "trends_cache"
        self.cache_dir.mkdir(exist_ok=True)
        
        # Initialize pytrends
        self.pytrends = TrendReq(hl='en-US', tz=360)
    
    def _get_cache_path(self, keyword: str, start: datetime, end: datetime) -> Path:
        """Get cache file path for the given parameters."""
        start_str = start.strftime('%Y%m%d')
        end_str = end.strftime('%Y%m%d')
        filename = f"{keyword}_{start_str}_{end_str}.pkl"
        return self.cache_dir / filename
    
    def _load_from_cache(self, cache_path: Path) -> pd.Series:
        """Load data from cache file."""
        try:
            with open(cache_path, 'rb') as f:
                return pickle.load(f)
        except (FileNotFoundError, pickle.PickleError):
            return None
    
    def _save_to_cache(self, cache_path: Path, data: pd.Series) -> None:
        """Save data to cache file."""
        try:
            with open(cache_path, 'wb') as f:
                pickle.dump(data, f)
        except Exception as e:
            logger.warning(f"Failed to save trends cache: {e}")
    
    async def fetch(
        self, 
        start: datetime, 
        end: datetime, 
        keyword: str,
        **kwargs: Any
    ) -> pd.Series:
        """
        Fetch Google Trends data.
        
        Args:
            start: Start date
            end: End date
            keyword: Search keyword
            **kwargs: Additional parameters
            
        Returns:
            pandas Series with datetime index
        """
        self._validate_date_range(start, end)
        
        # Check cache first
        cache_path = self._get_cache_path(keyword, start, end)
        cached_data = self._load_from_cache(cache_path)
        if cached_data is not None:
            logger.info(f"Using cached trends data for {keyword}")
            return cached_data
        
        # Fetch from Google Trends
        try:
            # Build payload
            timeframe = f"{start.strftime('%Y-%m-%d')} {end.strftime('%Y-%m-%d')}"
            
            # Build payload
            self.pytrends.build_payload([keyword], cat=0, timeframe=timeframe, geo='', gprop='')
            
            # Get interest over time
            interest_df = self.pytrends.interest_over_time()
            
            if interest_df.empty:
                logger.warning(f"No trends data found for keyword: {keyword}")
                return pd.Series(dtype=float)
            
            # Extract the keyword column
            series = interest_df[keyword]
            
            # Filter to requested date range
            series = series[(series.index >= start) & (series.index <= end)]
            
            # Align to daily frequency
            aligned_series = self._align_series(series, 'D')
            
            # Cache the result
            self._save_to_cache(cache_path, aligned_series)
            
            return aligned_series
            
        except Exception as e:
            logger.error(f"Failed to fetch trends data for {keyword}: {e}")
            return pd.Series(dtype=float)

# Register the fetcher
fetcher_registry['trends'] = TrendsFetcher() 