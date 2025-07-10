"""
Async data fetcher for multiple sources with proper error handling and caching.
"""

import asyncio
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import pandas as pd
import yaml
from pathlib import Path

from config import get_settings
from fetchers import fetcher_registry

logger = logging.getLogger(__name__)

class DataFetcher:
    """Main data fetcher that coordinates multiple sources."""
    
    def __init__(self):
        self.settings = get_settings()
        self.data_sources = self._load_data_sources()
    
    def _load_data_sources(self) -> Dict:
        """Load data sources configuration from YAML file."""
        try:
            with open(self.settings.data_sources_path, 'r') as f:
                config = yaml.safe_load(f)
            return config
        except Exception as e:
            logger.error(f"Failed to load data sources: {e}")
            return {"series": [], "defaults": {}}
    
    async def fetch_all_series(
        self, 
        start: datetime, 
        end: datetime,
        series_names: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Fetch all configured series for the given time range.
        
        Args:
            start: Start date
            end: End date
            series_names: Optional list of series to fetch (defaults to all)
            
        Returns:
            DataFrame with datetime index and series as columns
        """
        series_list = self.data_sources.get('series', [])
        
        if series_names:
            series_list = [s for s in series_list if s['name'] in series_names]
        
        if not series_list:
            logger.warning("No series configured for fetching")
            return pd.DataFrame()
        
        # Fetch all series concurrently
        tasks = []
        for series_config in series_list:
            task = self._fetch_single_series(start, end, series_config)
            tasks.append(task)
        
        # Wait for all fetches to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Combine results
        series_dict = {}
        for i, result in enumerate(results):
            series_name = series_list[i]['name']
            if isinstance(result, Exception):
                logger.error(f"Failed to fetch {series_name}: {result}")
                continue
            if not result.empty:
                series_dict[series_name] = result
        
        if not series_dict:
            logger.warning("No data fetched from any source")
            return pd.DataFrame()
        
        # Combine all series into a single DataFrame
        df = pd.concat(series_dict, axis=1)
        df.columns = series_dict.keys()
        
        # Align all series to the same date range
        df = self._align_dataframe(df)
        
        logger.info(f"Successfully fetched {len(df.columns)} series with {len(df)} data points")
        return df
    
    async def _fetch_single_series(
        self, 
        start: datetime, 
        end: datetime, 
        series_config: Dict
    ) -> pd.Series:
        """
        Fetch a single series using the appropriate fetcher.
        
        Args:
            start: Start date
            end: End date
            series_config: Series configuration dictionary
            
        Returns:
            pandas Series with datetime index
        """
        series_name = series_config['name']
        source = series_config['source']
        
        logger.info(f"Fetching {series_name} from {source}")
        
        # Get the appropriate fetcher
        fetcher = fetcher_registry.get(source)
        if not fetcher:
            logger.error(f"No fetcher found for source: {source}")
            return pd.Series(dtype=float)
        
        try:
            # Prepare kwargs for the fetcher
            kwargs = {k: v for k, v in series_config.items() 
                     if k not in ['name', 'source', 'freq']}
            
            # Fetch the data
            series = await fetcher.fetch(start, end, **kwargs)
            
            if series.empty:
                logger.warning(f"No data returned for {series_name}")
                return pd.Series(dtype=float)
            
            logger.info(f"Successfully fetched {series_name}: {len(series)} data points")
            return series
            
        except Exception as e:
            logger.error(f"Failed to fetch {series_name}: {e}")
            return pd.Series(dtype=float)
    
    def _align_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Align all series in the DataFrame to the same date range.
        
        Args:
            df: Input DataFrame with datetime index
            
        Returns:
            Aligned DataFrame
        """
        if df.empty:
            return df
        
        # Remove any completely empty columns
        df = df.dropna(axis=1, how='all')
        
        # Forward fill within reasonable limits (max 5 days)
        df = df.fillna(method='ffill', limit=5)
        
        # Remove any remaining NaN values
        df = df.dropna()
        
        return df
    
    def get_series_info(self) -> List[Dict]:
        """Get information about all configured series."""
        return self.data_sources.get('series', [])
    
    def get_defaults(self) -> Dict:
        """Get default parameters."""
        return self.data_sources.get('defaults', {})

# Convenience function for backward compatibility
async def fetch_data(
    start: datetime, 
    end: datetime,
    series_names: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Convenience function to fetch data.
    
    Args:
        start: Start date
        end: End date
        series_names: Optional list of series to fetch
        
    Returns:
        DataFrame with all series data
    """
    fetcher = DataFetcher()
    return await fetcher.fetch_all_series(start, end, series_names) 