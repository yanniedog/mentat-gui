"""
Async data fetcher for multiple sources with proper error handling and caching.
"""

import asyncio
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import pandas as pd
import yaml
from pathlib import Path

from config import get_settings, get_logger
from fetchers import fetcher_registry

logger = get_logger(__name__)

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
    
    def get_defaults(self) -> Dict:
        """Get default configuration values."""
        return self.data_sources.get("defaults", {})
    
    async def fetch_all(
        self,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        series_names: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Fetch data from all configured sources.
        
        Args:
            start: Start date for data fetching
            end: End date for data fetching
            series_names: Optional list of specific series to fetch
            
        Returns:
            DataFrame with all series as columns
        """
        if end is None:
            end = datetime.now()
        if start is None:
            start = end - timedelta(days=self.settings.lookback_days)
        
        # Get series to fetch
        series_configs = self.data_sources.get("series", [])
        if series_names:
            series_configs = [s for s in series_configs if s.get("name") in series_names]
        
        if not series_configs:
            logger.warning("No series configured for fetching")
            return pd.DataFrame()
        
        # Fetch data from all sources
        results = {}
        for series_config in series_configs:
            series_name = series_config.get("name")
            source = series_config.get("source")
            
            if not series_name or not source:
                continue
            
            result = await self._fetch_series(series_config, start, end)
            if result is not None:
                results[series_name] = result
        
        if not results:
            logger.warning("No data fetched from any source")
            return pd.DataFrame()
        
        # Combine all series into a DataFrame
        df = pd.DataFrame(results)
        
        # Align dates and handle missing values
        df = df.fillna(method='ffill').fillna(method='bfill')
        
        logger.info(f"Successfully fetched {len(df.columns)} series with {len(df)} data points")
        return df
    
    async def _fetch_series(
        self,
        series_config: Dict,
        start: datetime,
        end: datetime
    ) -> Optional[pd.Series]:
        """
        Fetch data for a single series.
        
        Args:
            series_config: Series configuration dictionary
            start: Start date
            end: End date
            
        Returns:
            pandas Series with datetime index
        """
        series_name = series_config.get("name")
        source = series_config.get("source")
        
        logger.info(f"Fetching {series_name} from {source}")
        
        try:
            # Get appropriate fetcher
            if source not in fetcher_registry:
                logger.error(f"No fetcher found for source: {source}")
                return None
            
            fetcher_class = fetcher_registry[source]
            fetcher = fetcher_class()
            
            # Fetch data
            series = await fetcher.fetch(start, end, **series_config)
            
            if series is not None and not series.empty:
                logger.info(f"Successfully fetched {series_name}: {len(series)} data points")
                return series
            else:
                logger.warning(f"No data returned for {series_name}")
                return None
                
        except Exception as e:
            logger.error(f"Failed to fetch {series_name}: {e}")
            return None
    
    def download(self) -> None:
        """Synchronous wrapper for fetch_all."""
        asyncio.run(self.fetch_all()) 