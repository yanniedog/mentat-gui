"""
FRED (Federal Reserve Economic Data) fetcher.
"""

import logging
from typing import Dict, Any
from datetime import datetime
import pandas as pd
from .base import BaseFetcher, fetcher_registry
from config import get_settings
import aiohttp

logger = logging.getLogger(__name__)

class FredFetcher(BaseFetcher):
    """Fetcher for FRED economic data."""
    
    def __init__(self):
        super().__init__()
        self.settings = get_settings()
        self.api_key = self.settings.fred_api_key
        if not self.api_key:
            logger.warning("FRED_API_KEY not set - some series may not be available")
        
        self.base_url = "https://api.stlouisfed.org/fred/series/observations"
    
    async def fetch(
        self, 
        start: datetime, 
        end: datetime, 
        series_id: str,
        **kwargs: Any
    ) -> pd.Series:
        """
        Fetch FRED series data.
        
        Args:
            start: Start date
            end: End date
            series_id: FRED series ID
            **kwargs: Additional parameters
            
        Returns:
            pandas Series with datetime index
        """
        self._validate_date_range(start, end)
        
        params = {
            'series_id': series_id,
            'observation_start': start.strftime('%Y-%m-%d'),
            'observation_end': end.strftime('%Y-%m-%d'),
            'frequency': kwargs.get('freq', 'd'),
            'units': kwargs.get('units', 'lin'),
            'file_type': 'json'
        }
        
        if self.api_key:
            params['api_key'] = self.api_key
        
        async with aiohttp.ClientSession() as session:
            data = await self._make_request(session, self.base_url, params)
        
        # Parse observations
        observations = data.get('observations', [])
        if not observations:
            logger.warning(f"No data found for FRED series {series_id}")
            return pd.Series(dtype=float)
        
        # Convert to DataFrame
        df = pd.DataFrame(observations)
        df['date'] = pd.to_datetime(df['date'])
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        
        # Create series with datetime index
        series = df.set_index('date')['value']
        
        # Align to daily frequency
        return self._align_series(series, 'D')

# Register the fetcher
fetcher_registry['fred'] = FredFetcher() 