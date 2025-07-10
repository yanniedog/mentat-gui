"""
FRED (Federal Reserve Economic Data) fetcher.
"""

import asyncio
from typing import Dict, Any
from datetime import datetime, timedelta
import pandas as pd
import aiohttp
from .base import BaseFetcher, register_fetcher
from config import get_settings, get_logger

logger = get_logger(__name__)

@register_fetcher("fred")
class FredFetcher(BaseFetcher):
    """Fetcher for FRED economic data."""
    
    def __init__(self):
        super().__init__()
        self.settings = get_settings()
        self.api_key = self.settings.fred_api_key
        self.base_url = "https://api.stlouisfed.org/fred/series/observations"
        
        if not self.api_key:
            logger.warning("FRED_API_KEY not set - some series may not be available")
    
    async def fetch(
        self, 
        start: datetime, 
        end: datetime, 
        id: str = None,
        **kwargs: Any
    ) -> pd.Series:
        """
        Fetch FRED series data.
        
        Args:
            start: Start date
            end: End date
            id: FRED series ID
            
        Returns:
            pandas Series with economic data
        """
        series_id = id or kwargs.get('series_id')
        if not series_id:
            logger.error("No FRED series ID provided")
            return pd.Series()
        
        return await self._fetch_from_api(start, end, series_id)
    
    async def _fetch_from_api(
        self, 
        start: datetime, 
        end: datetime, 
        series_id: str
    ) -> pd.Series:
        """Fetch data from FRED API."""
        if not self.api_key:
            logger.error("FRED API key not configured")
            return pd.Series()
        
        try:
            async with aiohttp.ClientSession() as session:
                params = {
                    'series_id': series_id,
                    'api_key': self.api_key,
                    'file_type': 'json',
                    'observation_start': start.strftime('%Y-%m-%d'),
                    'observation_end': end.strftime('%Y-%m-%d'),
                    'frequency': 'd'  # Daily frequency
                }
                
                data = await self._make_request(session, self.base_url, params)
                
                if not data or 'observations' not in data:
                    logger.warning(f"No data found for FRED series {series_id}")
                    return pd.Series()
                
                # Convert to DataFrame
                observations = data['observations']
                df = pd.DataFrame(observations)
                
                if df.empty:
                    logger.warning(f"No data found for FRED series {series_id}")
                    return pd.Series()
                
                # Convert date and value columns
                df['date'] = pd.to_datetime(df['date'])
                df['value'] = pd.to_numeric(df['value'], errors='coerce')
                
                # Set index and return values
                df.set_index('date', inplace=True)
                series = df['value']
                
                # Remove any '.' values (FRED's way of indicating no data)
                series = series[series != '.']
                
                return series
                
        except Exception as e:
            logger.error(f"Failed to fetch FRED data for {series_id}: {e}")
            return pd.Series() 