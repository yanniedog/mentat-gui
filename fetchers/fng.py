"""
Fear & Greed Index fetcher.
"""

import asyncio
from typing import Dict, Any
from datetime import datetime, timedelta
import pandas as pd
import aiohttp
from .base import BaseFetcher, register_fetcher
from config import get_settings, get_logger

logger = get_logger(__name__)

@register_fetcher("fng")
class FearGreedFetcher(BaseFetcher):
    """Fetcher for Fear & Greed Index data."""
    
    def __init__(self):
        super().__init__()
        self.settings = get_settings()
        self.base_url = "https://api.alternative.me/fng/"
    
    async def fetch(
        self, 
        start: datetime, 
        end: datetime, 
        **kwargs: Any
    ) -> pd.Series:
        """
        Fetch Fear & Greed Index data.
        
        Args:
            start: Start date
            end: End date
            
        Returns:
            pandas Series with Fear & Greed Index values
        """
        return await self._fetch_from_api(start, end)
    
    async def _fetch_from_api(
        self, 
        start: datetime, 
        end: datetime
    ) -> pd.Series:
        """Fetch data from Fear & Greed Index API."""
        try:
            async with aiohttp.ClientSession() as session:
                # Calculate number of days to fetch
                days = (end - start).days
                
                params = {
                    'limit': min(days, 365),  # API limit
                    'format': 'json'
                }
                
                data = await self._make_request(session, self.base_url, params)
                
                if not data or 'data' not in data:
                    logger.warning("No Fear & Greed data found")
                    return pd.Series()
                
                # Parse data points
                data_points = []
                for point in data['data']:
                    try:
                        date_str = point.get('timestamp')
                        value_str = point.get('value')
                        
                        if date_str and value_str:
                            date = datetime.strptime(date_str, '%Y-%m-%d')
                            value = int(value_str)
                            
                            # Filter to requested date range
                            if start <= date <= end:
                                data_points.append({
                                    'date': date,
                                    'value': value
                                })
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Failed to parse Fear & Greed data point: {e}")
                        continue
                
                if not data_points:
                    logger.warning("No valid Fear & Greed data points found")
                    return pd.Series()
                
                # Create DataFrame
                df = pd.DataFrame(data_points)
                df.set_index('date', inplace=True)
                
                # Sort by date
                df.sort_index(inplace=True)
                
                return df['value']
                
        except Exception as e:
            logger.error(f"Failed to fetch Fear & Greed data: {e}")
            return pd.Series() 