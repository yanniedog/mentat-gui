"""
Fear & Greed Index fetcher from Alternative.me.
"""

import logging
from typing import Dict, Any
from datetime import datetime, timedelta
import pandas as pd
from .base import BaseFetcher, fetcher_registry
import aiohttp

logger = logging.getLogger(__name__)

class FearGreedFetcher(BaseFetcher):
    """Fetcher for Fear & Greed Index data."""
    
    def __init__(self):
        super().__init__()
        self.base_url = "https://api.alternative.me/fng"
    
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
            **kwargs: Additional parameters
            
        Returns:
            pandas Series with datetime index
        """
        self._validate_date_range(start, end)
        
        # Calculate number of days
        days = (end - start).days
        
        params = {
            'limit': min(days, 365),  # API limit
            'format': 'json'
        }
        
        async with aiohttp.ClientSession() as session:
            data = await self._make_request(session, self.base_url, params)
        
        # Parse data
        values = data.get('data', [])
        if not values:
            logger.warning("No Fear & Greed data found")
            return pd.Series(dtype=float)
        
        # Convert to DataFrame
        records = []
        for item in values:
            try:
                date_str = item.get('timestamp')
                value_str = item.get('value')
                
                if date_str and value_str:
                    date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    value = int(value_str)
                    records.append({'date': date, 'value': value})
            except (ValueError, TypeError) as e:
                logger.warning(f"Failed to parse Fear & Greed data point: {e}")
                continue
        
        if not records:
            logger.warning("No valid Fear & Greed data points found")
            return pd.Series(dtype=float)
        
        # Create DataFrame
        df = pd.DataFrame(records)
        df = df.sort_values('date')
        
        # Create series with datetime index
        series = df.set_index('date')['value']
        
        # Filter to requested date range
        series = series[(series.index >= start) & (series.index <= end)]
        
        # Align to daily frequency
        return self._align_series(series, 'D')

# Register the fetcher
fetcher_registry['fng'] = FearGreedFetcher() 