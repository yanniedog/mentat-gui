"""
Yahoo Finance fetcher for stock data.
"""

import logging
from typing import Dict, Any
from datetime import datetime
import pandas as pd
from .base import BaseFetcher, fetcher_registry
import aiohttp

logger = logging.getLogger(__name__)

class YahooFetcher(BaseFetcher):
    """Fetcher for Yahoo Finance data."""
    
    def __init__(self):
        super().__init__()
        self.base_url = "https://query1.finance.yahoo.com/v8/finance/chart"
    
    async def fetch(
        self, 
        start: datetime, 
        end: datetime, 
        ticker: str,
        **kwargs: Any
    ) -> pd.Series:
        """
        Fetch Yahoo Finance data.
        
        Args:
            start: Start date
            end: End date
            ticker: Stock ticker symbol
            **kwargs: Additional parameters
            
        Returns:
            pandas Series with datetime index
        """
        self._validate_date_range(start, end)
        
        # Convert dates to timestamps
        start_ts = int(start.timestamp())
        end_ts = int(end.timestamp())
        
        params = {
            'symbol': ticker,
            'period1': start_ts,
            'period2': end_ts,
            'interval': '1d',
            'includePrePost': 'false',
            'events': 'div,split'
        }
        
        url = f"{self.base_url}/{ticker}"
        
        async with aiohttp.ClientSession() as session:
            data = await self._make_request(session, url, params)
        
        # Parse chart data
        chart = data.get('chart', {})
        if not chart or 'result' not in chart:
            logger.warning(f"No data found for Yahoo ticker {ticker}")
            return pd.Series(dtype=float)
        
        result = chart['result'][0]
        timestamps = result.get('timestamp', [])
        quotes = result.get('indicators', {}).get('quote', [{}])[0]
        close_prices = quotes.get('close', [])
        
        if not timestamps or not close_prices:
            logger.warning(f"No price data found for Yahoo ticker {ticker}")
            return pd.Series(dtype=float)
        
        # Create DataFrame
        df = pd.DataFrame({
            'timestamp': timestamps,
            'close': close_prices
        })
        
        # Convert timestamps to datetime
        df['date'] = pd.to_datetime(df['timestamp'], unit='s')
        
        # Create series with datetime index
        series = df.set_index('date')['close']
        
        # Align to daily frequency
        return self._align_series(series, 'D')

# Register the fetcher
fetcher_registry['yahoo'] = YahooFetcher() 