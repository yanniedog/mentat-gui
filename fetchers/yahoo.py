"""
Yahoo Finance fetcher.
"""

import asyncio
from typing import Dict, Any
from datetime import datetime, timedelta
import pandas as pd
import aiohttp
from .base import BaseFetcher, register_fetcher
from config import get_settings, get_logger

logger = get_logger(__name__)

@register_fetcher("yahoo")
class YahooFetcher(BaseFetcher):
    """Fetcher for Yahoo Finance data."""
    
    def __init__(self):
        super().__init__()
        self.settings = get_settings()
        self.base_url = "https://query1.finance.yahoo.com/v8/finance/chart"
    
    async def fetch(
        self, 
        start: datetime, 
        end: datetime, 
        ticker: str = None,
        **kwargs: Any
    ) -> pd.Series:
        """
        Fetch Yahoo Finance data.
        
        Args:
            start: Start date
            end: End date
            ticker: Stock ticker symbol
            
        Returns:
            pandas Series with price data
        """
        symbol = ticker or kwargs.get('symbol')
        if not symbol:
            logger.error("No ticker symbol provided")
            return pd.Series()
        
        return await self._fetch_from_api(start, end, symbol)
    
    async def _fetch_from_api(
        self, 
        start: datetime, 
        end: datetime, 
        symbol: str
    ) -> pd.Series:
        """Fetch data from Yahoo Finance API."""
        try:
            async with aiohttp.ClientSession() as session:
                # Convert dates to timestamps
                start_ts = int(start.timestamp())
                end_ts = int(end.timestamp())
                
                url = f"{self.base_url}/{symbol}"
                params = {
                    'period1': start_ts,
                    'period2': end_ts,
                    'interval': '1d',
                    'includePrePost': 'false',
                    'events': 'div,split'
                }
                
                data = await self._make_request(session, url, params)
                
                if not data or 'chart' not in data:
                    logger.warning(f"No data found for Yahoo ticker {symbol}")
                    return pd.Series()
                
                chart_data = data['chart']
                if 'result' not in chart_data or not chart_data['result']:
                    logger.warning(f"No data found for Yahoo ticker {symbol}")
                    return pd.Series()
                
                result = chart_data['result'][0]
                
                # Extract timestamps and prices
                timestamps = result.get('timestamp', [])
                quote = result.get('indicators', {}).get('quote', [{}])[0]
                close_prices = quote.get('close', [])
                
                if not timestamps or not close_prices:
                    logger.warning(f"No price data found for Yahoo ticker {symbol}")
                    return pd.Series()
                
                # Create DataFrame
                df = pd.DataFrame({
                    'timestamp': timestamps,
                    'close': close_prices
                })
                
                # Convert timestamp to datetime
                df['date'] = pd.to_datetime(df['timestamp'], unit='s')
                df.set_index('date', inplace=True)
                
                # Remove any NaN values
                series = df['close'].dropna()
                
                return series
                
        except Exception as e:
            logger.error(f"Failed to fetch Yahoo data for {symbol}: {e}")
            return pd.Series() 