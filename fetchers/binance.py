"""
Binance data fetcher with async support.
"""

import logging
import sqlite3
import asyncio
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
import pandas as pd
import aiohttp
from .base import BaseFetcher, fetcher_registry
from config import get_settings
import numpy as np

logger = logging.getLogger(__name__)

class BinanceFetcher(BaseFetcher):
    """Fetcher for Binance market data."""
    
    def __init__(self):
        super().__init__()
        self.settings = get_settings()
        self.base_url = self.settings.binance_api_base_url
        self.rate_limit_delay = self.settings.rate_limit_delay
        self.max_klines = self.settings.max_klines
        
        # Database setup
        self.db_path = self.settings.db_path / f"{self.settings.symbol}_{self.settings.interval}.db"
        self._setup_database()
    
    def _setup_database(self) -> None:
        """Setup SQLite database with proper indexing."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Create table with UNIQUE constraint
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS klines (
                        open_time INTEGER PRIMARY KEY,
                        open_price REAL,
                        high_price REAL,
                        low_price REAL,
                        close_price REAL,
                        volume REAL,
                        close_time INTEGER,
                        quote_asset_volume REAL,
                        number_of_trades INTEGER,
                        taker_buy_base_asset_volume REAL,
                        taker_buy_quote_asset_volume REAL
                    )
                ''')
                
                # Create index for faster queries
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_open_time 
                    ON klines(open_time)
                ''')
                
                conn.commit()
                
        except Exception as e:
            logger.error(f"Failed to setup database: {e}")
    
    async def fetch(
        self, 
        start: datetime, 
        end: datetime, 
        symbol: str = None,
        interval: str = None,
        **kwargs: Any
    ) -> pd.Series:
        """
        Fetch Binance market data.
        
        Args:
            start: Start date
            end: End date
            symbol: Trading symbol (defaults to config)
            interval: Time interval (defaults to config)
            **kwargs: Additional parameters
            
        Returns:
            pandas Series with datetime index
        """
        self._validate_date_range(start, end)
        
        symbol = symbol or self.settings.symbol
        interval = interval or self.settings.interval
        
        # Try to get from database first
        db_data = self._get_from_database(start, end, symbol, interval)
        if not db_data.empty:
            logger.info(f"Using cached Binance data for {symbol}")
            return db_data
        
        # Fetch from API
        api_data = await self._fetch_from_api(start, end, symbol, interval)
        if not api_data.empty:
            # Store in database
            self._store_in_database(api_data, symbol, interval)
        
        return api_data
    
    def _get_from_database(
        self, 
        start: datetime, 
        end: datetime, 
        symbol: str, 
        interval: str
    ) -> pd.Series:
        """Get data from local database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                start_ts = int(start.timestamp() * 1000)
                end_ts = int(end.timestamp() * 1000)
                
                query = '''
                    SELECT open_time, close_price 
                    FROM klines 
                    WHERE open_time BETWEEN ? AND ?
                    ORDER BY open_time
                '''
                
                df = pd.read_sql_query(query, conn, params=(start_ts, end_ts))
                
                if df.empty:
                    return pd.Series(dtype=float)
                
                # Convert timestamp to datetime
                df['date'] = pd.to_datetime(df['open_time'], unit='ms')
                return df.set_index('date')['close_price']
                
        except Exception as e:
            logger.warning(f"Failed to read from database: {e}")
            return pd.Series(dtype=float)
    
    async def _fetch_from_api(
        self, 
        start: datetime, 
        end: datetime, 
        symbol: str, 
        interval: str
    ) -> pd.Series:
        """Fetch data from Binance API."""
        start_ts = int(start.timestamp() * 1000)
        end_ts = int(end.timestamp() * 1000)
        
        all_klines = []
        current_start = start_ts
        
        async with aiohttp.ClientSession() as session:
            while current_start < end_ts:
                params = {
                    'symbol': symbol,
                    'interval': interval,
                    'startTime': current_start,
                    'endTime': end_ts,
                    'limit': self.max_klines
                }
                
                try:
                    data = await self._make_request(session, self.base_url, params)
                    klines = data.get('klines', [])
                    
                    if not klines:
                        break
                    
                    all_klines.extend(klines)
                    
                    # Update start time for next request
                    last_ts = int(klines[-1][0])
                    if last_ts <= current_start:
                        break
                    current_start = last_ts + 1
                    
                    # Rate limiting
                    await asyncio.sleep(self.rate_limit_delay)
                    
                except Exception as e:
                    logger.error(f"Failed to fetch Binance data: {e}")
                    break
        
        if not all_klines:
            return pd.Series(dtype=float)
        
        # Convert to DataFrame
        df = pd.DataFrame(all_klines, columns=[
            'open_time', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_volume', 'trades', 'taker_base', 'taker_quote'
        ])
        
        # Convert types
        df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
        df['close'] = pd.to_numeric(df['close'])
        
        # Create series
        series = df.set_index('open_time')['close']
        
        # Filter to requested range
        series = series[(series.index >= start) & (series.index <= end)]
        
        return self._align_series(series, 'D')
    
    def _store_in_database(self, data: pd.Series, symbol: str, interval: str) -> None:
        """Store data in local database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Convert series to DataFrame for storage
                df = data.reset_index()
                df.columns = ['open_time', 'close_price']
                df['open_time'] = df['open_time'].astype(np.int64) // 10**6  # Convert to milliseconds
                
                # Insert with conflict resolution
                df.to_sql('klines', conn, if_exists='append', index=False, method='multi')
                
        except Exception as e:
            logger.warning(f"Failed to store data in database: {e}")

# Register the fetcher
fetcher_registry['binance'] = BinanceFetcher() 