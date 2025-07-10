"""
Binance data fetcher with async support.
"""

import sqlite3
import asyncio
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
import pandas as pd
import aiohttp
from .base import BaseFetcher, register_fetcher
from config import get_settings, get_logger
import numpy as np

logger = get_logger(__name__)

@register_fetcher("binance")
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
    
    def _setup_database(self):
        """Setup SQLite database for caching."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS klines (
                    timestamp INTEGER PRIMARY KEY,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume REAL
                )
            ''')
            
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON klines(timestamp)')
            conn.commit()
            conn.close()
            
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
        Fetch Binance kline data.
        
        Args:
            start: Start date
            end: End date
            symbol: Trading pair symbol
            interval: Time interval
            
        Returns:
            pandas Series with close prices
        """
        symbol = symbol or self.settings.symbol
        interval = interval or self.settings.interval
        
        # Check cache first
        cached_data = self._read_from_cache(start, end, symbol, interval)
        if cached_data is not None and not cached_data.empty:
            logger.info(f"Using cached Binance data for {symbol}")
            return cached_data
        
        # Fetch from API
        return await self._fetch_from_api(start, end, symbol, interval)
    
    def _read_from_cache(
        self, 
        start: datetime, 
        end: datetime, 
        symbol: str, 
        interval: str
    ) -> Optional[pd.Series]:
        """Read data from local cache."""
        try:
            conn = sqlite3.connect(self.db_path)
            
            start_ts = int(start.timestamp() * 1000)
            end_ts = int(end.timestamp() * 1000)
            
            query = '''
                SELECT timestamp, close 
                FROM klines 
                WHERE timestamp BETWEEN ? AND ?
                ORDER BY timestamp
            '''
            
            df = pd.read_sql_query(query, conn, params=(start_ts, end_ts))
            conn.close()
            
            if df.empty:
                return None
            
            # Convert timestamp to datetime
            df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('datetime', inplace=True)
            
            return df['close']
            
        except Exception as e:
            logger.warning(f"Failed to read from database: {e}")
            return None
    
    async def _fetch_from_api(
        self, 
        start: datetime, 
        end: datetime, 
        symbol: str, 
        interval: str
    ) -> pd.Series:
        """Fetch data from Binance API."""
        try:
            async with aiohttp.ClientSession() as session:
                # Calculate timestamps
                start_ts = int(start.timestamp() * 1000)
                end_ts = int(end.timestamp() * 1000)
                
                # Fetch data in chunks
                all_klines = []
                current_start = start_ts
                
                while current_start < end_ts:
                    params = {
                        'symbol': symbol,
                        'interval': interval,
                        'startTime': current_start,
                        'endTime': min(current_start + (self.max_klines * self._get_interval_ms(interval)), end_ts),
                        'limit': self.max_klines
                    }
                    
                    data = await self._make_request(session, self.base_url, params)
                    
                    if not data:
                        break
                    
                    all_klines.extend(data)
                    
                    # Update start time for next request
                    if len(data) < self.max_klines:
                        break
                    
                    current_start = data[-1][0] + 1
                    
                    # Rate limiting
                    await asyncio.sleep(self.rate_limit_delay)
                
                if not all_klines:
                    return pd.Series()
                
                # Convert to DataFrame
                df = pd.DataFrame(all_klines, columns=[
                    'timestamp', 'open', 'high', 'low', 'close', 'volume',
                    'close_time', 'quote_volume', 'trades', 'taker_buy_base',
                    'taker_buy_quote', 'ignore'
                ])
                
                # Convert types
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                df['close'] = df['close'].astype(float)
                
                # Set index and return close prices
                df.set_index('timestamp', inplace=True)
                
                # Store in cache
                self._store_in_cache(df, symbol, interval)
                
                return df['close']
                
        except Exception as e:
            logger.error(f"Failed to fetch Binance data: {e}")
            return pd.Series()
    
    def _store_in_cache(self, df: pd.DataFrame, symbol: str, interval: str):
        """Store data in local cache."""
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Prepare data for insertion
            cache_data = []
            for timestamp, row in df.iterrows():
                cache_data.append((
                    int(timestamp.timestamp() * 1000),
                    float(row['open']),
                    float(row['high']),
                    float(row['low']),
                    float(row['close']),
                    float(row['volume'])
                ))
            
            # Insert or replace data
            cursor = conn.cursor()
            cursor.executemany('''
                INSERT OR REPLACE INTO klines 
                (timestamp, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', cache_data)
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.warning(f"Failed to store data in database: {e}")
    
    def _get_interval_ms(self, interval: str) -> int:
        """Convert interval string to milliseconds."""
        interval_map = {
            '1m': 60 * 1000,
            '3m': 3 * 60 * 1000,
            '5m': 5 * 60 * 1000,
            '15m': 15 * 60 * 1000,
            '30m': 30 * 60 * 1000,
            '1h': 60 * 60 * 1000,
            '2h': 2 * 60 * 60 * 1000,
            '4h': 4 * 60 * 60 * 1000,
            '6h': 6 * 60 * 60 * 1000,
            '8h': 8 * 60 * 60 * 1000,
            '12h': 12 * 60 * 60 * 1000,
            '1d': 24 * 60 * 60 * 1000,
            '3d': 3 * 24 * 60 * 60 * 1000,
            '1w': 7 * 24 * 60 * 60 * 1000,
            '1M': 30 * 24 * 60 * 60 * 1000
        }
        return interval_map.get(interval, 24 * 60 * 60 * 1000)  # Default to 1d 