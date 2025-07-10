"""
Base fetcher class and registry for data sources.
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
import pandas as pd
import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

class BaseFetcher(ABC):
    """Abstract base class for data fetchers."""
    
    def __init__(self, max_retries: int = 3, base_delay: float = 1.0):
        self.max_retries = max_retries
        self.base_delay = base_delay
    
    @abstractmethod
    async def fetch(
        self, 
        start: datetime, 
        end: datetime, 
        **kwargs: Any
    ) -> pd.Series:
        """
        Fetch data for the given time range.
        
        Args:
            start: Start date
            end: End date
            **kwargs: Additional parameters specific to the fetcher
            
        Returns:
            pandas Series with datetime index and numeric values
        """
        pass
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    async def _make_request(
        self, 
        session: aiohttp.ClientSession, 
        url: str, 
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Make HTTP request with retry logic.
        
        Args:
            session: aiohttp session
            url: Request URL
            params: Query parameters
            
        Returns:
            JSON response data
        """
        try:
            async with session.get(url, params=params) as response:
                response.raise_for_status()
                return await response.json()
        except aiohttp.ClientError as e:
            logger.warning(f"Request failed: {e}, retrying...")
            raise
    
    def _validate_date_range(self, start: datetime, end: datetime) -> None:
        """Validate date range parameters."""
        if start >= end:
            raise ValueError("Start date must be before end date")
        if start > datetime.now():
            raise ValueError("Start date cannot be in the future")
    
    def _align_series(self, series: pd.Series, freq: str = 'D') -> pd.Series:
        """
        Align series to consistent frequency without forward-filling.
        
        Args:
            series: Input series
            freq: Target frequency
            
        Returns:
            Aligned series
        """
        # Convert to datetime index if needed
        if not isinstance(series.index, pd.DatetimeIndex):
            series.index = pd.to_datetime(series.index)
        
        # Resample to target frequency, forward-fill only within day
        aligned = series.resample(freq).ffill()
        
        # Remove any NaN values at the beginning
        return aligned.dropna()

# Global fetcher registry
fetcher_registry: Dict[str, BaseFetcher] = {} 