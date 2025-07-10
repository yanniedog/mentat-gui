"""
Data fetchers package for retrieving financial data from various sources.
"""

from .base import BaseFetcher, fetcher_registry
from .fred import FredFetcher
from .yahoo import YahooFetcher
from .fng import FearGreedFetcher
from .trends import TrendsFetcher
from .binance import BinanceFetcher

__all__ = [
    'BaseFetcher',
    'fetcher_registry',
    'FredFetcher',
    'YahooFetcher', 
    'FearGreedFetcher',
    'TrendsFetcher',
    'BinanceFetcher'
] 