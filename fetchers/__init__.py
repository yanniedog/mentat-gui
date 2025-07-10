"""
Data fetchers package.
"""

from .base import fetcher_registry, register_fetcher
from .binance import BinanceFetcher
from .trends import TrendsFetcher
from .fred import FredFetcher
from .yahoo import YahooFetcher
from .fng import FearGreedFetcher

__all__ = [
    'fetcher_registry',
    'register_fetcher',
    'BinanceFetcher',
    'TrendsFetcher',
    'FredFetcher',
    'YahooFetcher',
    'FearGreedFetcher'
] 