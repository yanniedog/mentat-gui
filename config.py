"""
Global configuration for Crypto Signal Scanner
"""
import os
from pathlib import Path
from typing import Optional
from pydantic import BaseSettings, Field

# Project root directory
PROJECT_ROOT = Path(__file__).resolve().parent

class Settings(BaseSettings):
    """Application settings with environment variable support"""
    
    # Data sources configuration
    data_sources_file: str = Field(default="data_sources.yaml", env="DATA_SOURCES")
    
    # Database settings
    db_dir: str = Field(default="database", env="DB_DIR")
    
    # Logging
    log_dir: str = Field(default="logs", env="LOG_DIR")
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    
    # Binance settings
    binance_api_base_url: str = Field(
        default="https://api.binance.com/api/v3/klines", 
        env="BINANCE_API_BASE_URL"
    )
    binance_api_key: Optional[str] = Field(default=None, env="BINANCE_API_KEY")
    max_klines: int = Field(default=1000, env="MAX_KLINES")
    rate_limit_delay: float = Field(default=0.15, env="RATE_LIMIT_DELAY")
    
    # Default trading settings
    symbol: str = Field(default="BTCUSDT", env="SYMBOL")
    interval: str = Field(default="1d", env="INTERVAL")
    
    # Analysis parameters
    max_lag: int = Field(default=5, env="MAX_LAG")
    top_n: int = Field(default=2, env="TOP_N")
    lookback_days: int = Field(default=365, env="LOOKBACK_DAYS")
    
    # Output files
    results_csv: str = Field(default="results.csv", env="RESULTS_CSV")
    composite_csv: str = Field(default="composite_signal.csv", env="COMPOSITE_CSV")
    corr_plot: str = Field(default="correlations.png", env="CORR_PLOT")
    signal_plot: str = Field(default="composite_signal.png", env="SIGNAL_PLOT")
    
    # External API keys
    fred_api_key: Optional[str] = Field(default=None, env="FRED_API_KEY")
    
    class Config:
        env_file = ".env"
        case_sensitive = False
    
    @property
    def db_path(self) -> Path:
        """Get database directory path"""
        return PROJECT_ROOT / self.db_dir
    
    @property
    def log_path(self) -> Path:
        """Get log directory path"""
        return PROJECT_ROOT / self.log_dir
    
    @property
    def data_sources_path(self) -> Path:
        """Get data sources configuration file path"""
        return PROJECT_ROOT / self.data_sources_file

# Global settings instance
_settings: Optional[Settings] = None

def get_settings() -> Settings:
    """Get application settings singleton"""
    global _settings
    if _settings is None:
        _settings = Settings()
        # Ensure directories exist
        _settings.db_path.mkdir(exist_ok=True)
        _settings.log_path.mkdir(exist_ok=True)
    return _settings

# Backward compatibility - export settings as module-level variables
def _export_settings():
    """Export settings as module-level variables for backward compatibility"""
    settings = get_settings()
    globals().update({
        'DB_DIR': str(settings.db_path),
        'LOG_DIR': str(settings.log_path),
        'BINANCE_API_BASE_URL': settings.binance_api_base_url,
        'MAX_KLINES': settings.max_klines,
        'RATE_LIMIT_DELAY': settings.rate_limit_delay,
        'SYMBOL': settings.symbol,
        'INTERVAL': settings.interval,
        'MAX_LAG': settings.max_lag,
        'TOP_N': settings.top_n,
        'LOOKBACK_DAYS': settings.lookback_days,
        'RESULTS_CSV': settings.results_csv,
        'COMPOSITE_CSV': settings.composite_csv,
        'CORR_PLOT': settings.corr_plot,
        'SIGNAL_PLOT': settings.signal_plot,
    })

# Initialize settings and export for backward compatibility
_export_settings() 