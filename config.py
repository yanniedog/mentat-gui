"""
Global configuration for Crypto Signal Scanner
"""
import os
from pathlib import Path
from typing import Optional
from pydantic_settings import BaseSettings
from pydantic import Field
import logging
import sys
import warnings

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

def setup_abort_on_warning_or_error(log_file: str):
    """Configure logging to file and INSTANTLY abort on ANY error or warning."""
    log_path = get_settings().log_path / log_file
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(levelname)s %(message)s',
        handlers=[
            logging.FileHandler(log_path, mode='a', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    def INSTANT_ABORT(message="INSTANT ABORT DUE TO ERROR OR WARNING"):
        """INSTANTLY abort to command line - no continuation"""
        # Use direct write to avoid recursion
        sys.__stdout__.write(f"\n{message}\n")
        sys.__stdout__.write("INSTANT ABORT TO COMMAND LINE\n")
        sys.__stdout__.flush()
        os._exit(1)  # Force immediate exit - no cleanup, no continuation
    
    # Override ALL logging to INSTANTLY abort
    class InstantAbortHandler(logging.Handler):
        def emit(self, record):
            if record.levelno >= logging.WARNING:
                INSTANT_ABORT(f"LOGGING ERROR: {record.levelname} - {record.getMessage()}")
    
    logging.getLogger().addHandler(InstantAbortHandler())
    
    # Override warnings to INSTANTLY abort
    def instant_abort_on_warning(message, category, filename, lineno, file=None, line=None):
        INSTANT_ABORT(f"WARNING: {category.__name__}: {message} at {filename}:{lineno}")
    
    warnings.showwarning = instant_abort_on_warning
    warnings.filterwarnings("error")  # Convert warnings to errors
    warnings.simplefilter("error")  # Make all warnings errors
    
    # Override exception handler to INSTANTLY abort
    def instant_exception_handler(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            return
        INSTANT_ABORT(f"UNCAUGHT EXCEPTION: {exc_type.__name__}: {exc_value}")
    
    sys.excepthook = instant_exception_handler
    
    # Override thread exception handler to INSTANTLY abort
    import threading
    def instant_thread_exception_handler(args):
        INSTANT_ABORT(f"THREAD EXCEPTION: {args.exc_type.__name__}: {args.exc_value}")
    
    threading.excepthook = instant_thread_exception_handler
    
    # Override sys.exit to INSTANTLY abort
    original_exit = sys.exit
    def instant_exit(code=0):
        if code != 0:
            INSTANT_ABORT(f"SYSTEM EXIT WITH CODE: {code}")
        original_exit(code)
    sys.exit = instant_exit
    
    # Set up signal handlers for INSTANT abort
    import signal
    def instant_signal_handler(signum, frame):
        INSTANT_ABORT(f"SIGNAL RECEIVED: {signum}")
    
    signal.signal(signal.SIGTERM, instant_signal_handler)
    signal.signal(signal.SIGINT, instant_signal_handler)
    
    # Override print to INSTANTLY abort on error keywords
    original_print = print
    def instant_abort_print(*args, **kwargs):
        message = ' '.join(str(arg) for arg in args)
        if any(keyword in message.lower() for keyword in ['error', 'warning', 'exception', 'traceback', 'failed', 'resourcewarning']):
            INSTANT_ABORT(f"ERROR DETECTED IN PRINT: {message}")
        original_print(*args, **kwargs)
    
    # Replace built-in print
    import builtins
    builtins.print = instant_abort_print

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