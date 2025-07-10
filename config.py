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
import atexit
import signal
import threading
import time
from datetime import datetime

# Project root directory
PROJECT_ROOT = Path(__file__).resolve().parent

class Settings(BaseSettings):
    """Application settings with environment variable support"""
    
    # Data sources configuration
    data_sources_file: str = Field(default="data_sources.yaml", alias="DATA_SOURCES")
    
    # Database settings
    db_dir: str = Field(default="database", alias="DB_DIR")
    
    # Logging
    log_dir: str = Field(default="logs", alias="LOG_DIR")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    
    # Binance settings
    binance_api_base_url: str = Field(
        default="https://api.binance.com/api/v3/klines", 
        alias="BINANCE_API_BASE_URL"
    )
    binance_api_key: Optional[str] = Field(default=None, alias="BINANCE_API_KEY")
    max_klines: int = Field(default=1000, alias="MAX_KLINES")
    rate_limit_delay: float = Field(default=0.15, alias="RATE_LIMIT_DELAY")
    
    # Default trading settings
    symbol: str = Field(default="BTCUSDT", alias="SYMBOL")
    interval: str = Field(default="1d", alias="INTERVAL")
    
    # Analysis parameters
    max_lag: int = Field(default=5, alias="MAX_LAG")
    top_n: int = Field(default=2, alias="TOP_N")
    lookback_days: int = Field(default=365, alias="LOOKBACK_DAYS")
    
    # Output files
    results_csv: str = Field(default="results.csv", alias="RESULTS_CSV")
    composite_csv: str = Field(default="composite_signal.csv", alias="COMPOSITE_CSV")
    corr_plot: str = Field(default="correlations.png", alias="CORR_PLOT")
    signal_plot: str = Field(default="composite_signal.png", alias="SIGNAL_PLOT")
    
    # External API keys
    fred_api_key: Optional[str] = Field(default=None, alias="FRED_API_KEY")
    
    model_config = {
        "env_file": ".env",
        "case_sensitive": False
    }
    
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

class CentralizedLogger:
    """Centralized logging system that captures ALL output and aborts on errors/warnings."""
    
    def __init__(self, log_file: str = "app.log"):
        self.settings = get_settings()
        self.log_file = log_file
        self.log_path = self.settings.log_path / log_file
        self.original_stdout = sys.stdout
        self.original_stderr = sys.stderr
        self.original_print = print
        self.logger = None
        self.stdout_capture = None
        self.stderr_capture = None
        self._setup_logging()
        self._capture_all_output()
        self._setup_abort_handlers()
    
    def _setup_logging(self):
        """Setup comprehensive logging configuration."""
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Create file handler
        file_handler = logging.FileHandler(self.log_path, mode='a', encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        
        # Create console handler (only for non-error messages)
        console_handler = logging.StreamHandler(sys.__stdout__)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        
        # Create console error handler (for errors/warnings)
        error_handler = logging.StreamHandler(sys.__stderr__)
        error_handler.setLevel(logging.WARNING)
        error_handler.setFormatter(formatter)
        
        # Setup root logger
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)
        self.logger.handlers.clear()  # Remove any existing handlers
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        self.logger.addHandler(error_handler)
        
        # Prevent propagation to avoid duplicate logs
        self.logger.propagate = False
    
    def _capture_all_output(self):
        """Capture all stdout, stderr, and print statements."""
        # Capture stdout
        class StdoutCapture:
            def __init__(self, logger):
                self.logger = logger
                self.original_stdout = sys.__stdout__
            
            def write(self, text):
                if text.strip():  # Only log non-empty text
                    self.logger.info(f"STDOUT: {text.rstrip()}")
                self.original_stdout.write(text)
            
            def flush(self):
                self.original_stdout.flush()
        
        # Capture stderr
        class StderrCapture:
            def __init__(self, logger):
                self.logger = logger
                self.original_stderr = sys.__stderr__
            
            def write(self, text):
                if text.strip():  # Only log non-empty text
                    self.logger.error(f"STDERR: {text.rstrip()}")
                self.original_stderr.write(text)
            
            def flush(self):
                self.original_stderr.flush()
        
        self.stdout_capture = StdoutCapture(self.logger)
        self.stderr_capture = StderrCapture(self.logger)
        
        # Replace stdout and stderr
        sys.stdout = self.stdout_capture
        sys.stderr = self.stderr_capture
        
        # Override print function
        def logged_print(*args, **kwargs):
            message = ' '.join(str(arg) for arg in args)
            if message.strip():
                self.logger.info(f"PRINT: {message}")
            self.original_print(*args, **kwargs)
        
        # Replace built-in print
        import builtins
        builtins.print = logged_print
    
    def _setup_abort_handlers(self):
        """Setup handlers to abort immediately on any error or warning."""
        
        def INSTANT_ABORT(message="INSTANT ABORT DUE TO ERROR OR WARNING"):
            """INSTANTLY abort to command line - no continuation"""
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            abort_message = f"\n[{timestamp}] INSTANT ABORT: {message}\n"
            
            # Log the abort
            self.logger.critical(f"INSTANT ABORT: {message}")
            
            # Write to original stdout/stderr to ensure visibility
            sys.__stdout__.write(abort_message)
            sys.__stdout__.write("INSTANT ABORT TO COMMAND LINE\n")
            sys.__stdout__.flush()
            sys.__stderr__.write(abort_message)
            sys.__stderr__.flush()
            
            # Force immediate exit
            os._exit(1)
        
        # Override logging to abort on warnings and errors
        class AbortOnErrorHandler(logging.Handler):
            def emit(self, record):
                if record.levelno >= logging.WARNING:
                    INSTANT_ABORT(f"LOGGING {record.levelname}: {record.getMessage()}")
        
        self.logger.addHandler(AbortOnErrorHandler())
        
        # Override warnings to abort
        def abort_on_warning(message, category, filename, lineno, file=None, line=None):
            INSTANT_ABORT(f"WARNING: {category.__name__}: {message} at {filename}:{lineno}")
        
        warnings.showwarning = abort_on_warning
        warnings.filterwarnings("error")  # Convert warnings to errors
        warnings.simplefilter("error")  # Make all warnings errors
        
        # Override exception handler to abort
        def abort_exception_handler(exc_type, exc_value, exc_traceback):
            if issubclass(exc_type, KeyboardInterrupt):
                return
            INSTANT_ABORT(f"UNCAUGHT EXCEPTION: {exc_type.__name__}: {exc_value}")
        
        sys.excepthook = abort_exception_handler
        
        # Override sys.exit to abort on non-zero codes
        original_exit = sys.exit
        def abort_exit(code=0):
            if code != 0:
                INSTANT_ABORT(f"SYSTEM EXIT WITH CODE: {code}")
            original_exit(code)
        sys.exit = abort_exit
        
        # Setup signal handlers for abort
        def abort_signal_handler(signum, frame):
            INSTANT_ABORT(f"SIGNAL RECEIVED: {signum}")
        
        signal.signal(signal.SIGTERM, abort_signal_handler)
        signal.signal(signal.SIGINT, abort_signal_handler)
        
        # Setup atexit handler
        def cleanup_handler():
            self.logger.info("Application shutting down")
        
        atexit.register(cleanup_handler)
    
    def get_logger(self, name: str = None) -> logging.Logger:
        """Get a logger instance."""
        if name:
            return logging.getLogger(name)
        return self.logger
    
    def log_and_abort(self, message: str, level: str = "ERROR"):
        """Log a message and abort immediately."""
        if level.upper() == "ERROR":
            self.logger.error(message)
        elif level.upper() == "WARNING":
            self.logger.warning(message)
        elif level.upper() == "CRITICAL":
            self.logger.critical(message)
        else:
            self.logger.error(message)
        # The abort will happen automatically via the handler

# Global logger instance
_centralized_logger: Optional[CentralizedLogger] = None

def setup_centralized_logging(log_file: str = "app.log") -> CentralizedLogger:
    """Setup centralized logging system."""
    global _centralized_logger
    if _centralized_logger is None:
        _centralized_logger = CentralizedLogger(log_file)
    return _centralized_logger

def get_logger(name: str = None) -> logging.Logger:
    """Get a logger instance from the centralized system."""
    if _centralized_logger is None:
        setup_centralized_logging()
    return _centralized_logger.get_logger(name)

# Backward compatibility function
def setup_abort_on_warning_or_error(log_file: str = "app.log"):
    """Backward compatibility function that sets up centralized logging."""
    return setup_centralized_logging(log_file)

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