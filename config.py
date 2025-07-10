"""
Global configuration for Crypto Signal Scanner
"""
import os
import json
import logging

# Directories
DB_DIR = os.getenv('DB_DIR', 'database')
LOG_DIR = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

# Binance settings
BINANCE_API_BASE_URL = os.getenv('BINANCE_API_BASE_URL', 'https://api.binance.com/api/v3/klines')
MAX_KLINES = int(os.getenv('MAX_KLINES', '1000'))
RATE_LIMIT_DELAY = float(os.getenv('RATE_LIMIT_DELAY', '0.15'))

# Default symbol and interval
SYMBOL = os.getenv('SYMBOL', 'BTCUSDT')
INTERVAL = os.getenv('INTERVAL', '1d')

# Load indicator params from JSON
with open('indicator_params.json', 'r') as f:
    params = json.load(f)
SERIES_LIST = params['series_list']
MAX_LAG = int(os.getenv('MAX_LAG', str(params['max_lag'])))
TOP_N = int(os.getenv('TOP_N', str(params['top_n'])))
LOOKBACK_DAYS = int(os.getenv('LOOKBACK_DAYS', str(params['lookback_days'])))

# Output files
RESULTS_CSV   = os.getenv('RESULTS_CSV', 'results.csv')
COMPOSITE_CSV = os.getenv('COMPOSITE_CSV', 'composite_signal.csv')
CORR_PLOT     = os.getenv('CORR_PLOT', 'correlations.png')
SIGNAL_PLOT   = os.getenv('SIGNAL_PLOT', 'composite_signal.png') 

# Utility to set up abort-on-warning-or-error handler

def setup_abort_on_warning_or_error(logfile='app.log'):
    """
    Sets up logging so that any WARNING or ERROR causes immediate abort,
    and the abort message is logged to both file and console.
    """
    class AbortOnWarningOrError(logging.Handler):
        def emit(self, record):
            if record.levelno >= logging.WARNING:
                msg = f"Aborting due to log: {record.levelname} - {record.getMessage()}"
                logging.getLogger().removeHandler(self)
                logging.error(msg)
                print(msg)
                import sys
                sys.exit(1)
    
    # Remove all handlers
    for h in list(logging.getLogger().handlers):
        logging.getLogger().removeHandler(h)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s',
        handlers=[
            logging.FileHandler(os.path.join(LOG_DIR, logfile), mode='w'),
            logging.StreamHandler()
        ]
    )
    logging.getLogger().addHandler(AbortOnWarningOrError()) 