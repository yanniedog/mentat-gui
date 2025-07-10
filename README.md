# Mentat GUI - Crypto Signal Scanner

A high-performance, multi-source financial data analysis tool that identifies lead-lag relationships between various economic indicators and cryptocurrency prices.

## Features

- **Multi-Source Data**: Pull data from FRED, Yahoo Finance, Binance, Google Trends, and Fear & Greed Index
- **Vectorized Analysis**: Fast correlation analysis using NumPy with optional Numba acceleration
- **Async I/O**: Concurrent data fetching for improved performance
- **Statistical Correctness**: Proper z-score calculations and correlation analysis without synthetic forward-filling
- **Extensible Architecture**: Plugin-based fetcher system for easy addition of new data sources
- **Modern GUI**: Responsive PyQt5 interface with progress tracking
- **CLI Support**: Command-line interface for batch processing and automation

## Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/your-org/mentat-gui.git
cd mentat-gui

# Install dependencies
pip install -r requirements.txt

# For development (includes testing and linting tools)
pip install -e ".[dev]"

# For maximum performance (includes Numba)
pip install -e ".[fast]"
```

### Configuration

1. **Set up API keys** (optional but recommended):
   ```bash
   # Create .env file
   echo "FRED_API_KEY=your_fred_api_key_here" > .env
   echo "BINANCE_API_KEY=your_binance_api_key_here" >> .env
   ```

2. **Configure data sources** by editing `data_sources.yaml`:
   ```yaml
   series:
     - name: "CPI-AU"
       source: "fred"
       id: "CPALTT01AUA661S"
       freq: "M"
     
     - name: "S&P 500"
       source: "yahoo"
       ticker: "^GSPC"
       freq: "D"
     
     - name: "BTCUSDT"
       source: "binance"
       symbol: "BTCUSDT"
       interval: "1d"
       freq: "D"

   defaults:
     lookback_days: 730
     max_lag: 10
     top_n: 5
   ```

### Usage

#### Command Line Interface

```bash
# Basic scan with default settings
python -m mentat_gui

# Custom date range
python -m mentat_gui --start 2023-01-01 --end 2024-01-01

# Specific series and parameters
python -m mentat_gui --series "BTCUSDT,Fear-and-Greed" --max-lag 15 --top 3

# Skip plots for faster processing
python -m mentat_gui --no-plots --verbose
```

#### GUI Application

```bash
# Launch the GUI
python gui.py
```

## Adding New Data Sources

The system uses a plugin architecture for data fetchers. To add a new source:

1. **Create a new fetcher** in the `fetchers/` directory:
   ```python
   # fetchers/my_source.py
   from .base import BaseFetcher, fetcher_registry
   
   class MySourceFetcher(BaseFetcher):
       async def fetch(self, start, end, **kwargs):
           # Implement your data fetching logic
           pass
   
   # Register the fetcher
   fetcher_registry['my_source'] = MySourceFetcher()
   ```

2. **Add to data sources** in `data_sources.yaml`:
   ```yaml
   series:
     - name: "My Data"
       source: "my_source"
       param1: "value1"
       param2: "value2"
   ```

3. **Run the scan** - the new source will be automatically included!

## Architecture

### Core Components

- **`config.py`**: Centralized configuration using Pydantic settings
- **`data_fetcher.py`**: Async data fetching coordinator
- **`signal_scanner.py`**: Vectorized correlation analysis engine
- **`fetchers/`**: Plugin-based data source implementations
- **`gui.py`**: PyQt5-based user interface

### Data Flow

1. **Configuration**: Load settings from environment variables and `data_sources.yaml`
2. **Data Fetching**: Concurrent async requests to multiple data sources
3. **Processing**: Vectorized correlation analysis with proper statistical corrections
4. **Output**: Results saved to CSV files and optional plots generated

### Performance Optimizations

- **Async I/O**: All network requests use `aiohttp` for concurrent fetching
- **Vectorized Operations**: NumPy-based correlation calculations
- **Optional Numba**: JIT compilation for 5-10x speedup on CPU-intensive operations
- **Caching**: Local SQLite storage for Binance data, pickle cache for Google Trends
- **Database Indexing**: Proper indexes for fast data retrieval

## Development

### Code Quality

The project uses modern Python tooling:

```bash
# Format code
black --line-length 120 .

# Lint code
ruff check .

# Sort imports
isort .

# Run tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=. --cov-report=html
```

### Testing

```bash
# Run all tests
pytest

# Run specific test categories
pytest -m "not slow"  # Skip slow tests
pytest -m integration  # Run only integration tests

# Run GUI tests (requires X11 or headless display)
pytest tests/test_gui.py
```

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests and linting
5. Submit a pull request

## Configuration Reference

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `FRED_API_KEY` | FRED API key for economic data | None |
| `BINANCE_API_KEY` | Binance API key | None |
| `DATA_SOURCES` | Path to data sources config | `data_sources.yaml` |
| `DB_DIR` | Database directory | `database` |
| `LOG_DIR` | Log directory | `logs` |
| `MAX_LAG` | Maximum lag for correlation analysis | 10 |
| `TOP_N` | Number of top correlations to return | 5 |
| `LOOKBACK_DAYS` | Default lookback period | 730 |

### Data Sources Configuration

The `data_sources.yaml` file supports the following source types:

#### FRED (Federal Reserve Economic Data)
```yaml
- name: "CPI"
  source: "fred"
  id: "CPIAUCSL"  # FRED series ID
  freq: "M"       # Frequency (D, W, M, Q, A)
```

#### Yahoo Finance
```yaml
- name: "S&P 500"
  source: "yahoo"
  ticker: "^GSPC"  # Stock ticker symbol
  freq: "D"
```

#### Binance
```yaml
- name: "BTCUSDT"
  source: "binance"
  symbol: "BTCUSDT"  # Trading pair
  interval: "1d"     # Time interval
  freq: "D"
```

#### Google Trends
```yaml
- name: "Bitcoin Trends"
  source: "trends"
  kw: "bitcoin"  # Search keyword
  freq: "D"
```

#### Fear & Greed Index
```yaml
- name: "Fear & Greed"
  source: "fng"
  freq: "D"
```

## Troubleshooting

### Common Issues

1. **No data returned**: Check API keys and network connectivity
2. **Slow performance**: Enable Numba acceleration with `pip install numba`
3. **GUI not responding**: Long operations run in background threads
4. **Import errors**: Ensure all dependencies are installed

### Logging

Enable verbose logging for debugging:
```bash
python -m mentat_gui --verbose
```

Logs are saved to the `logs/` directory by default.

## License

MIT License - see LICENSE file for details.

## Acknowledgments

- FRED API for economic data
- Yahoo Finance for stock data
- Binance for cryptocurrency data
- Google Trends for search data
- Alternative.me for Fear & Greed Index 