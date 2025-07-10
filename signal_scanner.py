"""
Scans lead-lag correlations and generates composite signals
"""
import os
import logging
import datetime as dt
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, List, Optional

import pandas as pd
import requests
from scipy.stats import pearsonr
from pytrends.request import TrendReq
from config import (
    DB_DIR, LOG_DIR, SERIES_LIST, MAX_LAG,
    TOP_N, LOOKBACK_DAYS, RESULTS_CSV, COMPOSITE_CSV,
    CORR_PLOT, SIGNAL_PLOT
)
import sys
from config import setup_abort_on_warning_or_error
setup_abort_on_warning_or_error('scanner.log')

# Suppress pandas FutureWarning about downcasting
pd.set_option('future.no_silent_downcasting', True)

# Setup logging
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, 'scanner.log'), mode='w'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Log warnings and errors but don't abort
class WarningErrorHandler(logging.Handler):
    def emit(self, record):
        if record.levelno >= logging.WARNING:
            print(f"Warning/Error: {record.levelname} - {record.getMessage()}")

logging.getLogger().addHandler(WarningErrorHandler())
session = requests.Session()

class SignalScanner:
    def __init__(self, symbol: str = 'BTCUSDT'):
        self.symbol = symbol

    def _get_db_path(self, interval: str = '1d') -> str:
        return os.path.join(DB_DIR, f"{self.symbol}_{interval}.db")

    def _store_klines(self) -> pd.Series:
        db = self._get_db_path()
        conn = sqlite3.connect(db)
        df = pd.read_sql("SELECT open_time, close FROM klines ORDER BY open_time", conn)
        conn.close()
        df['date'] = pd.to_datetime(df['open_time'], unit='ms')
        series = df.set_index('date')['close'].asfreq('D').ffill().rename('BTCUSD')
        return series

    def _retry_request(self, url: str, params=None, max_retries: int = 3) -> Any:
        backoff = 1
        for attempt in range(1, max_retries+1):
            try:
                resp = session.get(url, params=params, timeout=15)
                resp.raise_for_status()
                return resp
            except Exception as e:
                logger.warning(f"Error fetching {url}: {e} (attempt {attempt})")
                time.sleep(backoff)
                backoff *= 2
        logger.error(f"Failed to fetch {url} after {max_retries} attempts; skipping this series.")
        return None

    def fetch_yahoo(self, ticker: str, start: dt.datetime, end: dt.datetime) -> pd.Series:
        start_ts = int(start.replace(tzinfo=dt.timezone.utc).timestamp())
        end_ts   = int((end + dt.timedelta(days=1)).replace(tzinfo=dt.timezone.utc).timestamp())
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        resp = self._retry_request(url, params={'period1':start_ts,'period2':end_ts,'interval':'1d'})
        if resp is None:
            logger.warning(f"No data for {ticker} from Yahoo; returning empty series.")
            return pd.Series(dtype=float, name=ticker)
        j = resp.json()
        data = j['chart']['result'][0]
        idx = pd.to_datetime(data['timestamp'], unit='s')
        vals = data['indicators']['adjclose'][0]['adjclose']
        return pd.Series(vals, index=idx).asfreq('D').ffill().rename(ticker)

    def fetch_fng(self, start: dt.datetime, end: dt.datetime) -> pd.Series:
        url = "https://api.alternative.me/fng/"
        j = self._retry_request(url, params={'limit':0,'format':'json'}).json()
        df = pd.DataFrame(j['data'])
        df['timestamp'] = pd.to_datetime(df['timestamp'].astype(int), unit='s')
        series = df.set_index('timestamp')['value'].astype(float)
        result = series[(series.index>=start)&(series.index<=end)].asfreq('D').ffill().rename('Fear & Greed')
        if not result.empty:
            logger.info(f"Fetched Fear & Greed: {result.index.min().date()} to {result.index.max().date()}, {result.count()} records")
            logger.info(f"Fear & Greed first 5 rows:\n{result.head().to_string()}")
        return result

    def fetch_trends(self, kw: str, start: dt.datetime, end: dt.datetime) -> pd.Series:
        tr = TrendReq()
        tr.build_payload([kw], timeframe=f"{start.date()} {end.date()}")
        df = tr.interest_over_time()
        result = df[kw].asfreq('D').ffill().rename(kw)
        if not result.empty:
            logger.info(f"Fetched {kw} Google Trends: {result.index.min().date()} to {result.index.max().date()}, {result.count()} records")
            logger.info(f"{kw} Google Trends first 5 rows:\n{result.head().to_string()}")
        return result

    def get_fred_last_date(self, series_id: str, api_key: str) -> 'datetime.date|None':
        url = "https://api.stlouisfed.org/fred/series/observations"
        params = {
            'series_id': series_id,
            'api_key': api_key,
            'file_type': 'json',
            'sort_order': 'desc',
            'limit': 1
        }
        try:
            resp = requests.get(url, params=params, timeout=10)
            if resp.status_code == 200:
                j = resp.json()
                obs = j.get('observations', [])
                if obs and obs[0]['value'] not in ('', None, '.'):
                    return pd.to_datetime(obs[0]['date']).date()
        except Exception:
            pass
        return None

    def fetch_fred(self, series_id: str, start: dt.datetime, end: dt.datetime) -> pd.Series:
        api_key = '238c842c8b03e34e2d9194781cc335b9'
        today_utc = dt.datetime.utcnow().date()
        # Try primary and fallback series
        series_to_try = [series_id]
        if series_id == 'GOLDPMGBD228NLBM':
            series_to_try.extend(['GOLDAMGBD228NLBM', 'LBMA/GOLD'])
        elif series_id == 'GOLDAMGBD228NLBM':
            series_to_try.extend(['GOLDPMGBD228NLBM', 'LBMA/GOLD'])
        elif series_id == 'LBMA/GOLD':
            series_to_try.extend(['GOLDPMGBD228NLBM', 'GOLDAMGBD228NLBM'])
        for current_series in series_to_try:
            last_date = self.get_fred_last_date(current_series, api_key)
            if not last_date or last_date < start.date():
                continue  # No data for this series in the window
            clamped_end = min(end.date(), last_date)
            url = f"https://api.stlouisfed.org/fred/series/observations"
            params = {
                'series_id': current_series,
                'observation_start': start.strftime('%Y-%m-%d'),
                'observation_end': clamped_end.strftime('%Y-%m-%d'),
                'api_key': api_key,
                'file_type': 'json'
            }
            try:
                resp = requests.get(url, params=params, timeout=10)
                if resp.status_code == 200:
                    j = resp.json()
                    if 'observations' in j:
                        obs = j['observations']
                        if obs:
                            idx = pd.to_datetime([o['date'] for o in obs])
                            vals = [float(o['value']) if o['value'] not in ('', None, '.') else float('nan') for o in obs]
                            series = pd.Series(vals, index=idx).asfreq('D').ffill().rename(current_series)
                            if not series.empty:
                                logger.info(f"Successfully fetched {current_series} from FRED")
                                logger.info(f"{current_series} data: {series.index.min().date()} to {series.index.max().date()}, {series.count()} records")
                                logger.info(f"{current_series} first 5 rows:\n{series.head().to_string()}")
                                return series
            except Exception as e:
                logger.error(f"Unexpected error fetching {current_series} from FRED: {e}")
                continue
        logger.info("No FRED gold series available for this window, falling back to Binance XAUUSDT")
        return self.fetch_binance_xauusdt(start, end)

    def fetch_binance_xauusdt(self, start: dt.datetime, end: dt.datetime) -> pd.Series:
        url = 'https://api.binance.com/api/v3/klines'
        params = {
            'symbol': 'XAUUSDT',
            'interval': '1d',
            'startTime': int(start.timestamp() * 1000),
            'endTime': int(end.timestamp() * 1000),
            'limit': 1000
        }
        try:
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            klines = resp.json()
            idx = [dt.datetime.utcfromtimestamp(k[0] / 1000) for k in klines]
            vals = [float(k[4]) for k in klines]  # close price
            series = pd.Series(vals, index=idx).asfreq('D').ffill().rename('XAUUSDT')
            if not series.empty:
                logger.info(f"Fetched XAUUSDT from Binance: {series.index.min().date()} to {series.index.max().date()}, {series.count()} records")
                logger.info(f"XAUUSDT first 5 rows:\n{series.head().to_string()}")
            return series
        except Exception as e:
            logger.info(f"No Binance XAUUSDT data available for this window.")
            return pd.Series(dtype=float, name='XAUUSDT')

    def _fetch_series(self, cfg: Dict[str, Any], start: dt.datetime, end: dt.datetime):
        if cfg['source']=='yahoo':  return cfg['name'], self.fetch_yahoo(cfg['symbol'],start,end)
        if cfg['source']=='fred':   return cfg['name'], self.fetch_fred(cfg['symbol'],start,end)
        if cfg['source']=='fng':    return cfg['name'], self.fetch_fng(start,end)
        if cfg['source']=='trends': return cfg['name'], self.fetch_trends(cfg['symbol'],start,end)
        raise ValueError(f"Unknown source {cfg['source']}")

    def scan_lead_lag(self, df: pd.DataFrame) -> pd.DataFrame:
        results = []
        for col in df.columns.difference(['BTCUSD']):
            best = {'series':col,'lag':0,'corr':0.0}
            for lag in range(1, MAX_LAG+1):
                x = df[col].shift(lag).dropna()
                y = df['BTCUSD'].reindex(x.index)
                if len(x) < 2 or len(y) < 2:
                    continue
                c,_ = pearsonr(x, y)
                if abs(c) > abs(best['corr']):
                    best.update({'lag':lag,'corr':c})
            results.append(best)
        res_df = pd.DataFrame(results)
        res_df['abs_corr'] = res_df['corr'].abs()
        return res_df.sort_values('abs_corr', ascending=False).drop(columns=['abs_corr']).reset_index(drop=True)

    def run(self, generate_plots: bool = True) -> None:
        now = dt.datetime.utcnow()
        start = now - dt.timedelta(days=LOOKBACK_DAYS)
        data = {}
        # parallel fetch
        with ThreadPoolExecutor(max_workers=len(SERIES_LIST)) as ex:
            futures = {ex.submit(self._fetch_series, cfg, start, now): cfg for cfg in SERIES_LIST}
            for fut in as_completed(futures):
                name, series = fut.result()
                data[name] = series
                logger.info(f"Fetched series {name}")
                # Save each series as CSV for GUI plotting
                try:
                    if series is not None and not series.empty:
                        safe_name = name.replace(' ', '_').replace('/', '_')
                        series.to_csv(f"{safe_name}.csv")
                        logger.info(f"Saved {name} to {safe_name}.csv")
                except Exception as e:
                    logger.warning(f"Could not save {name} as CSV: {e}")
        data['BTCUSD'] = self._store_klines()

        master = pd.DataFrame(data).dropna()
        scores = self.scan_lead_lag(master)
        scores.head(TOP_N).to_csv(RESULTS_CSV, index=False)
        logger.info(f"Top signals saved to {RESULTS_CSV}")

        # composite z-score
        top = scores.head(TOP_N)
        zs = [ (master[r['series']] - master[r['series']].mean()) / master[r['series']].std()
               for _, r in top.iterrows() ]
        comp = pd.concat([z.shift(r['lag']) for z, r in zip(zs, top.to_dict('records'))], axis=1).sum(axis=1)
        comp.to_csv(COMPOSITE_CSV)
        logger.info(f"Composite saved to {COMPOSITE_CSV}")

        # plots - only generate if requested and not in GUI context
        if generate_plots:
            try:
                import matplotlib.pyplot as plt
                plt.figure()
                comp.plot()
                plt.title('Composite Signal')
                plt.tight_layout()
                plt.savefig(SIGNAL_PLOT)

                plt.figure()
                top.set_index('series')['corr'].plot(kind='bar')
                plt.title('Top Correlations')
                plt.tight_layout()
                plt.savefig(CORR_PLOT)

                logger.info("Plots generated")
            except Exception as e:
                logger.warning(f"Plot generation failed: {e}")

if __name__ == '__main__':
    scanner = SignalScanner()
    scanner.run() 