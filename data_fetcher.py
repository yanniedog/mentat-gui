"""
Fetches historical Binance klines and stores them in SQLite
"""
import os
import time
import sqlite3
import logging
from datetime import datetime, timezone
from typing import List, Optional

import requests
import pandas as pd
from config import (
    DB_DIR, LOG_DIR, BINANCE_API_BASE_URL,
    MAX_KLINES, RATE_LIMIT_DELAY, SYMBOL, INTERVAL
)
import sys
from config import setup_abort_on_warning_or_error
setup_abort_on_warning_or_error('data_fetcher.log')

# Setup logging
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, 'data_fetcher.log'), mode='w'),
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

class DataFetcher:
    def __init__(self, symbol: str = SYMBOL, interval: str = INTERVAL):
        self.symbol = symbol.upper()
        self.interval = interval
        os.makedirs(DB_DIR, exist_ok=True)

    def _get_db_path(self) -> str:
        return os.path.join(DB_DIR, f"{self.symbol}_{self.interval}.db")

    def _ensure_tables(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS klines (
                open_time INTEGER PRIMARY KEY,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL,
                close_time INTEGER NOT NULL,
                quote_asset_volume REAL NOT NULL,
                num_trades INTEGER NOT NULL,
                taker_buy_base REAL NOT NULL,
                taker_buy_quote REAL NOT NULL
            )
            """
        )
        conn.commit()

    def _get_last_timestamp(self, conn: sqlite3.Connection) -> Optional[int]:
        cur = conn.execute("SELECT MAX(open_time) FROM klines")
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else None

    def _fetch_klines(self, start_ms: int, end_ms: int) -> List[List]:
        all_klines: List[List] = []
        current = start_ms
        session = requests.Session()
        while current < end_ms:
            params = {
                'symbol': self.symbol,
                'interval': self.interval,
                'startTime': current,
                'endTime': end_ms,
                'limit': MAX_KLINES
            }
            try:
                resp = session.get(BINANCE_API_BASE_URL, params=params, timeout=20)
                resp.raise_for_status()
                klines = resp.json()
            except Exception as e:
                logger.error(f"Binance request failed: {e}")
                break

            if not klines:
                break
            all_klines.extend(klines)
            current = klines[-1][0] + 1
            logger.info(f"Fetched {len(klines)} klines; advancing to {current}")
            time.sleep(RATE_LIMIT_DELAY)
        return all_klines

    def _process_klines(self, raw: List[List]) -> pd.DataFrame:
        cols = [
            'open_time','open','high','low','close','volume',
            'close_time','quote_asset_volume','num_trades',
            'taker_buy_base','taker_buy_quote','ignore'
        ]
        df = pd.DataFrame(raw, columns=cols)
        df = df.drop(columns=['ignore'])
        df['open_time'] = pd.to_datetime(df['open_time'], unit='ms', utc=True)
        df['close_time'] = pd.to_datetime(df['close_time'], unit='ms', utc=True)
        for c in ['open','high','low','close','volume',
                  'quote_asset_volume','taker_buy_base','taker_buy_quote']:
            df[c] = pd.to_numeric(df[c], errors='coerce')
        df = df.drop_duplicates(subset=['open_time']).dropna()
        return df

    def download(self) -> None:
        db_path = self._get_db_path()
        conn = sqlite3.connect(db_path)
        self._ensure_tables(conn)
        last_ts = self._get_last_timestamp(conn)
        conn.close()

        start_ms = last_ts + 1 if last_ts else int(
            datetime(2017,1,1, tzinfo=timezone.utc).timestamp() * 1000
        )
        end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        raw = self._fetch_klines(start_ms, end_ms)
        if not raw:
            logger.info("No new klines to download.")
            return
        df = self._process_klines(raw)

        conn = sqlite3.connect(db_path)
        self._ensure_tables(conn)
        df['open_time'] = df['open_time'].astype(int) // 10**6
        df['close_time'] = df['close_time'].astype(int) // 10**6
        df.to_sql('klines', conn, if_exists='append', index=False)
        conn.close()
        logger.info(f"Inserted {len(df)} rows into {db_path}")


if __name__ == '__main__':
    fetcher = DataFetcher()
    fetcher.download() 