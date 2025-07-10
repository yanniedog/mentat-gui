import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from signal_scanner import SignalScanner

def test_store_klines(tmp_path):
    db_file = tmp_path / 'BTCUSDT_1d.db'
    import sqlite3
    conn = sqlite3.connect(db_file)
    conn.execute("CREATE TABLE klines (open_time INTEGER PRIMARY KEY, close REAL)")
    conn.execute("INSERT INTO klines VALUES (?, ?)", (1234567890000, 10000.0))
    conn.commit()
    conn.close()
    scanner = SignalScanner()
    scanner.symbol = 'BTCUSDT'
    scanner._get_db_path = lambda interval='1d': str(db_file)
    s = scanner._store_klines()
    assert s.iloc[0] == 10000.0

@patch('signal_scanner.SignalScanner.fetch_yahoo')
@patch('signal_scanner.SignalScanner.fetch_fng')
@patch('signal_scanner.SignalScanner.fetch_trends')
def test_fetch_series(mock_trends, mock_fng, mock_yahoo):
    scanner = SignalScanner()
    mock_yahoo.return_value = pd.Series([1,2,3], index=pd.date_range('2020-01-01', periods=3))
    mock_fng.return_value = pd.Series([4,5,6], index=pd.date_range('2020-01-01', periods=3))
    mock_trends.return_value = pd.Series([7,8,9], index=pd.date_range('2020-01-01', periods=3))
    yahoo = {'name':'Gold Futures','source':'yahoo','symbol':'GC=F'}
    fng = {'name':'Fear & Greed','source':'fng','symbol':None}
    trends = {'name':'BTC Trends','source':'trends','symbol':'bitcoin'}
    assert scanner._fetch_series(yahoo, pd.Timestamp('2020-01-01'), pd.Timestamp('2020-01-03'))[0] == 'Gold Futures'
    assert scanner._fetch_series(fng, pd.Timestamp('2020-01-01'), pd.Timestamp('2020-01-03'))[0] == 'Fear & Greed'
    assert scanner._fetch_series(trends, pd.Timestamp('2020-01-01'), pd.Timestamp('2020-01-03'))[0] == 'BTC Trends'

def test_scan_lead_lag():
    scanner = SignalScanner()
    df = pd.DataFrame({
        'BTCUSD': [1,2,3,4,5,6],
        'A': [1,2,3,4,5,6],
        'B': [6,5,4,3,2,1]
    })
    res = scanner.scan_lead_lag(df)
    assert 'series' in res.columns
    assert 'lag' in res.columns
    assert 'corr' in res.columns 