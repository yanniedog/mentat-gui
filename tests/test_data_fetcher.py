import os
import sqlite3
import pytest
from unittest.mock import patch, MagicMock
from data_fetcher import DataFetcher

def test_db_path(tmp_path):
    fetcher = DataFetcher(symbol='BTCUSDT', interval='1d')
    fetcher.DB_DIR = tmp_path
    db_path = fetcher._get_db_path()
    assert db_path.endswith('BTCUSDT_1d.db')

def test_ensure_tables(tmp_path):
    db_file = tmp_path / 'test.db'
    conn = sqlite3.connect(db_file)
    fetcher = DataFetcher()
    fetcher._ensure_tables(conn)
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='klines'")
    assert cur.fetchone()[0] == 'klines'
    conn.close()

@patch('data_fetcher.requests.Session')
def test_fetch_klines(mock_session):
    fetcher = DataFetcher()
    mock_resp = MagicMock()
    mock_resp.json.return_value = [[1,2,3,4,5,6,7,8,9,10,11,12]]
    mock_resp.raise_for_status.return_value = None
    mock_session.return_value.get.return_value = mock_resp
    klines = fetcher._fetch_klines(1, 2)
    assert klines[0][0] == 1 