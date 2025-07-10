import os
import importlib
import pytest

def test_config_env(monkeypatch):
    monkeypatch.setenv('DB_DIR', 'testdb')
    monkeypatch.setenv('LOG_DIR', 'testlogs')
    monkeypatch.setenv('BINANCE_API_BASE_URL', 'http://test')
    monkeypatch.setenv('MAX_KLINES', '123')
    monkeypatch.setenv('RATE_LIMIT_DELAY', '0.5')
    monkeypatch.setenv('SYMBOL', 'ETHUSDT')
    monkeypatch.setenv('INTERVAL', '4h')
    monkeypatch.setenv('MAX_LAG', '7')
    monkeypatch.setenv('TOP_N', '3')
    monkeypatch.setenv('LOOKBACK_DAYS', '10')
    monkeypatch.setenv('RESULTS_CSV', 'r.csv')
    monkeypatch.setenv('COMPOSITE_CSV', 'c.csv')
    monkeypatch.setenv('CORR_PLOT', 'cp.png')
    monkeypatch.setenv('SIGNAL_PLOT', 'sp.png')
    import config
    importlib.reload(config)
    assert config.DB_DIR == 'testdb'
    assert config.LOG_DIR == 'testlogs'
    assert config.BINANCE_API_BASE_URL == 'http://test'
    assert config.MAX_KLINES == 123
    assert config.RATE_LIMIT_DELAY == 0.5
    assert config.SYMBOL == 'ETHUSDT'
    assert config.INTERVAL == '4h'
    assert config.MAX_LAG == 7
    assert config.TOP_N == 3
    assert config.LOOKBACK_DAYS == 10
    assert config.RESULTS_CSV == 'r.csv'
    assert config.COMPOSITE_CSV == 'c.csv'
    assert config.CORR_PLOT == 'cp.png'
    assert config.SIGNAL_PLOT == 'sp.png' 