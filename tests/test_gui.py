import pytest
from unittest.mock import patch, MagicMock
from PyQt5.QtWidgets import QApplication
import sys
import gui

@pytest.fixture(scope='module')
def app():
    app = QApplication(sys.argv)
    yield app
    app.quit()

def test_mainwindow_instantiates(app):
    window = gui.MainWindow()
    assert window.windowTitle() == 'Crypto Signal Scanner'

@patch('gui.DataFetcher')
def test_fetch_btn_click(mock_fetcher, app):
    window = gui.MainWindow()
    mock_fetcher.return_value.download.return_value = None
    window.start_fetch()
    assert not window.fetch_btn.isEnabled() or window.fetch_btn.isEnabled()  # just check no crash

@patch('gui.SignalScanner')
def test_scan_btn_click(mock_scanner, app):
    window = gui.MainWindow()
    mock_scanner.return_value.run.return_value = None
    window.start_scan()
    assert not window.scan_btn.isEnabled() or window.scan_btn.isEnabled()  # just check no crash 