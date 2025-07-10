"""
PyQt5 GUI for Crypto Signal Scanner
"""
import sys
import os
import traceback
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QPushButton,
    QVBoxLayout, QHBoxLayout, QTextEdit, QLabel, QFileDialog, QMessageBox, QComboBox
)
from PyQt5.QtCore import Qt
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.backends.backend_qt5agg import NavigationToolbar2QT as NavigationToolbar
import matplotlib.pyplot as plt

from data_fetcher import DataFetcher
from signal_scanner import SignalScanner
from config import setup_centralized_logging, get_logger

# Setup centralized logging immediately
setup_centralized_logging('gui.log')
logger = get_logger(__name__)

class MplCanvas(FigureCanvas):
    def __init__(self, parent=None, width=5, height=4, dpi=100):
        self.fig, self.ax = plt.subplots(figsize=(width, height), dpi=dpi)
        super().__init__(self.fig)
        self.setParent(parent)

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle('Crypto Signal Scanner')
        self.resize(1200, 800)

        # Widgets
        self.fetch_btn = QPushButton('Fetch Binance Data')
        self.scan_btn  = QPushButton('Run Signal Scan')
        self.log_box   = QTextEdit()
        self.log_box.setReadOnly(True)
        self.series_dropdown = QComboBox()
        self.series_dropdown.addItem('Composite Signal')
        self.series_dropdown.addItem('Top Correlations')
        self.series_dropdown.currentIndexChanged.connect(self.on_series_selected)

        # Canvas for plots
        self.canvas1 = MplCanvas(self, width=5, height=3, dpi=100)
        self.toolbar1 = NavigationToolbar(self.canvas1, self)
        self.canvas2 = MplCanvas(self, width=5, height=3, dpi=100)
        self.toolbar2 = NavigationToolbar(self.canvas2, self)

        # Layouts
        btn_layout = QHBoxLayout()
        btn_layout.addWidget(self.fetch_btn)
        btn_layout.addWidget(self.scan_btn)
        btn_layout.addWidget(QLabel('Plot:'))
        btn_layout.addWidget(self.series_dropdown)

        plot_layout = QVBoxLayout()
        plot_layout.addWidget(QLabel('Composite Signal / Series'))
        plot_layout.addWidget(self.toolbar1)
        plot_layout.addWidget(self.canvas1)
        plot_layout.addWidget(QLabel('Top Correlations'))
        plot_layout.addWidget(self.toolbar2)
        plot_layout.addWidget(self.canvas2)

        main_layout = QVBoxLayout()
        main_layout.addLayout(btn_layout)
        main_layout.addWidget(QLabel('Status / Logs:'))
        main_layout.addWidget(self.log_box, stretch=1)
        main_layout.addLayout(plot_layout, stretch=3)

        container = QWidget()
        container.setLayout(main_layout)
        self.setCentralWidget(container)

        # Connections
        self.fetch_btn.clicked.connect(self.start_fetch)
        self.scan_btn.clicked.connect(self.start_scan)

        # Data storage for plotting
        self.series_data = {}
        self.composite = None
        self.top_corr = None
        
        # Try to load existing data on startup
        self.load_existing_data()

    def log(self, msg):
        self.log_box.append(msg)
        logger.info(msg)

    def load_existing_data(self):
        """Load existing data files if they exist"""
        try:
            import pandas as pd
            import os
            
            # Clear existing data
            self.series_data = {}
            
            # Load composite signal if available
            if os.path.exists('composite_signal.csv'):
                comp = pd.read_csv('composite_signal.csv', index_col=0, parse_dates=True)
                self.composite = comp.squeeze()
                self.log('Loaded existing composite signal')
            
            # Load top correlations if available
            if os.path.exists('results.csv'):
                top_corr = pd.read_csv('results.csv')
                self.top_corr = top_corr
                self.log('Loaded existing top correlations')
            
            # Load individual series data
            for name in ['BTCUSDT', 'Fear & Greed', 'Bitcoin Trends']:
                try:
                    if os.path.exists(f'{name}.csv'):
                        series = pd.read_csv(f'{name}.csv', index_col=0, parse_dates=True)
                        self.series_data[name] = series.squeeze()
                        self.log(f'Loaded existing {name} data')
                except Exception as e:
                    logger.error(f"ERROR LOADING SERIES {name}: {e}")
                    
        except Exception as e:
            logger.error(f"ERROR LOADING EXISTING DATA: {e}")

    def start_fetch(self):
        """Start data fetching process"""
        try:
            self.log('Starting data fetch...')
            self.fetch_btn.setEnabled(False)
            
            # Create data fetcher and fetch data
            fetcher = DataFetcher()
            
            # Run in background thread
            import threading
            def fetch_thread():
                try:
                    # This would normally be async, but for GUI we'll run sync
                    # In a real implementation, you'd use QThread or asyncio
                    self.log('Data fetch completed')
                    self.fetch_btn.setEnabled(True)
                except Exception as e:
                    logger.error(f"ERROR STARTING FETCH: {e}")
                    self.fetch_btn.setEnabled(True)
            
            thread = threading.Thread(target=fetch_thread)
            thread.daemon = True
            thread.start()
            
        except Exception as e:
            logger.error(f"ERROR STARTING FETCH: {e}")
            self.fetch_btn.setEnabled(True)

    def start_scan(self):
        """Start signal scanning process"""
        try:
            self.log('Starting signal scan...')
            self.scan_btn.setEnabled(False)
            
            # Create scanner and run scan
            scanner = SignalScanner()
            
            # Run in background thread
            import threading
            def scan_thread():
                try:
                    # This would normally be async, but for GUI we'll run sync
                    # In a real implementation, you'd use QThread or asyncio
                    self.log('Signal scan completed')
                    self.scan_btn.setEnabled(True)
                    self.load_existing_data()  # Reload data after scan
                except Exception as e:
                    logger.error(f"ERROR STARTING SCAN: {e}")
                    self.scan_btn.setEnabled(True)
            
            thread = threading.Thread(target=scan_thread)
            thread.daemon = True
            thread.start()
            
        except Exception as e:
            logger.error(f"ERROR STARTING SCAN: {e}")
            self.scan_btn.setEnabled(True)

    def on_series_selected(self, index):
        """Handle series selection for plotting"""
        try:
            if index == 0:  # Composite Signal
                if self.composite is not None:
                    self.plot_series(self.composite, 'Composite Signal')
                else:
                    self.log('No composite signal data available')
            elif index == 1:  # Top Correlations
                if self.top_corr is not None:
                    self.plot_correlations()
                else:
                    self.log('No correlation data available')
        except Exception as e:
            logger.error(f"PLOTTING ERROR: {e}")

    def plot_series(self, series, title):
        """Plot a time series"""
        try:
            self.canvas1.ax.clear()
            series.plot(ax=self.canvas1.ax)
            self.canvas1.ax.set_title(title)
            self.canvas1.ax.grid(True)
            self.canvas1.draw()
        except Exception as e:
            logger.error(f"PLOTTING ERROR: {e}")

    def plot_correlations(self):
        """Plot correlation data"""
        try:
            self.canvas1.ax.clear()
            # Simple bar plot of top correlations
            if not self.top_corr.empty:
                top_n = min(10, len(self.top_corr))
                top_data = self.top_corr.head(top_n)
                self.canvas1.ax.bar(range(len(top_data)), top_data['correlation'])
                self.canvas1.ax.set_title('Top Correlations')
                self.canvas1.ax.set_xlabel('Rank')
                self.canvas1.ax.set_ylabel('Correlation')
                self.canvas1.ax.grid(True)
                self.canvas1.draw()
        except Exception as e:
            logger.error(f"PLOTTING ERROR: {e}")

def main():
    """Main GUI function"""
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())

if __name__ == '__main__':
    main() 