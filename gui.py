"""
PyQt5 GUI for Crypto Signal Scanner
"""
import sys
import os
import traceback
import logging
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QPushButton,
    QVBoxLayout, QHBoxLayout, QTextEdit, QLabel, QFileDialog, QMessageBox, QComboBox
)
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.backends.backend_qt5agg import NavigationToolbar2QT as NavigationToolbar
import matplotlib.pyplot as plt

from data_fetcher import DataFetcher
from signal_scanner import SignalScanner
from config import LOG_DIR, setup_abort_on_warning_or_error

# Setup INSTANT abort mechanism for GUI
setup_abort_on_warning_or_error('gui.log')

# Setup GUI logging
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, 'app.log'), mode='w'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class WorkerThread(QThread):
    finished = pyqtSignal(object)
    error = pyqtSignal(str)

    def __init__(self, fn, *args, **kwargs):
        super().__init__()
        self.fn = fn
        self.args = args
        self.kwargs = kwargs

    def run(self):
        try:
            result = self.fn(*self.args, **self.kwargs)
            self.finished.emit(result)
        except Exception as e:
            # INSTANTLY abort on any exception in worker thread
            logger.error(f"WORKER THREAD EXCEPTION: {type(e).__name__}: {e}")
            os._exit(1)  # Force immediate exit

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
        
    def on_error(self, err):
        # INSTANTLY abort on any error instead of showing message box
        logger.error(f"GUI ERROR: {err}")
        os._exit(1)  # Force immediate exit

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
                scores = pd.read_csv('results.csv')
                self.top_corr = scores
                self.log('Loaded existing correlation results')
                
                # Clear and repopulate dropdown
                self.series_dropdown.blockSignals(True)
                self.series_dropdown.clear()
                self.series_dropdown.addItem('Composite Signal')
                self.series_dropdown.addItem('Top Correlations')
                
                # Try to load individual series
                for name in scores['series'].unique():
                    try:
                        safe_name = name.replace(' ', '_').replace('/', '_')
                        if os.path.exists(f'{safe_name}.csv'):
                            s = pd.read_csv(f'{safe_name}.csv', index_col=0, parse_dates=True).squeeze()
                            self.series_data[name] = s
                            self.series_dropdown.addItem(name)
                    except Exception as e:
                        # INSTANTLY abort on any error loading data
                        logger.error(f"ERROR LOADING SERIES {name}: {e}")
                        os._exit(1)
                        
                self.series_dropdown.blockSignals(False)
                        
            # Plot if we have data
            if self.composite is not None or self.top_corr is not None:
                self.plot_selected_series()
                
        except Exception as e:
            # INSTANTLY abort on any error loading data
            logger.error(f"ERROR LOADING EXISTING DATA: {e}")
            os._exit(1)

    def start_fetch(self):
        try:
            self.fetch_btn.setEnabled(False)
            self.log('Starting data fetch...')
            self.thread = WorkerThread(DataFetcher().download)
            self.thread.finished.connect(self.on_fetch_done)
            self.thread.error.connect(self.on_error)
            self.thread.start()
        except Exception as e:
            # INSTANTLY abort on any error starting fetch
            logger.error(f"ERROR STARTING FETCH: {e}")
            os._exit(1)

    def on_fetch_done(self, _):
        self.log('Data fetch complete')
        self.fetch_btn.setEnabled(True)

    def start_scan(self):
        try:
            self.scan_btn.setEnabled(False)
            self.log('Starting signal scan...')
            self.thread = WorkerThread(SignalScanner().run, generate_plots=False)
            self.thread.finished.connect(self.on_scan_done)
            self.thread.error.connect(self.on_error)
            self.thread.start()
        except Exception as e:
            # INSTANTLY abort on any error starting scan
            logger.error(f"ERROR STARTING SCAN: {e}")
            os._exit(1)

    def on_scan_done(self, _):
        self.log('Signal scan complete')
        self.scan_btn.setEnabled(True)
        # Reload all data and update plots
        self.load_existing_data()

    def on_series_selected(self, idx):
        self.plot_selected_series()

    def plot_selected_series(self):
        try:
            import pandas as pd
            sel = self.series_dropdown.currentText()
            self.canvas1.ax.clear()
            if sel == 'Composite Signal' and self.composite is not None:
                self.canvas1.ax.plot(self.composite.index, self.composite.values, label='Composite Signal')
                self.canvas1.ax.legend()
            elif sel in self.series_data:
                s = self.series_data[sel]
                self.canvas1.ax.plot(s.index, s.values, label=sel)
                self.canvas1.ax.legend()
            self.canvas1.draw()
            # Always plot top correlations in canvas2
            self.canvas2.ax.clear()
            if self.top_corr is not None:
                self.top_corr.set_index('series')['corr'].plot(kind='bar', ax=self.canvas2.ax)
            self.canvas2.draw()
        except Exception as e:
            # INSTANTLY abort on any plotting error
            logger.error(f"PLOTTING ERROR: {e}")
            os._exit(1)

if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_()) 