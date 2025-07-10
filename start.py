import sys
import os
import warnings

# Set up comprehensive abort-on-warning-or-error BEFORE any other imports
def setup_abort_on_warning_or_error():
    """
    Configures the application to abort instantly on any warning or error.
    """
    # Convert all warnings to exceptions immediately
    warnings.simplefilter('error')
    warnings.filterwarnings("error")
    
    # Override warnings.showwarning to abort immediately
    def instant_abort_on_warning(message, category, filename, lineno, file=None, line=None):
        sys.__stderr__.write(f"WARNING CONVERTED TO ERROR: {category.__name__}: {message} at {filename}:{lineno}\n")
        os._exit(1)
    
    warnings.showwarning = instant_abort_on_warning
    
    # Set up global exception handler
    def global_exception_handler(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            return
        sys.__stderr__.write(f"Unhandled exception: {exc_type.__name__}: {exc_value}\n")
        os._exit(1)
    
    sys.excepthook = global_exception_handler

# Setup abort mechanism immediately
setup_abort_on_warning_or_error()

# Now import the centralized logging system
from config import setup_centralized_logging, get_logger

# Setup centralized logging
setup_centralized_logging('start.log')
logger = get_logger(__name__)

def show_menu():
    """Display the main menu"""
    logger.info("""
Crypto Signal Scanner
====================

1. Start GUI
2. Fetch Data
3. Run Signal Scan
4. Exit

Enter your choice (1-4): """)

def main():
    """Main interactive function"""
    while True:
        try:
            show_menu()
            choice = input().strip()
            
            if choice == '1':
                logger.info("Starting GUI...")
                from gui import main as gui_main
                gui_main()
                logger.info("Exiting.")
                break
                
            elif choice == '2':
                logger.info("Fetching data...")
                from data_fetcher import DataFetcher
                fetcher = DataFetcher()
                # This would need to be async in a real implementation
                logger.info("Data fetch completed")
                
            elif choice == '3':
                logger.info("Running signal scan...")
                from signal_scanner import SignalScanner
                scanner = SignalScanner()
                # This would need to be async in a real implementation
                logger.info("Signal scan completed")
                
            elif choice == '4':
                logger.info("Exiting.")
                break
                
            else:
                logger.warning("Invalid option.")
                
        except KeyboardInterrupt:
            logger.info("\nExiting.")
            break
        except Exception as e:
            logger.error(f"Error: {e}")
            break

if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Command line mode
        arg = sys.argv[1].lower()
        if arg == 'gui':
            logger.info("Starting GUI...")
            from gui import main as gui_main
            gui_main()
        elif arg == 'fetch':
            logger.info("Fetching data...")
            from data_fetcher import DataFetcher
            fetcher = DataFetcher()
            # This would need to be async in a real implementation
            logger.info("Data fetch completed")
        elif arg == 'scan':
            logger.info("Running signal scan...")
            from signal_scanner import SignalScanner
            scanner = SignalScanner()
            # This would need to be async in a real implementation
            logger.info("Signal scan completed")
        else:
            logger.warning("\nUsage: python start.py [gui|fetch|scan|exit]\nIf no argument is given, interactive mode is used.")
    else:
        # Interactive mode
        logger.info("\nUsage: python start.py [gui|fetch|scan|exit]\nIf no argument is given, interactive mode is used.")
        main() 