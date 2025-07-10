import sys

from data_fetcher import DataFetcher
from signal_scanner import SignalScanner
from config import setup_abort_on_warning_or_error
setup_abort_on_warning_or_error('app.log')

def print_menu():
    print("""
Crypto Signal Scanner
====================
1. Launch GUI
2. Fetch Binance Data
3. Run Signal Scan
4. Exit
""")
    sys.stdout.flush()

def run_option(choice):
    if choice == '1' or choice == 'gui':
        try:
            import gui
            gui.main()
        except AttributeError:
            import subprocess
            subprocess.run([sys.executable, 'gui.py'])
    elif choice == '2' or choice == 'fetch':
        fetcher = DataFetcher()
        fetcher.download()
    elif choice == '3' or choice == 'scan':
        scanner = SignalScanner()
        scanner.run()
    elif choice == '4' or choice == 'exit':
        print("Exiting.")
        return True
    else:
        print("Invalid option.")
    return False

def main():
    # Command-line argument support
    if len(sys.argv) > 1:
        arg = sys.argv[1].lower()
        if run_option(arg):
            return
        print("\nUsage: python start.py [gui|fetch|scan|exit]\nIf no argument is given, interactive mode is used.")
        return
    while True:
        print_menu()
        try:
            choice = input("Select an option [1-4]: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nExiting.")
            return
        if run_option(choice):
            break
        # print() removed to avoid double menu before input

if __name__ == '__main__':
    main() 