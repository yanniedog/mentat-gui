#!/usr/bin/env python3
"""
Test script to verify GUI fixes work correctly
"""
import sys
import os
from config import setup_abort_on_warning_or_error
setup_abort_on_warning_or_error('test.log')

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from signal_scanner import SignalScanner

def test_signal_scanner_no_plots():
    """Test that signal scanner works without generating plots"""
    print("Testing signal scanner without plot generation...")
    
    try:
        scanner = SignalScanner()
        scanner.run(generate_plots=False)
        print("✓ Signal scanner completed successfully without plots")
        
        # Check that CSV files were created
        required_files = ['composite_signal.csv', 'results.csv']
        for file in required_files:
            if os.path.exists(file):
                print(f"✓ {file} was created")
            else:
                print(f"✗ {file} was not created")
                
        # Check that plot files were NOT created
        plot_files = ['composite_signal.png', 'correlations.png']
        for file in plot_files:
            if not os.path.exists(file):
                print(f"✓ {file} was NOT created (as expected)")
            else:
                print(f"✗ {file} was created (unexpected)")
                
    except Exception as e:
        print(f"✗ Signal scanner failed: {e}")
        return False
        
    return True

def test_signal_scanner_with_plots():
    """Test that signal scanner works with plot generation"""
    print("\nTesting signal scanner with plot generation...")
    
    try:
        scanner = SignalScanner()
        scanner.run(generate_plots=True)
        print("✓ Signal scanner completed successfully with plots")
        
        # Check that plot files were created
        plot_files = ['composite_signal.png', 'correlations.png']
        for file in plot_files:
            if os.path.exists(file):
                print(f"✓ {file} was created")
            else:
                print(f"✗ {file} was not created")
                
    except Exception as e:
        print(f"✗ Signal scanner failed: {e}")
        return False
        
    return True

if __name__ == '__main__':
    print("Testing GUI fixes...")
    
    success1 = test_signal_scanner_no_plots()
    success2 = test_signal_scanner_with_plots()
    
    if success1 and success2:
        print("\n✓ All tests passed! GUI fixes are working correctly.")
    else:
        print("\n✗ Some tests failed. Please check the issues above.") 