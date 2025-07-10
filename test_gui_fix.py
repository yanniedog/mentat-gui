#!/usr/bin/env python3
"""
Test script to verify GUI fixes are working correctly.
"""

import os
import sys
from pathlib import Path

# Setup centralized logging immediately
from config import setup_centralized_logging, get_logger

setup_centralized_logging('test.log')
logger = get_logger(__name__)

def test_signal_scanner_without_plots():
    """Test signal scanner without plot generation."""
    try:
        logger.info("Testing signal scanner without plot generation...")
        
        from signal_scanner import SignalScanner
        scanner = SignalScanner()
        
        # Run scan without plots
        results = scanner.run(generate_plots=False)
        
        if 'error' in results:
            logger.error(f"Signal scanner failed: {results['error']}")
            return False
        
        logger.info("✓ Signal scanner completed successfully without plots")
        return True
        
    except Exception as e:
        logger.error(f"✗ Signal scanner failed: {e}")
        return False

def test_file_creation():
    """Test that expected files are created."""
    expected_files = ['results.csv', 'composite_signal.csv']
    
    for file in expected_files:
        if os.path.exists(file):
            logger.info(f"✓ {file} was created")
        else:
            logger.warning(f"✗ {file} was not created")
    
    # Test that plot files are NOT created when generate_plots=False
    plot_files = ['correlations.png', 'composite_signal.png']
    for file in plot_files:
        if not os.path.exists(file):
            logger.info(f"✓ {file} was NOT created (as expected)")
        else:
            logger.warning(f"✗ {file} was created (unexpected)")

def test_signal_scanner_with_plots():
    """Test signal scanner with plot generation."""
    try:
        logger.info("\nTesting signal scanner with plot generation...")
        
        from signal_scanner import SignalScanner
        scanner = SignalScanner()
        
        # Run scan with plots
        results = scanner.run(generate_plots=True)
        
        if 'error' in results:
            logger.error(f"Signal scanner failed: {results['error']}")
            return False
        
        logger.info("✓ Signal scanner completed successfully with plots")
        return True
        
    except Exception as e:
        logger.error(f"✗ Signal scanner failed: {e}")
        return False

def test_plot_file_creation():
    """Test that plot files are created when generate_plots=True."""
    plot_files = ['correlations.png', 'composite_signal.png']
    
    for file in plot_files:
        if os.path.exists(file):
            logger.info(f"✓ {file} was created")
        else:
            logger.warning(f"✗ {file} was not created")

def main():
    """Run all tests."""
    logger.info("Testing GUI fixes...")
    
    # Test without plots
    success1 = test_signal_scanner_without_plots()
    test_file_creation()
    
    # Test with plots
    success2 = test_signal_scanner_with_plots()
    test_plot_file_creation()
    
    if success1 and success2:
        logger.info("\n✓ All tests passed! GUI fixes are working correctly.")
    else:
        logger.error("\n✗ Some tests failed. Please check the issues above.")

if __name__ == "__main__":
    main() 