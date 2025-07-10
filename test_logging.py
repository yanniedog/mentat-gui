#!/usr/bin/env python3
"""
Test script to verify centralized logging system.
"""

import sys
import os
from pathlib import Path

# Setup centralized logging immediately
from config import setup_centralized_logging, get_logger

# Setup logging
setup_centralized_logging('test_logging.log')
logger = get_logger(__name__)

def test_normal_logging():
    """Test normal logging functionality."""
    logger.info("This is a normal info message")
    logger.debug("This is a debug message")
    logger.info("Normal logging test completed successfully")

def test_error_abort():
    """Test that errors cause immediate abort."""
    logger.error("This error should cause immediate abort")
    # This line should never be reached
    logger.info("This should not be logged")

def test_warning_abort():
    """Test that warnings cause immediate abort."""
    logger.warning("This warning should cause immediate abort")
    # This line should never be reached
    logger.info("This should not be logged")

def test_exception_abort():
    """Test that exceptions cause immediate abort."""
    raise ValueError("This exception should cause immediate abort")

def main():
    """Run logging tests."""
    if len(sys.argv) > 1:
        test_type = sys.argv[1]
        
        if test_type == "normal":
            test_normal_logging()
        elif test_type == "error":
            test_error_abort()
        elif test_type == "warning":
            test_warning_abort()
        elif test_type == "exception":
            test_exception_abort()
        else:
            logger.error(f"Unknown test type: {test_type}")
    else:
        logger.info("Running normal logging test...")
        test_normal_logging()
        logger.info("All tests completed successfully")

if __name__ == "__main__":
    main() 