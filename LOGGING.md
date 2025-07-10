# Centralized Logging System

This document describes the centralized logging system implemented in the mentat-gui project, which ensures that ALL output is captured in log files and processes cease immediately upon any error or warning.

## Overview

The centralized logging system provides:

1. **Complete Output Capture**: All stdout, stderr, and print statements are captured and logged
2. **Immediate Process Termination**: Any error or warning causes instant process termination
3. **Comprehensive Logging**: All output goes to both console and log files
4. **Automatic Error Detection**: Built-in detection of error keywords in print statements

## Architecture

### Core Components

#### CentralizedLogger Class
Located in `config.py`, this class provides the main logging functionality:

- **File Logging**: All output is saved to log files in the `logs/` directory
- **Console Output**: Non-error messages are displayed on console
- **Error Handling**: Errors and warnings are displayed on stderr
- **Output Capture**: All stdout, stderr, and print statements are intercepted and logged

#### Abort Mechanisms
The system includes multiple layers of abort functionality:

1. **Logging Abort**: Any log message with WARNING or ERROR level triggers immediate termination
2. **Warning Conversion**: All Python warnings are converted to errors and trigger termination
3. **Exception Handling**: Uncaught exceptions trigger immediate termination
4. **Signal Handling**: System signals (SIGTERM, SIGINT) trigger immediate termination
5. **Print Monitoring**: Print statements containing error keywords trigger termination

## Usage

### Basic Setup

```python
from config import setup_centralized_logging, get_logger

# Setup logging (call this early in your application)
setup_centralized_logging('app.log')

# Get a logger instance
logger = get_logger(__name__)

# Use the logger
logger.info("Application started")
logger.error("This will cause immediate termination")
```

### Entry Points

The system is automatically set up in all major entry points:

- **CLI**: `__main__.py` - Uses `cli.log`
- **GUI**: `gui.py` - Uses `gui.log`
- **Start Script**: `start.py` - Uses `start.log`
- **Tests**: `test_*.py` - Uses `test.log`

### Logger Functions

```python
# Get a logger for a specific module
logger = get_logger(__name__)

# Log levels (all captured in log file)
logger.debug("Debug information")
logger.info("General information")
logger.warning("Warning - will cause abort")
logger.error("Error - will cause abort")
logger.critical("Critical error - will cause abort")
```

## Abort Behavior

### What Triggers Abort

1. **Logging Levels**: Any log message with WARNING or ERROR level
2. **Python Warnings**: All warnings are converted to errors
3. **Uncaught Exceptions**: Any exception not handled by try/catch
4. **System Signals**: SIGTERM, SIGINT, etc.
5. **Print Statements**: Any print containing error keywords
6. **System Exit**: Non-zero exit codes

### Abort Process

When an abort is triggered:

1. **Log the Abort**: The abort reason is logged to the log file
2. **Display Message**: Abort message is written to stdout and stderr
3. **Immediate Exit**: `os._exit(1)` is called to force immediate termination
4. **No Cleanup**: No cleanup or graceful shutdown is performed

### Error Keywords

Print statements containing these keywords trigger immediate abort:
- `error`
- `warning`
- `exception`
- `traceback`
- `failed`
- `resourcewarning`

## Log Files

### File Structure

Logs are stored in the `logs/` directory with the following structure:

```
logs/
├── app.log          # General application logs
├── cli.log          # Command-line interface logs
├── gui.log          # GUI application logs
├── start.log        # Start script logs
├── test.log         # Test script logs
└── trends_cache/    # Google Trends cache files
```

### Log Format

Each log entry includes:

```
2024-01-15 14:30:45 [INFO] module_name: Log message content
```

Components:
- **Timestamp**: ISO format with seconds
- **Level**: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- **Module**: Source module name
- **Message**: Actual log message

### Output Capture

The system captures and logs:

1. **Log Messages**: All logger calls
2. **Print Statements**: All print() calls (prefixed with "PRINT:")
3. **Stdout**: All sys.stdout.write() calls (prefixed with "STDOUT:")
4. **Stderr**: All sys.stderr.write() calls (prefixed with "STDERR:")

## Testing

### Test Scripts

Use the provided test scripts to verify the logging system:

```bash
# Test normal logging
python test_logging.py normal

# Test error abort
python test_logging.py error

# Test warning abort
python test_logging.py warning

# Test exception abort
python test_logging.py exception
```

### Expected Behavior

- **Normal Test**: Should complete successfully and log messages
- **Error/Warning/Exception Tests**: Should abort immediately with appropriate message

## Configuration

### Environment Variables

The logging system respects these environment variables:

- `LOG_DIR`: Directory for log files (default: "logs")
- `LOG_LEVEL`: Minimum log level (default: "INFO")

### Settings

Logging configuration is managed through the Settings class in `config.py`:

```python
class Settings(BaseSettings):
    log_dir: str = Field(default="logs", alias="LOG_DIR")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
```

## Integration

### With Existing Code

The centralized logging system is designed to be non-intrusive:

1. **Automatic Setup**: Called automatically in entry points
2. **Backward Compatibility**: Existing logger calls work unchanged
3. **Transparent Capture**: All output is captured without code changes

### With External Libraries

The system works with external libraries:

- **Matplotlib**: Plot generation errors are captured and logged
- **Pandas**: Data processing errors are captured and logged
- **HTTP Libraries**: Network errors are captured and logged
- **Database Libraries**: Database errors are captured and logged

## Troubleshooting

### Common Issues

1. **Log Files Not Created**: Check that the `logs/` directory exists and is writable
2. **Premature Aborts**: Check for warnings or errors in external libraries
3. **Missing Output**: Verify that centralized logging is set up before other imports

### Debug Mode

To enable debug logging:

```python
import os
os.environ['LOG_LEVEL'] = 'DEBUG'
```

### Manual Log Inspection

Check log files for issues:

```bash
# View recent logs
tail -f logs/app.log

# Search for errors
grep "ERROR" logs/*.log

# Search for warnings
grep "WARNING" logs/*.log
```

## Best Practices

1. **Early Setup**: Call `setup_centralized_logging()` as early as possible
2. **Use Loggers**: Prefer logger calls over print statements
3. **Handle Exceptions**: Use try/catch blocks to prevent unwanted aborts
4. **Check Logs**: Regularly review log files for issues
5. **Test Abort Behavior**: Verify that errors cause appropriate termination

## Security Considerations

1. **Log File Permissions**: Ensure log files are not world-readable
2. **Sensitive Data**: Avoid logging sensitive information (API keys, passwords)
3. **Log Rotation**: Consider implementing log rotation for long-running applications
4. **Disk Space**: Monitor log file sizes to prevent disk space issues 