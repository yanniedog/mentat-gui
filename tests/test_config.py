"""
Tests for configuration system.
"""

import pytest
import os
from pathlib import Path
from unittest.mock import patch

from config import get_settings, Settings


class TestSettings:
    """Test the Settings class."""
    
    def test_default_settings(self):
        """Test default settings values."""
        settings = Settings()
        
        assert settings.data_sources_file == "data_sources.yaml"
        assert settings.db_dir == "database"
        assert settings.log_dir == "logs"
        assert settings.max_lag == 5
        assert settings.top_n == 2
        assert settings.lookback_days == 365
    
    def test_environment_variables(self):
        """Test environment variable overrides."""
        with patch.dict(os.environ, {
            'FRED_API_KEY': 'test_key',
            'MAX_LAG': '10',
            'TOP_N': '5'
        }):
            settings = Settings()
            
            assert settings.fred_api_key == 'test_key'
            assert settings.max_lag == 10
            assert settings.top_n == 5
    
    def test_path_properties(self):
        """Test path property calculations."""
        settings = Settings()
        
        # Mock PROJECT_ROOT for testing
        with patch('config.PROJECT_ROOT', Path('/test/project')):
            assert str(settings.db_path) == '/test/project/database'
            assert str(settings.log_path) == '/test/project/logs'
            assert str(settings.data_sources_path) == '/test/project/data_sources.yaml'


class TestGetSettings:
    """Test the get_settings function."""
    
    def test_singleton_behavior(self):
        """Test that get_settings returns the same instance."""
        settings1 = get_settings()
        settings2 = get_settings()
        
        assert settings1 is settings2
    
    def test_directory_creation(self, tmp_path):
        """Test that directories are created."""
        with patch('config.PROJECT_ROOT', tmp_path):
            settings = get_settings()
            
            # Directories should be created
            assert (tmp_path / settings.db_dir).exists()
            assert (tmp_path / settings.log_dir).exists()
    
    def test_backward_compatibility(self):
        """Test backward compatibility exports."""
        from config import DB_DIR, LOG_DIR, MAX_LAG, TOP_N
        
        # These should be available as module-level variables
        assert DB_DIR is not None
        assert LOG_DIR is not None
        assert MAX_LAG is not None
        assert TOP_N is not None 