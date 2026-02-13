"""Tests for configuration module."""

import sys
from pathlib import Path

# Add parent directory to path to import scripts
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts import config  # noqa: E402


def test_default_values():
    """Test that default configuration values are set."""
    assert config.TWENTY_API_URL is not None
    assert config.EXCEL_FILE_PATH is not None
    assert config.SYNC_STATE_PATH is not None
    assert config.CONFLICT_STRATEGY in ["crm_wins", "excel_wins", "newest_wins"]


def test_sync_objects_structure():
    """Test that SYNC_OBJECTS has the expected structure."""
    assert isinstance(config.SYNC_OBJECTS, dict)
    assert "companies" in config.SYNC_OBJECTS
    assert "people" in config.SYNC_OBJECTS

    for obj_name, obj_config in config.SYNC_OBJECTS.items():
        assert "sheet_name" in obj_config
        assert "fields" in obj_config
        assert isinstance(obj_config["fields"], list)


def test_numeric_config_types():
    """Test that numeric configuration values have correct types."""
    assert isinstance(config.API_RATE_LIMIT_DELAY, float)
    assert config.API_RATE_LIMIT_DELAY > 0

    assert isinstance(config.BATCH_SIZE, int)
    assert config.BATCH_SIZE > 0

    assert isinstance(config.SYNC_INTERVAL_MINUTES, int)
    assert config.SYNC_INTERVAL_MINUTES > 0


def test_linkedin_config():
    """Test LinkedIn configuration values exist."""
    assert config.LINKEDIN_CLIENT_ID is not None
    assert config.LINKEDIN_CLIENT_SECRET is not None
    assert config.LINKEDIN_REDIRECT_URI is not None
    assert config.LINKEDIN_SCOPE is not None
    assert isinstance(config.LINKEDIN_SNAPSHOT_DOMAINS, list)
