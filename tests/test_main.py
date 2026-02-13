"""Tests for main CLI module."""

import sys
from pathlib import Path

# Add parent directory to path to import main
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import main  # noqa: E402


def test_main_module_imports():
    """Test that main module and its key functions can be imported."""
    assert hasattr(main, 'main')
    assert hasattr(main, 'cmd_health')
    assert hasattr(main, 'cmd_pull')
    assert hasattr(main, 'cmd_push')
    assert hasattr(main, 'cmd_sync')
    assert hasattr(main, 'cmd_schedule')


def test_main_has_logger():
    """Test that main module has logger configured."""
    assert hasattr(main, 'logger')
    assert main.logger is not None


def test_main_has_signal_handlers():
    """Test that signal handlers are defined."""
    assert hasattr(main, '_handle_signal')
    assert hasattr(main, '_shutdown')
