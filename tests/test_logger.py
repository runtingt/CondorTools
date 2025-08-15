"""Tests for condor_stat.py module."""

import logging

import pytest

from ..condor_tools.condor_tools import CustomFormatter


class TestCustomFormatter:
    """Test cases for CustomFormatter class."""

    @pytest.fixture
    def formatter(self):
        """Fixture providing a CustomFormatter instance."""
        return CustomFormatter(fmt="%(levelname)s: %(asctime)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    @pytest.fixture
    def log_record(self):
        """Fixture providing a basic LogRecord."""
        return logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test_path",
            lineno=1,
            msg="Test message",
            args=(),
            exc_info=None,
        )

    def test_format_without_simple_attribute(self, formatter, log_record):
        """Test formatting without simple attribute uses default formatter."""
        result = formatter.format(log_record)
        assert "INFO:" in result
        assert "Test message" in result

    def test_format_with_simple_attribute(self, formatter, log_record):
        """Test formatting with simple attribute returns plain message."""
        log_record.simple = True
        result = formatter.format(log_record)
        assert result == "Test message"
        assert "INFO:" not in result
