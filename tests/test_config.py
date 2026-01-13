"""Tests for configuration module."""

import pytest
from src.config import Settings, GEOGRAPHY_LEVELS


class TestSettings:
    """Tests for Settings configuration."""

    def test_default_settings(self):
        """Test default settings values."""
        settings = Settings()
        assert settings.api_base_url == "https://api.data.abs.gov.au"
        assert settings.api_agency_id == "ABS"
        assert settings.api_timeout == 120

    def test_data_directories_exist(self):
        """Test data directory paths are configured."""
        settings = Settings()
        assert settings.data_dir.name == "data"
        assert settings.raw_data_dir.name == "raw"
        assert settings.processed_data_dir.name == "processed"


class TestGeographyLevels:
    """Tests for geography level constants."""

    def test_geography_levels_defined(self):
        """Test that all expected geography levels are defined."""
        expected = ["SA1", "SA2", "SA3", "SA4", "LGA", "STE", "AUS"]
        for level in expected:
            assert level in GEOGRAPHY_LEVELS
