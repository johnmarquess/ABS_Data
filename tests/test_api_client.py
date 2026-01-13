"""Tests for the ABS API client."""

import pytest
from unittest.mock import Mock, patch
from src.api_client import ABSClient
from src.config import Settings


class TestABSClient:
    """Tests for ABSClient."""

    def test_init_default_settings(self):
        """Test client initializes with default settings."""
        client = ABSClient()
        assert client.base_url == "https://api.data.abs.gov.au"
        assert client.agency_id == "ABS"

    def test_init_custom_settings(self):
        """Test client initializes with custom settings."""
        settings = Settings(api_timeout=60)
        client = ABSClient(settings)
        assert client.timeout == 60

    def test_build_url(self):
        """Test URL building from parts."""
        client = ABSClient()
        url = client._build_url("data", "ABS,CPI", "all")
        assert url == "https://api.data.abs.gov.au/data/ABS,CPI/all"

    def test_build_url_strips_slashes(self):
        """Test URL building strips extra slashes."""
        client = ABSClient()
        url = client._build_url("/data/", "/ABS,CPI/", "/all/")
        assert url == "https://api.data.abs.gov.au/data/ABS,CPI/all"


class TestABSClientDataflows:
    """Tests for dataflow-related methods."""

    def test_parse_dataflows(self):
        """Test parsing dataflow JSON response."""
        client = ABSClient()
        mock_response = {
            "data": {
                "dataflows": [
                    {
                        "id": "TEST_DF",
                        "name": "Test Dataflow",
                        "version": "1.0.0",
                        "agencyID": "ABS",
                    }
                ]
            }
        }

        df = client._parse_dataflows(mock_response)
        assert len(df) == 1
        assert df.iloc[0]["id"] == "TEST_DF"
        assert df.iloc[0]["name"] == "Test Dataflow"

    def test_parse_codelist(self):
        """Test parsing codelist JSON response."""
        client = ABSClient()
        mock_response = {
            "data": {
                "codelists": [
                    {
                        "id": "CL_TEST",
                        "codes": [
                            {"id": "1", "name": "Code One"},
                            {"id": "2", "name": "Code Two"},
                        ],
                    }
                ]
            }
        }

        df = client._parse_codelist(mock_response)
        assert len(df) == 2
        assert df.iloc[0]["code"] == "1"
        assert df.iloc[0]["name"] == "Code One"
