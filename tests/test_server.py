"""Test suite for chat server."""

import pytest


class TestServerCreation:
    """Test server creation."""
    
    @pytest.mark.server
    def test_server_creation(self, mock_chat_server):
        """Test server creation."""
        assert mock_chat_server is not None
        assert mock_chat_server.port == 8000
    
    @pytest.mark.server
    def test_server_config(self, mock_server_config):
        """Test server configuration."""
        assert mock_server_config["host"] == "localhost"
        assert mock_server_config["port"] == 8000
        assert mock_server_config["debug"] is True


class TestServerOperations:
    """Test server operations."""
    
    @pytest.mark.server
    def test_server_start(self, mock_chat_server):
        """Test server start."""
        mock_chat_server.start()
        mock_chat_server.start.assert_called_once()
    
    @pytest.mark.server
    def test_server_stop(self, mock_chat_server):
        """Test server stop."""
        mock_chat_server.stop()
        mock_chat_server.stop.assert_called_once()
