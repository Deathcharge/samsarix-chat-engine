"""Test suite for WebSocket functionality."""

import pytest


class TestWebSocketConnection:
    """Test WebSocket connection."""
    
    @pytest.mark.websocket
    def test_websocket_creation(self, mock_websocket):
        """Test WebSocket creation."""
        assert mock_websocket is not None
        assert callable(mock_websocket.send)
    
    @pytest.mark.websocket
    def test_websocket_methods(self, mock_websocket):
        """Test WebSocket methods."""
        assert callable(mock_websocket.accept)
        assert callable(mock_websocket.close)


class TestWebSocketManager:
    """Test WebSocket manager."""
    
    @pytest.mark.websocket
    def test_manager_creation(self, mock_websocket_manager):
        """Test manager creation."""
        assert mock_websocket_manager is not None
    
    @pytest.mark.websocket
    def test_manager_methods(self, mock_websocket_manager):
        """Test manager methods."""
        assert callable(mock_websocket_manager.connect)
        assert callable(mock_websocket_manager.disconnect)
        assert callable(mock_websocket_manager.broadcast)
    
    @pytest.mark.websocket
    def test_get_active_connections(self, mock_websocket_manager):
        """Test getting active connections."""
        connections = mock_websocket_manager.get_active_connections()
        assert isinstance(connections, list)
