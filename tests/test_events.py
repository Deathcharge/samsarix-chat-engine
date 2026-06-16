"""Test suite for chat events."""

import pytest


class TestEventCreation:
    """Test event creation."""
    
    @pytest.mark.event
    def test_event_creation(self, mock_chat_event):
        """Test event creation."""
        assert mock_chat_event["type"] == "message"
        assert mock_chat_event["user_id"] == "user-1"
    
    @pytest.mark.event
    def test_event_data(self, mock_chat_event):
        """Test event data."""
        assert "data" in mock_chat_event
        assert mock_chat_event["data"]["content"] == "Hello"


class TestEventList:
    """Test event list."""
    
    @pytest.mark.event
    def test_event_list(self, mock_events):
        """Test event list."""
        assert len(mock_events) == 3
        assert mock_events[0]["type"] == "user_joined"
    
    @pytest.mark.event
    def test_event_types(self, mock_events):
        """Test event types."""
        types = [e["type"] for e in mock_events]
        assert "user_joined" in types
        assert "message" in types
        assert "user_left" in types
