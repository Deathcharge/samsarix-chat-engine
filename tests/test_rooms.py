"""Test suite for chat rooms."""

import pytest


class TestRoomCreation:
    """Test room creation."""
    
    @pytest.mark.room
    def test_room_creation(self, mock_chat_room):
        """Test room creation."""
        assert mock_chat_room.id == "room-1"
        assert mock_chat_room.name == "TestRoom"
    
    @pytest.mark.room
    def test_room_users(self, mock_chat_room):
        """Test room users."""
        assert len(mock_chat_room.users) == 2
        assert "user-1" in mock_chat_room.users


class TestRoomConfiguration:
    """Test room configuration."""
    
    @pytest.mark.room
    def test_room_config(self, mock_chat_room_config):
        """Test room configuration."""
        assert mock_chat_room_config["name"] == "TestRoom"
        assert mock_chat_room_config["max_users"] == 100
        assert mock_chat_room_config["is_private"] is False
