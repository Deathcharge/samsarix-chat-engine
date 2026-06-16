"""Test suite for chat messages."""

import pytest


class TestMessageCreation:
    """Test message creation."""
    
    @pytest.mark.message
    def test_message_creation(self, mock_chat_message):
        """Test message creation."""
        assert mock_chat_message["id"] == "msg-1"
        assert mock_chat_message["sender"] == "user-1"
        assert mock_chat_message["content"] == "Hello"
    
    @pytest.mark.message
    def test_message_timestamp(self, mock_chat_message):
        """Test message timestamp."""
        assert mock_chat_message["timestamp"] > 0


class TestMessageList:
    """Test message list."""
    
    @pytest.mark.message
    def test_message_list(self, mock_chat_messages):
        """Test message list."""
        assert len(mock_chat_messages) == 3
        assert mock_chat_messages[0]["id"] == "msg-1"
    
    @pytest.mark.message
    def test_message_ordering(self, mock_chat_messages):
        """Test message ordering."""
        for i in range(len(mock_chat_messages) - 1):
            assert mock_chat_messages[i]["timestamp"] <= mock_chat_messages[i+1]["timestamp"]
