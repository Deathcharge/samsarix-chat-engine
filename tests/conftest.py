"""Comprehensive pytest configuration and fixtures for helix-chat-engine."""

import pytest
from unittest.mock import Mock, MagicMock, patch, AsyncMock
import asyncio


# ============================================================================
# Chat Message Fixtures
# ============================================================================

@pytest.fixture
def mock_chat_message():
    """Mock chat message."""
    return {
        "id": "msg-1",
        "sender": "user-1",
        "content": "Hello",
        "timestamp": 1234567890,
        "room_id": "room-1"
    }


@pytest.fixture
def mock_chat_messages():
    """Mock list of chat messages."""
    return [
        {"id": "msg-1", "sender": "user-1", "content": "Hello", "timestamp": 1234567890},
        {"id": "msg-2", "sender": "user-2", "content": "Hi there", "timestamp": 1234567891},
        {"id": "msg-3", "sender": "user-1", "content": "How are you?", "timestamp": 1234567892}
    ]


# ============================================================================
# Chat Room Fixtures
# ============================================================================

@pytest.fixture
def mock_chat_room():
    """Mock chat room."""
    room = MagicMock()
    room.id = "room-1"
    room.name = "TestRoom"
    room.users = ["user-1", "user-2"]
    room.created_at = 1234567890
    return room


@pytest.fixture
def mock_chat_room_config():
    """Mock chat room configuration."""
    return {
        "name": "TestRoom",
        "description": "Test chat room",
        "max_users": 100,
        "is_private": False
    }


# ============================================================================
# User Fixtures
# ============================================================================

@pytest.fixture
def mock_user():
    """Mock user."""
    user = MagicMock()
    user.id = "user-1"
    user.username = "testuser"
    user.status = "online"
    user.rooms = ["room-1", "room-2"]
    return user


@pytest.fixture
def mock_users():
    """Mock list of users."""
    users = []
    for i in range(3):
        user = MagicMock()
        user.id = f"user-{i}"
        user.username = f"user{i}"
        user.status = "online"
        users.append(user)
    return users


# ============================================================================
# WebSocket Fixtures
# ============================================================================

@pytest.fixture
def mock_websocket():
    """Mock WebSocket connection."""
    ws = AsyncMock()
    ws.send = AsyncMock()
    ws.receive = AsyncMock(return_value={"type": "websocket.receive", "text": '{"type": "message"}'})
    ws.accept = AsyncMock()
    ws.close = AsyncMock()
    return ws


@pytest.fixture
def mock_websocket_manager():
    """Mock WebSocket manager."""
    manager = MagicMock()
    manager.connect = MagicMock()
    manager.disconnect = MagicMock()
    manager.broadcast = AsyncMock()
    manager.send_personal = AsyncMock()
    manager.get_active_connections = MagicMock(return_value=[])
    return manager


# ============================================================================
# Server Fixtures
# ============================================================================

@pytest.fixture
def mock_chat_server():
    """Mock chat server."""
    server = MagicMock()
    server.start = MagicMock()
    server.stop = MagicMock()
    server.is_running = False
    server.port = 8000
    return server


@pytest.fixture
def mock_server_config():
    """Mock server configuration."""
    return {
        "host": "localhost",
        "port": 8000,
        "debug": True,
        "workers": 4
    }


# ============================================================================
# Event Fixtures
# ============================================================================

@pytest.fixture
def mock_chat_event():
    """Mock chat event."""
    return {
        "type": "message",
        "user_id": "user-1",
        "room_id": "room-1",
        "data": {"content": "Hello"},
        "timestamp": 1234567890
    }


@pytest.fixture
def mock_events():
    """Mock list of events."""
    return [
        {"type": "user_joined", "user_id": "user-1", "room_id": "room-1"},
        {"type": "message", "user_id": "user-1", "room_id": "room-1", "data": {"content": "Hi"}},
        {"type": "user_left", "user_id": "user-1", "room_id": "room-1"}
    ]


# ============================================================================
# Connection Fixtures
# ============================================================================

@pytest.fixture
def mock_connection():
    """Mock connection."""
    conn = MagicMock()
    conn.id = "conn-1"
    conn.user_id = "user-1"
    conn.room_id = "room-1"
    conn.connected_at = 1234567890
    conn.is_active = True
    return conn


@pytest.fixture
def mock_connections():
    """Mock list of connections."""
    connections = []
    for i in range(3):
        conn = MagicMock()
        conn.id = f"conn-{i}"
        conn.user_id = f"user-{i}"
        conn.room_id = "room-1"
        conn.is_active = True
        connections.append(conn)
    return connections


# ============================================================================
# Scenario Fixtures
# ============================================================================

@pytest.fixture
def multi_user_chat_scenario():
    """Multi-user chat scenario."""
    return {
        "num_users": 5,
        "num_messages": 50,
        "room_id": "room-1",
        "duration": 300
    }


@pytest.fixture
def websocket_connection_scenario():
    """WebSocket connection scenario."""
    return {
        "num_connections": 10,
        "connection_duration": 60,
        "message_rate": 10,
        "should_succeed": True
    }


@pytest.fixture
def error_recovery_scenario():
    """Error recovery scenario."""
    return {
        "error_type": "connection_lost",
        "retry_count": 3,
        "retry_delay": 5,
        "should_recover": True
    }
