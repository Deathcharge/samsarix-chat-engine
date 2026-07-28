# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Public API for Samsarix Chat Engine."""

from .app import create_app
from .config import ConfigurationError, Settings
from .models import Message, MessageCreate, MessagePage, Room, RoomCreate
from .store import ChatStore
from .websocket_manager import ConnectionManager

__version__ = "0.3.0"

__all__ = [
    "ChatStore",
    "ConfigurationError",
    "ConnectionManager",
    "Message",
    "MessageCreate",
    "MessagePage",
    "Room",
    "RoomCreate",
    "Settings",
    "__version__",
    "create_app",
]
