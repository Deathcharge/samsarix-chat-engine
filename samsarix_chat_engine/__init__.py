# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Public API for Samsarix Chat Engine."""

from .app import create_app
from .auth import AccessTokenService, AuthenticationError, Principal
from .config import ConfigurationError, Settings
from .models import Message, MessageCreate, MessagePage, Room, RoomCreate
from .store import ChatStore
from .websocket_manager import ConnectionManager

__version__ = "0.4.0"

__all__ = [
    "AccessTokenService",
    "AuthenticationError",
    "ChatStore",
    "ConfigurationError",
    "ConnectionManager",
    "Message",
    "MessageCreate",
    "MessagePage",
    "Principal",
    "Room",
    "RoomCreate",
    "Settings",
    "__version__",
    "create_app",
]
