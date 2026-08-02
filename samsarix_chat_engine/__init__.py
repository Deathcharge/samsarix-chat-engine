# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Public API for Samsarix Chat Engine."""

from .app import create_app
from .auth import AccessTokenService, AuthenticationError, Principal
from .config import ConfigurationError, Settings
from .models import (
    AuditEvent,
    AuditEventPage,
    Message,
    MessageCreate,
    MessagePage,
    ReadState,
    ReadStateUpdate,
    RetentionResult,
    Room,
    RoomCreate,
    RoomUpdate,
    WebhookDelivery,
    WebhookDeliveryPage,
)
from .store import ChatStore
from .websocket_manager import ConnectionManager

__version__ = "0.10.0"

__all__ = [
    "AccessTokenService",
    "AuthenticationError",
    "AuditEvent",
    "AuditEventPage",
    "ChatStore",
    "ConfigurationError",
    "ConnectionManager",
    "Message",
    "MessageCreate",
    "MessagePage",
    "Principal",
    "ReadState",
    "ReadStateUpdate",
    "RetentionResult",
    "Room",
    "RoomCreate",
    "RoomUpdate",
    "Settings",
    "WebhookDelivery",
    "WebhookDeliveryPage",
    "__version__",
    "create_app",
]
