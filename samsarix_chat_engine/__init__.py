# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Public API for Samsarix Chat Engine."""

from .app import create_app
from .auth import (
    AccessTokenService,
    AccessTokenVerifier,
    AuthenticationError,
    JWKSAccessTokenVerifier,
    Principal,
    TokenKeySetError,
)
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

__version__ = "0.12.0"

__all__ = [
    "AccessTokenService",
    "AccessTokenVerifier",
    "AuthenticationError",
    "AuditEvent",
    "AuditEventPage",
    "ChatStore",
    "ConfigurationError",
    "ConnectionManager",
    "Message",
    "MessageCreate",
    "MessagePage",
    "JWKSAccessTokenVerifier",
    "Principal",
    "ReadState",
    "ReadStateUpdate",
    "RetentionResult",
    "Room",
    "RoomCreate",
    "RoomUpdate",
    "Settings",
    "TokenKeySetError",
    "WebhookDelivery",
    "WebhookDeliveryPage",
    "__version__",
    "create_app",
]
