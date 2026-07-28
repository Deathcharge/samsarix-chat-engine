"""Public API for Helix Chat Engine."""

from .app import create_app
from .config import ConfigurationError, Settings
from .models import Message, MessageCreate, MessagePage, Room, RoomCreate
from .store import ChatStore
from .websocket_manager import ConnectionManager

__version__ = "0.2.0"

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
