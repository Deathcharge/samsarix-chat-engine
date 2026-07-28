"""Public request, response, and event models."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

ROOM_ID_PATTERN = r"^[a-z0-9][a-z0-9_-]{0,63}$"


class APIModel(BaseModel):
    """Base model with strict, predictable input behavior."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class RoomCreate(APIModel):
    """Payload for creating a persisted chat room."""

    name: str = Field(min_length=1, max_length=80)
    description: str = Field(default="", max_length=500)
    id: str | None = Field(default=None, min_length=1, max_length=64, pattern=ROOM_ID_PATTERN)


class Room(APIModel):
    """A persisted chat room."""

    id: str
    name: str
    description: str
    created_at: datetime


class MessageCreate(APIModel):
    """Payload for posting a message over HTTP."""

    sender: str = Field(min_length=1, max_length=64)
    content: str = Field(min_length=1, max_length=100_000)
    client_message_id: str | None = Field(default=None, min_length=1, max_length=128)

    @field_validator("content")
    @classmethod
    def reject_blank_content(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("content must not be blank")
        return value


class Message(APIModel):
    """A persisted chat message."""

    id: str
    room_id: str
    sender: str
    content: str
    created_at: datetime
    client_message_id: str | None = None


class MessagePage(APIModel):
    """Chronological page of messages and an older-page cursor."""

    items: list[Message]
    next_before: str | None


class WebSocketMessage(APIModel):
    """Client-to-server WebSocket message command."""

    type: Literal["message"]
    content: str = Field(min_length=1, max_length=100_000)
    client_message_id: str | None = Field(default=None, min_length=1, max_length=128)

    @field_validator("content")
    @classmethod
    def reject_blank_content(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("content must not be blank")
        return value


class WebSocketPing(APIModel):
    """Client heartbeat command."""

    type: Literal["ping"]


class WebSocketAuth(APIModel):
    """First-message authentication command used by browser clients."""

    type: Literal["auth"]
    api_key: str = Field(min_length=1, max_length=4_096)
