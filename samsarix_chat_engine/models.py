# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Public request, response, and event models."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

ROOM_ID_PATTERN = r"^[a-z0-9][a-z0-9_-]{0,63}$"
REACTION_KEY_PATTERN = r"^[a-z0-9][a-z0-9_+\-]{0,29}$"


class APIModel(BaseModel):
    """Base model with strict, predictable input behavior."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class _MessageContentPayload(APIModel):
    """Shared message-content validation for every write transport."""

    content: str = Field(min_length=1, max_length=100_000)

    @field_validator("content")
    @classmethod
    def reject_blank_content(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("content must not be blank")
        return value


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
    archived_at: datetime | None = None
    frozen_at: datetime | None = None


class RoomUpdate(APIModel):
    """Administrative room lifecycle update."""

    archived: bool | None = None
    frozen: bool | None = None

    @model_validator(mode="after")
    def require_lifecycle_change(self) -> RoomUpdate:
        if self.archived is None and self.frozen is None:
            raise ValueError("at least one of archived or frozen is required")
        return self


class MessageCreate(_MessageContentPayload):
    """Payload for posting a message over HTTP."""

    sender: str | None = Field(default=None, min_length=1, max_length=64)
    client_message_id: str | None = Field(default=None, min_length=1, max_length=128)
    parent_message_id: str | None = Field(default=None, min_length=1, max_length=128)


class ReactionSummary(APIModel):
    """A stable reaction key and its current distinct-reactor count."""

    key: str = Field(min_length=1, max_length=30, pattern=REACTION_KEY_PATTERN)
    count: int = Field(ge=1)


class Message(APIModel):
    """A persisted chat message."""

    id: str
    room_id: str
    sender: str
    content: str
    created_at: datetime
    client_message_id: str | None = None
    parent_message_id: str | None = None
    reactions: list[ReactionSummary] = Field(default_factory=list)
    pinned_at: datetime | None = None
    pinned_by: str | None = Field(default=None, min_length=1, max_length=64)
    edited_at: datetime | None = None
    deleted_at: datetime | None = None

    @model_validator(mode="after")
    def require_complete_pin_metadata(self) -> Message:
        if (self.pinned_at is None) != (self.pinned_by is None):
            raise ValueError("pinned_at and pinned_by must both be set or both be null")
        if self.deleted_at is not None and self.pinned_at is not None:
            raise ValueError("deleted messages cannot remain pinned")
        return self


class MessageUpdate(_MessageContentPayload):
    """Author or administrator message-content update."""


class ReactionActor(APIModel):
    """Optional actor identity for operator/local reaction mutations."""

    reactor: str | None = Field(default=None, min_length=1, max_length=64)


class ReactionMutation(APIModel):
    """Result of an idempotent add/remove reaction operation."""

    message: Message
    key: str = Field(min_length=1, max_length=30, pattern=REACTION_KEY_PATTERN)
    reactor: str = Field(min_length=1, max_length=64)
    present: bool
    changed: bool
    updated_at: datetime


class PinActor(APIModel):
    """Optional actor identity for operator/local pin mutations."""

    pinner: str | None = Field(default=None, min_length=1, max_length=64)


class PinMutation(APIModel):
    """Result of an idempotent room-wide pin or unpin operation."""

    message: Message
    pinner: str = Field(min_length=1, max_length=64)
    pinned: bool
    changed: bool
    updated_at: datetime


class MessagePage(APIModel):
    """Chronological page of messages and an older-page cursor."""

    items: list[Message]
    next_before: str | None


class ReadStateUpdate(APIModel):
    """Advance the signed caller's read cursor through a message or the latest room state."""

    message_id: str | None = Field(default=None, min_length=1, max_length=128)


class ReadState(APIModel):
    """One signed subject's monotonic room cursor and derived unread count."""

    room_id: str
    subject: str
    last_read_message_id: str | None
    last_read_at: datetime | None
    unread_count: int


class AuditEvent(APIModel):
    """Administrative event containing metadata but no message body or credential."""

    id: str
    action: str
    actor: str
    room_id: str | None
    created_at: datetime
    details: dict[str, Any] = Field(default_factory=dict)


class AuditEventPage(APIModel):
    """Newest administrative events and an older-page cursor."""

    items: list[AuditEvent]
    next_before: str | None


class WebhookDelivery(APIModel):
    """Persisted delivery metadata; payloads and response bodies are intentionally omitted."""

    id: str
    event_type: str
    room_id: str
    created_at: datetime
    attempt_count: int
    next_attempt_at: datetime | None
    last_attempt_at: datetime | None
    delivered_at: datetime | None
    failed_at: datetime | None
    last_status_code: int | None
    last_error: str | None
    replayable: bool


class WebhookDeliveryPage(APIModel):
    """Newest webhook deliveries and an older-page cursor."""

    items: list[WebhookDelivery]
    next_before: str | None


class RetentionResult(APIModel):
    """Result of an explicit retention pass."""

    deleted_messages: int
    cutoff: datetime


class MemberModerationUpdate(APIModel):
    """Relative mute/ban durations; zero clears the matching control."""

    muted_for_seconds: int | None = Field(default=None, ge=0, le=31_536_000)
    banned_for_seconds: int | None = Field(default=None, ge=0, le=31_536_000)

    @model_validator(mode="after")
    def require_control(self) -> MemberModerationUpdate:
        if self.muted_for_seconds is None and self.banned_for_seconds is None:
            raise ValueError("at least one moderation duration is required")
        return self


class MemberModeration(APIModel):
    """Persisted moderation state for one room subject."""

    room_id: str
    subject: str
    muted_until: datetime | None
    banned_until: datetime | None
    updated_at: datetime


class WebSocketMessage(_MessageContentPayload):
    """Client-to-server WebSocket message command."""

    type: Literal["message"]
    client_message_id: str | None = Field(default=None, min_length=1, max_length=128)
    parent_message_id: str | None = Field(default=None, min_length=1, max_length=128)


class WebSocketPing(APIModel):
    """Client heartbeat command."""

    type: Literal["ping"]


class WebSocketTyping(APIModel):
    """Ephemeral typing state that automatically expires on the server."""

    type: Literal["typing"]
    active: bool


class WebSocketAuth(APIModel):
    """First-message authentication command used by browser clients."""

    type: Literal["auth"]
    token: str | None = Field(default=None, min_length=1, max_length=8_192)
    api_key: str | None = Field(default=None, min_length=1, max_length=4_096)

    @model_validator(mode="after")
    def require_one_credential(self) -> WebSocketAuth:
        if (self.token is None) == (self.api_key is None):
            raise ValueError("exactly one of token or api_key is required")
        return self
