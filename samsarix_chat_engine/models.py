# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Public request, response, and event models."""

from __future__ import annotations

import json
import math
import re
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

ROOM_ID_PATTERN = r"^[a-z0-9][a-z0-9_-]{0,63}$"
REACTION_KEY_PATTERN = r"^[a-z0-9][a-z0-9_+\-]{0,29}$"
MESSAGE_METADATA_KEY_PATTERN = r"^[a-z][a-z0-9_.-]{0,63}$"
MESSAGE_METADATA_MAX_KEYS = 20
MESSAGE_METADATA_MAX_BYTES = 4_096
MESSAGE_METADATA_MAX_SAFE_INTEGER = 9_007_199_254_740_991
ATTACHMENT_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"
ATTACHMENT_MEDIA_TYPE_PATTERN = r"^[a-z0-9][a-z0-9!#$&^_.+\-]{0,62}/[a-z0-9][a-z0-9!#$&^_.+\-]{0,62}$"
ATTACHMENT_SHA256_PATTERN = r"^[a-f0-9]{64}$"
MESSAGE_ATTACHMENTS_MAX_COUNT = 5
MESSAGE_ATTACHMENTS_MAX_BYTES = 8_192
READ_STATE_QUERY_MAX_ROOMS = 100

MessageMetadataValue = str | int | float | bool | None
MessageMetadata = dict[str, MessageMetadataValue]


def validate_message_metadata(value: MessageMetadata) -> MessageMetadata:
    """Return stable metadata after enforcing the public cross-language contract."""

    if len(value) > MESSAGE_METADATA_MAX_KEYS:
        raise ValueError(f"metadata must contain at most {MESSAGE_METADATA_MAX_KEYS} keys")
    normalized: MessageMetadata = {}
    for key, item in sorted(value.items()):
        if re.fullmatch(MESSAGE_METADATA_KEY_PATTERN, key) is None:
            raise ValueError("metadata keys must be 1-64 lowercase ASCII key characters")
        if isinstance(item, bool) or item is None or isinstance(item, str):
            normalized[key] = item
        elif isinstance(item, int):
            if abs(item) > MESSAGE_METADATA_MAX_SAFE_INTEGER:
                raise ValueError("metadata integers must be exactly representable by JavaScript")
            normalized[key] = item
        elif isinstance(item, float):
            if not math.isfinite(item):
                raise ValueError("metadata numbers must be finite")
            if item.is_integer() and abs(item) > MESSAGE_METADATA_MAX_SAFE_INTEGER:
                raise ValueError("metadata integers must be exactly representable by JavaScript")
            normalized[key] = item
        else:
            raise ValueError("metadata values must be JSON scalars; arrays and objects are not supported")
    encoded = json.dumps(
        normalized,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    if len(encoded) > MESSAGE_METADATA_MAX_BYTES:
        raise ValueError(f"metadata must not exceed {MESSAGE_METADATA_MAX_BYTES} UTF-8 JSON bytes")
    return normalized


def validate_optional_message_metadata(value: MessageMetadata | None) -> MessageMetadata | None:
    """Validate an update value while preserving omission/null as no change."""

    return None if value is None else validate_message_metadata(value)


class APIModel(BaseModel):
    """Base model with strict, predictable input behavior."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class AttachmentReference(APIModel):
    """A bounded opaque reference to one host-application-owned file."""

    id: str = Field(min_length=1, max_length=128, pattern=ATTACHMENT_ID_PATTERN)
    name: str = Field(min_length=1, max_length=255)
    media_type: str = Field(min_length=3, max_length=127, pattern=ATTACHMENT_MEDIA_TYPE_PATTERN)
    size_bytes: int = Field(ge=0, le=MESSAGE_METADATA_MAX_SAFE_INTEGER)
    sha256: str | None = Field(default=None, pattern=ATTACHMENT_SHA256_PATTERN)

    @field_validator("name")
    @classmethod
    def reject_control_characters(cls, value: str) -> str:
        if any(ord(character) < 32 or ord(character) == 127 for character in value):
            raise ValueError("attachment names must not contain control characters")
        return value


def validate_attachment_references(value: list[AttachmentReference]) -> list[AttachmentReference]:
    """Return ordered attachment references after enforcing their aggregate bounds."""

    if len(value) > MESSAGE_ATTACHMENTS_MAX_COUNT:
        raise ValueError(f"attachments must contain at most {MESSAGE_ATTACHMENTS_MAX_COUNT} items")
    identifiers = [attachment.id for attachment in value]
    if len(identifiers) != len(set(identifiers)):
        raise ValueError("attachment IDs must be unique within a message")
    encoded = json.dumps(
        [attachment.model_dump(mode="json") for attachment in value],
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    if len(encoded) > MESSAGE_ATTACHMENTS_MAX_BYTES:
        raise ValueError(f"attachments must not exceed {MESSAGE_ATTACHMENTS_MAX_BYTES} UTF-8 JSON bytes")
    return value


def validate_read_state_query_room_ids(value: list[str]) -> list[str]:
    """Return a bounded unique ordered set of canonical room identifiers."""

    if not 1 <= len(value) <= READ_STATE_QUERY_MAX_ROOMS:
        raise ValueError(f"room_ids must contain between 1 and {READ_STATE_QUERY_MAX_ROOMS} items")
    if any(re.fullmatch(ROOM_ID_PATTERN, room_id) is None for room_id in value):
        raise ValueError("room_ids must contain valid room IDs")
    if len(value) != len(set(value)):
        raise ValueError("room_ids must not contain duplicates")
    return value


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


class MessageCreate(APIModel):
    """Payload for posting a message over HTTP."""

    content: str = Field(default="", max_length=100_000)
    sender: str | None = Field(default=None, min_length=1, max_length=64)
    client_message_id: str | None = Field(default=None, min_length=1, max_length=128)
    parent_message_id: str | None = Field(default=None, min_length=1, max_length=128)
    metadata: MessageMetadata = Field(default_factory=dict)
    attachments: list[AttachmentReference] = Field(default_factory=list)

    _validate_metadata = field_validator("metadata")(validate_message_metadata)
    _validate_attachments = field_validator("attachments")(validate_attachment_references)

    @model_validator(mode="after")
    def require_content_or_attachment(self) -> MessageCreate:
        if not self.content.strip() and not self.attachments:
            raise ValueError("content or at least one attachment is required")
        return self


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
    metadata: MessageMetadata = Field(default_factory=dict)
    attachments: list[AttachmentReference] = Field(default_factory=list)
    edited_at: datetime | None = None
    deleted_at: datetime | None = None

    _validate_metadata = field_validator("metadata")(validate_message_metadata)
    _validate_attachments = field_validator("attachments")(validate_attachment_references)

    @model_validator(mode="after")
    def require_complete_pin_metadata(self) -> Message:
        if (self.pinned_at is None) != (self.pinned_by is None):
            raise ValueError("pinned_at and pinned_by must both be set or both be null")
        if self.deleted_at is not None and self.pinned_at is not None:
            raise ValueError("deleted messages cannot remain pinned")
        if self.deleted_at is not None and self.metadata:
            raise ValueError("deleted messages cannot retain application metadata")
        if self.deleted_at is not None and self.attachments:
            raise ValueError("deleted messages cannot retain attachment references")
        return self


class MessageUpdate(_MessageContentPayload):
    """Author or administrator message-content update."""

    metadata: MessageMetadata | None = None

    _validate_metadata = field_validator("metadata")(validate_optional_message_metadata)


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


class ReadStateQuery(APIModel):
    """Bounded ordered room set for one signed subject's inbox snapshot."""

    room_ids: list[str] = Field(min_length=1, max_length=READ_STATE_QUERY_MAX_ROOMS)

    @field_validator("room_ids")
    @classmethod
    def validate_room_ids(cls, value: list[str]) -> list[str]:
        return validate_read_state_query_room_ids(value)


class ReadStateSummary(APIModel):
    """One room's cursor, unread count, and content-free latest activity."""

    room_id: str
    last_read_message_id: str | None
    last_read_at: datetime | None
    unread_count: int = Field(ge=0)
    latest_message_id: str | None
    latest_message_at: datetime | None

    @model_validator(mode="after")
    def require_complete_latest_message(self) -> ReadStateSummary:
        if (self.latest_message_id is None) != (self.latest_message_at is None):
            raise ValueError("latest_message_id and latest_message_at must both be set or both be null")
        return self


class ReadStateQueryResult(APIModel):
    """Content-free cross-room inbox state for one signed subject."""

    subject: str
    items: list[ReadStateSummary]
    total_unread_count: int = Field(ge=0)
    unread_room_count: int = Field(ge=0)


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


class WebSocketMessage(APIModel):
    """Client-to-server WebSocket message command."""

    type: Literal["message"]
    content: str = Field(default="", max_length=100_000)
    client_message_id: str | None = Field(default=None, min_length=1, max_length=128)
    parent_message_id: str | None = Field(default=None, min_length=1, max_length=128)
    metadata: MessageMetadata = Field(default_factory=dict)
    attachments: list[AttachmentReference] = Field(default_factory=list)

    _validate_metadata = field_validator("metadata")(validate_message_metadata)
    _validate_attachments = field_validator("attachments")(validate_attachment_references)

    @model_validator(mode="after")
    def require_content_or_attachment(self) -> WebSocketMessage:
        if not self.content.strip() and not self.attachments:
            raise ValueError("content or at least one attachment is required")
        return self


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
