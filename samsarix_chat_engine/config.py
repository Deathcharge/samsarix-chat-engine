# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Runtime configuration for Samsarix Chat Engine."""

from __future__ import annotations

import os
import warnings
from dataclasses import dataclass, replace
from pathlib import Path
from urllib.parse import urlparse


class ConfigurationError(ValueError):
    """Raised when runtime configuration is unsafe or malformed."""


def _read_env(suffix: str) -> str | None:
    canonical = f"SAMSARIX_CHAT_{suffix}"
    legacy = f"HELIX_CHAT_{suffix}"
    if canonical in os.environ:
        if legacy in os.environ:
            warnings.warn(
                f"{legacy} is ignored because {canonical} is set",
                FutureWarning,
                stacklevel=3,
            )
        return os.environ[canonical]
    if legacy in os.environ:
        warnings.warn(
            f"{legacy} is deprecated; use {canonical}",
            FutureWarning,
            stacklevel=3,
        )
        return os.environ[legacy]
    return None


def _read_int(suffix: str, default: int, *, minimum: int, maximum: int) -> int:
    name = f"SAMSARIX_CHAT_{suffix}"
    raw = _read_env(suffix)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be an integer") from exc
    if not minimum <= value <= maximum:
        raise ConfigurationError(f"{name} must be between {minimum} and {maximum}")
    return value


def _read_optional_int(suffix: str, *, minimum: int, maximum: int) -> int | None:
    raw = _read_env(suffix)
    if raw is None or not raw.strip():
        return None
    name = f"SAMSARIX_CHAT_{suffix}"
    try:
        value = int(raw)
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be an integer") from exc
    if not minimum <= value <= maximum:
        raise ConfigurationError(f"{name} must be between {minimum} and {maximum}")
    return value


def _read_float(suffix: str, default: float, *, minimum: float, maximum: float) -> float:
    name = f"SAMSARIX_CHAT_{suffix}"
    raw = _read_env(suffix)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be a number") from exc
    if not minimum <= value <= maximum:
        raise ConfigurationError(f"{name} must be between {minimum} and {maximum}")
    return value


def _read_origins() -> tuple[str, ...]:
    raw = _read_env("ALLOWED_ORIGINS") or ""
    return tuple(dict.fromkeys(value.strip().rstrip("/") for value in raw.split(",") if value.strip()))


def _default_database_path() -> Path:
    canonical = Path("data/samsarix-chat.db")
    legacy = Path("data/helix-chat.db")
    if not canonical.exists() and legacy.exists():
        warnings.warn(
            f"Using legacy database {legacy}; move it to {canonical} or set SAMSARIX_CHAT_DATABASE",
            FutureWarning,
            stacklevel=3,
        )
        return legacy
    return canonical


@dataclass(frozen=True, slots=True)
class Settings:
    """Validated service settings.

    Defaults deliberately favor a single-instance service bound to loopback. The
    CLI prevents an unauthenticated instance from binding to a public interface
    unless the operator supplies an explicit override.
    """

    database_path: Path = Path("data/samsarix-chat.db")
    api_key: str | None = None
    token_signing_secret: str | None = None
    token_issuer: str = "samsarix-chat-engine"  # noqa: S105 - public JWT issuer identifier
    token_audience: str = "samsarix-chat"  # noqa: S105 - public JWT audience identifier
    token_max_lifetime_seconds: int = 86_400
    token_clock_skew_seconds: int = 30
    allowed_origins: tuple[str, ...] = ()
    max_message_chars: int = 4_000
    max_connections: int = 200
    max_connections_per_room: int = 100
    messages_per_minute: int = 60
    max_rooms: int = 1_000
    max_stored_messages: int = 100_000
    max_stored_messages_per_room: int = 10_000
    max_read_states_per_room: int = 10_000
    message_retention_days: int | None = None
    max_audit_events: int = 100_000
    typing_events_per_minute: int = 60
    typing_timeout_seconds: float = 8.0
    websocket_auth_timeout_seconds: float = 5.0
    websocket_send_timeout_seconds: float = 2.0
    websocket_max_bytes: int = 16_384

    def __post_init__(self) -> None:
        if self.api_key is not None and not 16 <= len(self.api_key) <= 4_096:
            raise ConfigurationError("SAMSARIX_CHAT_API_KEY must be between 16 and 4096 characters")
        if self.token_signing_secret is not None:
            secret_bytes = len(self.token_signing_secret.encode("utf-8"))
            if not 32 <= secret_bytes <= 4_096:
                raise ConfigurationError("SAMSARIX_CHAT_TOKEN_SIGNING_SECRET must be between 32 and 4096 bytes")
        for claim_name, claim_value in {
            "token issuer": self.token_issuer,
            "token audience": self.token_audience,
        }.items():
            if not 1 <= len(claim_value) <= 256 or claim_value != claim_value.strip():
                raise ConfigurationError(f"{claim_name} must be 1 to 256 non-whitespace-padded characters")
        checks = {
            "max_message_chars": (self.max_message_chars, 1, 100_000),
            "max_connections": (self.max_connections, 1, 100_000),
            "max_connections_per_room": (self.max_connections_per_room, 1, 100_000),
            "messages_per_minute": (self.messages_per_minute, 1, 100_000),
            "max_rooms": (self.max_rooms, 1, 1_000_000),
            "max_stored_messages": (self.max_stored_messages, 1, 10_000_000),
            "max_stored_messages_per_room": (self.max_stored_messages_per_room, 1, 1_000_000),
            "max_read_states_per_room": (self.max_read_states_per_room, 1, 1_000_000),
            "max_audit_events": (self.max_audit_events, 100, 10_000_000),
            "typing_events_per_minute": (self.typing_events_per_minute, 1, 100_000),
            "websocket_max_bytes": (self.websocket_max_bytes, 256, 16_777_216),
            "token_max_lifetime_seconds": (self.token_max_lifetime_seconds, 60, 604_800),
            "token_clock_skew_seconds": (self.token_clock_skew_seconds, 0, 300),
        }
        for name, (value, minimum, maximum) in checks.items():
            if not minimum <= value <= maximum:
                raise ConfigurationError(f"{name} must be between {minimum} and {maximum}")
        if self.max_connections_per_room > self.max_connections:
            raise ConfigurationError("max_connections_per_room cannot exceed max_connections")
        if self.max_stored_messages_per_room > self.max_stored_messages:
            raise ConfigurationError("max_stored_messages_per_room cannot exceed max_stored_messages")
        if self.message_retention_days is not None and not 1 <= self.message_retention_days <= 3_650:
            raise ConfigurationError("message_retention_days must be between 1 and 3650")
        if not 0.1 <= self.websocket_auth_timeout_seconds <= 60:
            raise ConfigurationError("websocket_auth_timeout_seconds must be between 0.1 and 60")
        if not 0.1 <= self.websocket_send_timeout_seconds <= 60:
            raise ConfigurationError("websocket_send_timeout_seconds must be between 0.1 and 60")
        if not 1 <= self.typing_timeout_seconds <= 30:
            raise ConfigurationError("typing_timeout_seconds must be between 1 and 30")
        for origin in self.allowed_origins:
            parsed = urlparse(origin)
            if (
                parsed.scheme not in {"http", "https"}
                or not parsed.netloc
                or parsed.username is not None
                or parsed.password is not None
                or parsed.path not in {"", "/"}
                or parsed.params
                or parsed.query
                or parsed.fragment
                or origin.endswith("/")
            ):
                raise ConfigurationError(
                    "allowed origins must be exact http(s) origins without credentials, "
                    "paths, query strings, or trailing slashes"
                )

    @classmethod
    def from_env(cls) -> Settings:
        """Load settings from ``SAMSARIX_CHAT_*`` variables and legacy aliases."""

        api_key = _read_env("API_KEY") or None
        token_signing_secret = _read_env("TOKEN_SIGNING_SECRET") or None
        configured_database = _read_env("DATABASE")
        return cls(
            database_path=Path(configured_database) if configured_database else _default_database_path(),
            api_key=api_key,
            token_signing_secret=token_signing_secret,
            token_issuer=_read_env("TOKEN_ISSUER") or "samsarix-chat-engine",
            token_audience=_read_env("TOKEN_AUDIENCE") or "samsarix-chat",
            token_max_lifetime_seconds=_read_int("TOKEN_MAX_LIFETIME", 86_400, minimum=60, maximum=604_800),
            token_clock_skew_seconds=_read_int("TOKEN_CLOCK_SKEW", 30, minimum=0, maximum=300),
            allowed_origins=_read_origins(),
            max_message_chars=_read_int("MAX_MESSAGE_CHARS", 4_000, minimum=1, maximum=100_000),
            max_connections=_read_int("MAX_CONNECTIONS", 200, minimum=1, maximum=100_000),
            max_connections_per_room=_read_int("MAX_CONNECTIONS_PER_ROOM", 100, minimum=1, maximum=100_000),
            messages_per_minute=_read_int("MESSAGES_PER_MINUTE", 60, minimum=1, maximum=100_000),
            max_rooms=_read_int("MAX_ROOMS", 1_000, minimum=1, maximum=1_000_000),
            max_stored_messages=_read_int("MAX_STORED_MESSAGES", 100_000, minimum=1, maximum=10_000_000),
            max_stored_messages_per_room=_read_int(
                "MAX_STORED_MESSAGES_PER_ROOM", 10_000, minimum=1, maximum=1_000_000
            ),
            max_read_states_per_room=_read_int("MAX_READ_STATES_PER_ROOM", 10_000, minimum=1, maximum=1_000_000),
            message_retention_days=_read_optional_int("MESSAGE_RETENTION_DAYS", minimum=1, maximum=3_650),
            max_audit_events=_read_int("MAX_AUDIT_EVENTS", 100_000, minimum=100, maximum=10_000_000),
            typing_events_per_minute=_read_int("TYPING_EVENTS_PER_MINUTE", 60, minimum=1, maximum=100_000),
            typing_timeout_seconds=_read_float("TYPING_TIMEOUT", 8.0, minimum=1, maximum=30),
            websocket_auth_timeout_seconds=_read_float("WS_AUTH_TIMEOUT", 5.0, minimum=0.1, maximum=60),
            websocket_send_timeout_seconds=_read_float("WS_SEND_TIMEOUT", 2.0, minimum=0.1, maximum=60),
            websocket_max_bytes=_read_int("WS_MAX_BYTES", 16_384, minimum=256, maximum=16_777_216),
        )

    def with_database_path(self, database_path: Path) -> Settings:
        """Return a validated copy with a CLI-selected database path."""

        return replace(self, database_path=database_path)
