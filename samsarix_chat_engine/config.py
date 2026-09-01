# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Runtime configuration for Samsarix Chat Engine."""

from __future__ import annotations

import os
import re
import warnings
from base64 import b64decode
from binascii import Error as Base64Error
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Literal
from urllib.parse import parse_qs, urlparse


class ConfigurationError(ValueError):
    """Raised when runtime configuration is unsafe or malformed."""


WEBHOOK_EVENT_TYPES = frozenset(
    {
        "member.moderation.updated",
        "message.created",
        "message.deleted",
        "message.pin.updated",
        "message.reaction.updated",
        "message.updated",
    }
)
_POSTGRES_INSTANCE_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


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


def _read_secret_file(variable: str, path_value: str) -> str:
    if not path_value.strip():
        raise ConfigurationError(f"{variable} must name a readable secret file")
    path = Path(path_value)
    try:
        with path.open("rb") as handle:
            encoded = handle.read(4_098)
    except OSError as exc:
        raise ConfigurationError(f"{variable} must name a readable secret file") from exc
    if len(encoded) > 4_097:
        raise ConfigurationError(f"{variable} secret file must not exceed 4097 bytes")
    try:
        value = encoded.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ConfigurationError(f"{variable} secret file must contain UTF-8 text") from exc
    value = value.removesuffix("\n").removesuffix("\r")
    if not value or "\n" in value or "\r" in value or "\x00" in value:
        raise ConfigurationError(f"{variable} secret file must contain exactly one non-empty text line")
    return value


def _read_secret_env(suffix: str) -> str | None:
    canonical = f"SAMSARIX_CHAT_{suffix}"
    canonical_file = f"{canonical}_FILE"
    legacy = f"HELIX_CHAT_{suffix}"
    legacy_file = f"{legacy}_FILE"
    if canonical in os.environ and canonical_file in os.environ:
        raise ConfigurationError(f"set only one of {canonical} or {canonical_file}")

    canonical_value = os.environ.get(canonical)
    if canonical_file in os.environ:
        canonical_value = _read_secret_file(canonical_file, os.environ[canonical_file])
    if canonical_value is not None:
        if legacy in os.environ or legacy_file in os.environ:
            warnings.warn(
                f"legacy {legacy} configuration is ignored because canonical {canonical} configuration is set",
                FutureWarning,
                stacklevel=3,
            )
        return canonical_value

    if legacy in os.environ and legacy_file in os.environ:
        raise ConfigurationError(f"set only one of {legacy} or {legacy_file}")
    if legacy_file in os.environ:
        warnings.warn(
            f"{legacy_file} is deprecated; use {canonical_file}",
            FutureWarning,
            stacklevel=3,
        )
        return _read_secret_file(legacy_file, os.environ[legacy_file])
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


def _read_bool(suffix: str, default: bool = False) -> bool:
    raw = _read_env(suffix)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ConfigurationError(f"SAMSARIX_CHAT_{suffix} must be true or false")


def _read_origins() -> tuple[str, ...]:
    raw = _read_env("ALLOWED_ORIGINS") or ""
    return tuple(dict.fromkeys(value.strip().rstrip("/") for value in raw.split(",") if value.strip()))


def _read_webhook_events(webhook_url: str | None) -> tuple[str, ...]:
    raw = _read_env("WEBHOOK_EVENTS")
    if raw is None:
        return tuple(sorted(WEBHOOK_EVENT_TYPES)) if webhook_url else ()
    events = tuple(dict.fromkeys(value.strip() for value in raw.split(",") if value.strip()))
    unknown = sorted(set(events) - WEBHOOK_EVENT_TYPES)
    if unknown:
        raise ConfigurationError(f"SAMSARIX_CHAT_WEBHOOK_EVENTS contains unsupported values: {', '.join(unknown)}")
    return events


def decode_webhook_secret(value: str) -> bytes:
    """Decode one Standard Webhooks symmetric secret after strict validation."""

    if not value.startswith("whsec_"):
        raise ConfigurationError("webhook signing secrets must use the whsec_ base64 format")
    try:
        decoded = b64decode(value.removeprefix("whsec_"), validate=True)
    except (Base64Error, ValueError) as exc:
        raise ConfigurationError("webhook signing secrets must contain valid base64") from exc
    if not 24 <= len(decoded) <= 64:
        raise ConfigurationError("webhook signing secrets must decode to between 24 and 64 bytes")
    return decoded


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
    storage_backend: Literal["sqlite", "postgres"] = "sqlite"
    postgres_url: str | None = field(default=None, repr=False)
    postgres_instance_id: str | None = None
    postgres_min_pool_size: int = 1
    postgres_max_pool_size: int = 10
    postgres_pool_timeout_seconds: float = 10.0
    postgres_operation_timeout_seconds: float = 10.0
    postgres_lease_seconds: int = 30
    postgres_relay_poll_seconds: float = 0.25
    postgres_relay_max_pending_events: int = 10_000
    postgres_relay_max_event_age_seconds: int = 30
    postgres_maintenance_interval_seconds: float = 1.0
    postgres_max_rate_buckets: int = 100_000
    postgres_max_realtime_events: int = 100_000
    postgres_realtime_event_max_age_seconds: int = 604_800
    api_key: str | None = None
    token_signing_secret: str | None = None
    token_verification_jwks_path: Path | None = None
    token_issuer: str = "samsarix-chat-engine"  # noqa: S105 - public JWT issuer identifier
    token_audience: str = "samsarix-chat"  # noqa: S105 - public JWT audience identifier
    token_max_lifetime_seconds: int = 86_400
    token_clock_skew_seconds: int = 30
    allowed_origins: tuple[str, ...] = ()
    max_message_chars: int = 4_000
    max_connections: int = 200
    max_connections_per_room: int = 100
    messages_per_minute: int = 60
    searches_per_minute: int = 30
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
    webhook_url: str | None = None
    webhook_signing_secret: str | None = None
    webhook_previous_signing_secret: str | None = None
    webhook_events: tuple[str, ...] = ()
    webhook_timeout_seconds: float = 10.0
    webhook_max_attempts: int = 9
    max_webhook_deliveries: int = 100_000
    webhook_allow_private_targets: bool = False

    def __post_init__(self) -> None:
        if self.storage_backend not in {"sqlite", "postgres"}:
            raise ConfigurationError("SAMSARIX_CHAT_STORAGE must be sqlite or postgres")
        if self.storage_backend == "sqlite":
            postgres_tuning_is_nondefault = (
                self.postgres_min_pool_size != 1
                or self.postgres_max_pool_size != 10
                or self.postgres_pool_timeout_seconds != 10.0
                or self.postgres_operation_timeout_seconds != 10.0
                or self.postgres_lease_seconds != 30
                or self.postgres_relay_poll_seconds != 0.25
                or self.postgres_relay_max_pending_events != 10_000
                or self.postgres_relay_max_event_age_seconds != 30
                or self.postgres_maintenance_interval_seconds != 1.0
                or self.postgres_max_rate_buckets != 100_000
                or self.postgres_max_realtime_events != 100_000
                or self.postgres_realtime_event_max_age_seconds != 604_800
            )
            if self.postgres_url is not None or self.postgres_instance_id is not None or postgres_tuning_is_nondefault:
                raise ConfigurationError("PostgreSQL settings require SAMSARIX_CHAT_STORAGE=postgres")
        else:
            if self.postgres_url is None:
                raise ConfigurationError("SAMSARIX_CHAT_POSTGRES_URL or SAMSARIX_CHAT_POSTGRES_URL_FILE is required")
            if not 1 <= len(self.postgres_url) <= 4_096:
                raise ConfigurationError("PostgreSQL connection information must be between 1 and 4096 characters")
            if self.postgres_instance_id is None or not _POSTGRES_INSTANCE_PATTERN.fullmatch(self.postgres_instance_id):
                raise ConfigurationError(
                    "SAMSARIX_CHAT_POSTGRES_INSTANCE_ID must be 1 to 128 safe identifier characters"
                )
            parsed_postgres = urlparse(self.postgres_url)
            if parsed_postgres.scheme not in {"postgres", "postgresql"} or parsed_postgres.hostname is None:
                raise ConfigurationError("PostgreSQL connection information must be a PostgreSQL URL")
            loopback_postgres = parsed_postgres.hostname.lower() in {"localhost", "127.0.0.1", "::1"}
            ssl_modes = parse_qs(parsed_postgres.query).get("sslmode", [])
            if not loopback_postgres and ssl_modes != ["verify-full"]:
                raise ConfigurationError("remote PostgreSQL URLs must set sslmode=verify-full")
        if not 0 <= self.postgres_min_pool_size <= self.postgres_max_pool_size:
            raise ConfigurationError("PostgreSQL minimum pool size cannot exceed its maximum")
        if self.api_key is not None and not 16 <= len(self.api_key) <= 4_096:
            raise ConfigurationError("SAMSARIX_CHAT_API_KEY must be between 16 and 4096 characters")
        if self.token_signing_secret is not None:
            secret_bytes = len(self.token_signing_secret.encode("utf-8"))
            if not 32 <= secret_bytes <= 4_096:
                raise ConfigurationError("SAMSARIX_CHAT_TOKEN_SIGNING_SECRET must be between 32 and 4096 bytes")
        if self.token_signing_secret is not None and self.token_verification_jwks_path is not None:
            raise ConfigurationError(
                "set only one of SAMSARIX_CHAT_TOKEN_SIGNING_SECRET or SAMSARIX_CHAT_TOKEN_VERIFICATION_JWKS_FILE"
            )
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
            "searches_per_minute": (self.searches_per_minute, 1, 100_000),
            "max_rooms": (self.max_rooms, 1, 1_000_000),
            "max_stored_messages": (self.max_stored_messages, 1, 10_000_000),
            "max_stored_messages_per_room": (self.max_stored_messages_per_room, 1, 1_000_000),
            "max_read_states_per_room": (self.max_read_states_per_room, 1, 1_000_000),
            "max_audit_events": (self.max_audit_events, 100, 10_000_000),
            "typing_events_per_minute": (self.typing_events_per_minute, 1, 100_000),
            "websocket_max_bytes": (self.websocket_max_bytes, 256, 16_777_216),
            "token_max_lifetime_seconds": (self.token_max_lifetime_seconds, 60, 604_800),
            "token_clock_skew_seconds": (self.token_clock_skew_seconds, 0, 300),
            "webhook_max_attempts": (self.webhook_max_attempts, 1, 20),
            "max_webhook_deliveries": (self.max_webhook_deliveries, 100, 10_000_000),
            "postgres_max_pool_size": (self.postgres_max_pool_size, 1, 100),
            "postgres_lease_seconds": (self.postgres_lease_seconds, 3, 300),
            "postgres_max_rate_buckets": (self.postgres_max_rate_buckets, 1, 10_000_000),
            "postgres_max_realtime_events": (self.postgres_max_realtime_events, 1, 10_000_000),
            "postgres_relay_max_pending_events": (self.postgres_relay_max_pending_events, 1, 100_000),
            "postgres_relay_max_event_age_seconds": (self.postgres_relay_max_event_age_seconds, 1, 3_600),
            "postgres_realtime_event_max_age_seconds": (
                self.postgres_realtime_event_max_age_seconds,
                60,
                31_536_000,
            ),
        }
        for name, (value, minimum, maximum) in checks.items():
            if (
                name in {"postgres_relay_max_pending_events", "postgres_relay_max_event_age_seconds"}
                and type(value) is not int
            ):
                raise ConfigurationError(f"{name} must be an integer")
            if not minimum <= value <= maximum:
                raise ConfigurationError(f"{name} must be between {minimum} and {maximum}")
        if self.token_verification_jwks_path is not None:
            from .auth import JWKSAccessTokenVerifier, TokenKeySetError

            try:
                JWKSAccessTokenVerifier.from_file(
                    self.token_verification_jwks_path,
                    issuer=self.token_issuer,
                    audience=self.token_audience,
                    max_lifetime_seconds=self.token_max_lifetime_seconds,
                    clock_skew_seconds=self.token_clock_skew_seconds,
                )
            except TokenKeySetError as exc:
                raise ConfigurationError(str(exc)) from exc
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
        if not 0.1 <= self.webhook_timeout_seconds <= 30:
            raise ConfigurationError("webhook_timeout_seconds must be between 0.1 and 30")
        if not 0.1 <= self.postgres_pool_timeout_seconds <= 60:
            raise ConfigurationError("postgres_pool_timeout_seconds must be between 0.1 and 60")
        if not 0.1 <= self.postgres_operation_timeout_seconds <= 300:
            raise ConfigurationError("postgres_operation_timeout_seconds must be between 0.1 and 300")
        if not 0.01 <= self.postgres_relay_poll_seconds <= 5:
            raise ConfigurationError("postgres_relay_poll_seconds must be between 0.01 and 5")
        if not 0.1 <= self.postgres_maintenance_interval_seconds <= 60:
            raise ConfigurationError("postgres_maintenance_interval_seconds must be between 0.1 and 60")
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
        webhook_values = {
            "SAMSARIX_CHAT_WEBHOOK_SIGNING_SECRET": self.webhook_signing_secret,
            "SAMSARIX_CHAT_WEBHOOK_PREVIOUS_SIGNING_SECRET": self.webhook_previous_signing_secret,
        }
        if self.webhook_url is None:
            if any(webhook_values.values()) or self.webhook_events:
                raise ConfigurationError("webhook secrets and events require SAMSARIX_CHAT_WEBHOOK_URL")
        else:
            if self.webhook_signing_secret is None:
                raise ConfigurationError("SAMSARIX_CHAT_WEBHOOK_SIGNING_SECRET is required with a webhook URL")
            parsed = urlparse(self.webhook_url)
            try:
                hostname = parsed.hostname
                _ = parsed.port
            except ValueError as exc:
                raise ConfigurationError("webhook URL contains an invalid host or port") from exc
            loopback = hostname is not None and hostname.lower() in {"localhost", "127.0.0.1", "::1"}
            if (
                parsed.scheme not in {"http", "https"}
                or not parsed.netloc
                or hostname is None
                or parsed.username is not None
                or parsed.password is not None
                or parsed.query
                or parsed.fragment
                or (parsed.scheme == "http" and not loopback)
            ):
                raise ConfigurationError(
                    "webhook URL must be HTTPS without credentials, query, or fragments; "
                    "HTTP is allowed only on loopback"
                )
            unknown = sorted(set(self.webhook_events) - WEBHOOK_EVENT_TYPES)
            if not self.webhook_events or unknown:
                raise ConfigurationError("webhook_events must contain at least one supported event type")
            primary = decode_webhook_secret(self.webhook_signing_secret)
            if self.webhook_previous_signing_secret is not None:
                previous = decode_webhook_secret(self.webhook_previous_signing_secret)
                if primary == previous:
                    raise ConfigurationError("current and previous webhook signing secrets must differ")

    @classmethod
    def from_env(cls) -> Settings:
        """Load settings from ``SAMSARIX_CHAT_*`` variables and legacy aliases."""

        api_key = _read_secret_env("API_KEY") or None
        token_signing_secret = _read_secret_env("TOKEN_SIGNING_SECRET") or None
        token_verification_jwks_file = _read_env("TOKEN_VERIFICATION_JWKS_FILE")
        webhook_url = _read_env("WEBHOOK_URL") or None
        configured_database = _read_env("DATABASE")
        storage_backend = _read_env("STORAGE") or "sqlite"
        postgres_url = _read_secret_env("POSTGRES_URL") or None
        postgres_operational_settings = (
            "POSTGRES_INSTANCE_ID",
            "POSTGRES_MIN_POOL_SIZE",
            "POSTGRES_MAX_POOL_SIZE",
            "POSTGRES_POOL_TIMEOUT",
            "POSTGRES_LEASE_SECONDS",
            "POSTGRES_RELAY_POLL",
            "POSTGRES_RELAY_MAX_PENDING_EVENTS",
            "POSTGRES_RELAY_MAX_EVENT_AGE",
            "POSTGRES_MAINTENANCE_INTERVAL",
            "POSTGRES_MAX_RATE_BUCKETS",
            "POSTGRES_MAX_REALTIME_EVENTS",
            "POSTGRES_REALTIME_EVENT_MAX_AGE",
        )
        if storage_backend != "postgres" and (
            postgres_url is not None or any(_read_env(name) is not None for name in postgres_operational_settings)
        ):
            raise ConfigurationError("PostgreSQL settings require SAMSARIX_CHAT_STORAGE=postgres")
        if storage_backend == "postgres" and configured_database is not None:
            raise ConfigurationError("SAMSARIX_CHAT_DATABASE cannot be combined with PostgreSQL storage")
        return cls(
            database_path=Path(configured_database) if configured_database else _default_database_path(),
            storage_backend=storage_backend,  # type: ignore[arg-type]
            postgres_url=postgres_url,
            postgres_instance_id=_read_env("POSTGRES_INSTANCE_ID") or None,
            postgres_min_pool_size=_read_int("POSTGRES_MIN_POOL_SIZE", 1, minimum=0, maximum=100),
            postgres_max_pool_size=_read_int("POSTGRES_MAX_POOL_SIZE", 10, minimum=1, maximum=100),
            postgres_pool_timeout_seconds=_read_float("POSTGRES_POOL_TIMEOUT", 10.0, minimum=0.1, maximum=60),
            postgres_operation_timeout_seconds=_read_float(
                "POSTGRES_OPERATION_TIMEOUT", 10.0, minimum=0.1, maximum=300
            ),
            postgres_lease_seconds=_read_int("POSTGRES_LEASE_SECONDS", 30, minimum=3, maximum=300),
            postgres_relay_poll_seconds=_read_float("POSTGRES_RELAY_POLL", 0.25, minimum=0.01, maximum=5),
            postgres_relay_max_pending_events=_read_int(
                "POSTGRES_RELAY_MAX_PENDING_EVENTS", 10_000, minimum=1, maximum=100_000
            ),
            postgres_relay_max_event_age_seconds=_read_int(
                "POSTGRES_RELAY_MAX_EVENT_AGE", 30, minimum=1, maximum=3_600
            ),
            postgres_maintenance_interval_seconds=_read_float(
                "POSTGRES_MAINTENANCE_INTERVAL", 1.0, minimum=0.1, maximum=60
            ),
            postgres_max_rate_buckets=_read_int("POSTGRES_MAX_RATE_BUCKETS", 100_000, minimum=1, maximum=10_000_000),
            postgres_max_realtime_events=_read_int(
                "POSTGRES_MAX_REALTIME_EVENTS", 100_000, minimum=1, maximum=10_000_000
            ),
            postgres_realtime_event_max_age_seconds=_read_int(
                "POSTGRES_REALTIME_EVENT_MAX_AGE", 604_800, minimum=60, maximum=31_536_000
            ),
            api_key=api_key,
            token_signing_secret=token_signing_secret,
            token_verification_jwks_path=(Path(token_verification_jwks_file) if token_verification_jwks_file else None),
            token_issuer=_read_env("TOKEN_ISSUER") or "samsarix-chat-engine",
            token_audience=_read_env("TOKEN_AUDIENCE") or "samsarix-chat",
            token_max_lifetime_seconds=_read_int("TOKEN_MAX_LIFETIME", 86_400, minimum=60, maximum=604_800),
            token_clock_skew_seconds=_read_int("TOKEN_CLOCK_SKEW", 30, minimum=0, maximum=300),
            allowed_origins=_read_origins(),
            max_message_chars=_read_int("MAX_MESSAGE_CHARS", 4_000, minimum=1, maximum=100_000),
            max_connections=_read_int("MAX_CONNECTIONS", 200, minimum=1, maximum=100_000),
            max_connections_per_room=_read_int("MAX_CONNECTIONS_PER_ROOM", 100, minimum=1, maximum=100_000),
            messages_per_minute=_read_int("MESSAGES_PER_MINUTE", 60, minimum=1, maximum=100_000),
            searches_per_minute=_read_int("SEARCHES_PER_MINUTE", 30, minimum=1, maximum=100_000),
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
            webhook_url=webhook_url,
            webhook_signing_secret=_read_secret_env("WEBHOOK_SIGNING_SECRET") or None,
            webhook_previous_signing_secret=_read_secret_env("WEBHOOK_PREVIOUS_SIGNING_SECRET") or None,
            webhook_events=_read_webhook_events(webhook_url),
            webhook_timeout_seconds=_read_float("WEBHOOK_TIMEOUT", 10.0, minimum=0.1, maximum=30),
            webhook_max_attempts=_read_int("WEBHOOK_MAX_ATTEMPTS", 9, minimum=1, maximum=20),
            max_webhook_deliveries=_read_int("MAX_WEBHOOK_DELIVERIES", 100_000, minimum=100, maximum=10_000_000),
            webhook_allow_private_targets=_read_bool("WEBHOOK_ALLOW_PRIVATE_TARGETS"),
        )

    def with_database_path(self, database_path: Path) -> Settings:
        """Return a validated copy with a CLI-selected database path."""

        if self.storage_backend != "sqlite":
            raise ConfigurationError("--database cannot be combined with PostgreSQL storage")
        return replace(self, database_path=database_path)
