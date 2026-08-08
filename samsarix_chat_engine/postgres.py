# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Internal PostgreSQL coordination primitives for the v0.13 backend.

This module deliberately is not wired into public configuration yet.  It
establishes the migration and durable realtime contracts that the complete
PostgreSQL chat store will build upon.
"""

from __future__ import annotations

import asyncio
import json
import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any, cast

import psycopg
from psycopg import AsyncConnection
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool, PoolTimeout

POSTGRES_SCHEMA_VERSION = 7
POSTGRES_MIGRATION_LOCK_ID = 7_495_346_927_831_819_041
POSTGRES_EVENT_SEQUENCE_LOCK_ID = 7_495_346_927_831_819_042
REALTIME_CHANNEL = "samsarix_realtime_v1"
MAX_EVENT_PAYLOAD_BYTES = 512 * 1024
MAX_INSTANCE_ID_CHARS = 128
MAX_EVENT_TYPE_CHARS = 80
_INSTANCE_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_EVENT_TYPE_PATTERN = re.compile(r"^[a-z][a-z0-9_.-]{0,79}$")
_ROOM_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")


class PostgresFoundationError(RuntimeError):
    """Base error for PostgreSQL foundation operations."""


class PostgresUnavailableError(PostgresFoundationError):
    """Raised without connection details when PostgreSQL cannot be used."""


class UnsupportedPostgresSchemaError(PostgresFoundationError):
    """Raised when the database schema is newer than this application."""


class InvalidRealtimeEventError(PostgresFoundationError):
    """Raised when a realtime event exceeds its bounded contract."""


class InstanceLeaseError(PostgresFoundationError):
    """Raised when an instance cursor is missing, expired, or invalid."""


@dataclass(frozen=True, slots=True)
class RealtimeEvent:
    """One committed event-log row in global sequence order."""

    sequence: int
    room_id: str
    event_type: str
    payload: dict[str, Any]
    created_at: datetime


class PostgresFoundation:
    """Own PostgreSQL pool, schema lifecycle, event log, and instance cursors."""

    def __init__(
        self,
        conninfo: str,
        *,
        min_pool_size: int = 1,
        max_pool_size: int = 5,
        pool_timeout_seconds: float = 10.0,
    ) -> None:
        if not conninfo.strip():
            raise ValueError("PostgreSQL connection information is required")
        if min_pool_size < 0 or max_pool_size < 1 or min_pool_size > max_pool_size:
            raise ValueError("invalid PostgreSQL pool bounds")
        if pool_timeout_seconds <= 0:
            raise ValueError("PostgreSQL pool timeout must be positive")
        self._pool = AsyncConnectionPool(
            conninfo=conninfo,
            min_size=min_pool_size,
            max_size=max_pool_size,
            timeout=pool_timeout_seconds,
            open=False,
        )
        self._pool_timeout_seconds = pool_timeout_seconds
        self._lifecycle_lock = asyncio.Lock()
        self._opened = False

    async def __aenter__(self) -> PostgresFoundation:
        await self.open()
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.close()

    async def open(self) -> None:
        """Open the pool and initialize the compatible schema exactly once."""

        async with self._lifecycle_lock:
            if self._opened:
                return
            try:
                await self._pool.open(wait=True, timeout=self._pool_timeout_seconds)
                await self._initialize_schema()
            except (psycopg.OperationalError, PoolTimeout, OSError):
                await self._pool.close()
                raise PostgresUnavailableError("PostgreSQL storage is unavailable") from None
            except psycopg.DatabaseError as exc:
                await self._pool.close()
                raise PostgresFoundationError(_safe_database_error("schema initialization", exc)) from None
            except Exception:
                await self._pool.close()
                raise
            self._opened = True

    async def close(self) -> None:
        """Close the connection pool; repeated calls are harmless."""

        async with self._lifecycle_lock:
            await self._pool.close()
            self._opened = False

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncConnection[tuple[Any, ...]]]:
        """Yield a transaction for an authoritative mutation plus event row."""

        self._require_open()
        try:
            async with self._pool.connection() as connection:
                async with connection.transaction():
                    await connection.execute("SET LOCAL search_path = pg_catalog, public")
                    yield connection
        except (psycopg.OperationalError, PoolTimeout, OSError):
            raise PostgresUnavailableError("PostgreSQL storage is unavailable") from None
        except psycopg.DatabaseError as exc:
            raise PostgresFoundationError(_safe_database_error("operation", exc)) from None

    async def schema_version(self) -> int:
        """Return the initialized PostgreSQL schema version."""

        async with self.transaction() as connection:
            cursor = await connection.execute(
                "SELECT version FROM public.samsarix_schema_metadata WHERE singleton = TRUE"
            )
            row = await cursor.fetchone()
        if row is None:
            raise UnsupportedPostgresSchemaError("PostgreSQL schema metadata is missing")
        return int(row[0])

    async def append_event(
        self,
        connection: AsyncConnection[tuple[Any, ...]],
        *,
        room_id: str,
        event_type: str,
        payload: dict[str, Any],
    ) -> int:
        """Insert and notify an event inside the caller's state transaction."""

        normalized_payload = _validate_event(room_id, event_type, payload)
        # Identity values follow allocation order, not commit order. Holding
        # this lock until commit prevents a later sequence from becoming
        # visible and acknowledged while an earlier transaction is pending.
        await connection.execute("SELECT pg_advisory_xact_lock(%s)", (POSTGRES_EVENT_SEQUENCE_LOCK_ID,))
        cursor = await connection.execute(
            """
            INSERT INTO public.samsarix_realtime_events (room_id, event_type, payload)
            VALUES (%s, %s, %s)
            RETURNING sequence
            """,
            (room_id, event_type, Jsonb(normalized_payload)),
        )
        row = await cursor.fetchone()
        if row is None:  # pragma: no cover - PostgreSQL guarantees RETURNING
            raise PostgresFoundationError("PostgreSQL did not return an event sequence")
        sequence = int(row[0])
        await connection.execute("SELECT pg_notify(%s, %s)", (REALTIME_CHANNEL, str(sequence)))
        return sequence

    async def current_head(self) -> int:
        """Return the greatest committed event sequence, or zero."""

        async with self.transaction() as connection:
            cursor = await connection.execute("SELECT COALESCE(MAX(sequence), 0) FROM public.samsarix_realtime_events")
            row = await cursor.fetchone()
        return int(row[0]) if row is not None else 0

    async def register_instance(self, instance_id: str, *, lease_seconds: int) -> int:
        """Create or renew an instance lease and return its durable cursor."""

        _validate_instance(instance_id, lease_seconds)
        async with self.transaction() as connection:
            cursor = await connection.execute(
                """
                INSERT INTO public.samsarix_instance_cursors (
                    instance_id, generation, last_sequence, lease_expires_at, updated_at
                )
                VALUES (
                    %s,
                    gen_random_uuid(),
                    (SELECT COALESCE(MAX(sequence), 0) FROM public.samsarix_realtime_events),
                    clock_timestamp() + make_interval(secs => %s),
                    clock_timestamp()
                )
                ON CONFLICT (instance_id) DO UPDATE SET
                    generation = CASE
                        WHEN samsarix_instance_cursors.lease_expires_at <= clock_timestamp()
                        THEN EXCLUDED.generation
                        ELSE samsarix_instance_cursors.generation
                    END,
                    lease_expires_at = EXCLUDED.lease_expires_at,
                    updated_at = clock_timestamp()
                RETURNING last_sequence
                """,
                (instance_id, lease_seconds),
            )
            row = await cursor.fetchone()
        if row is None:  # pragma: no cover - PostgreSQL guarantees RETURNING
            raise InstanceLeaseError("instance registration failed")
        return int(row[0])

    async def heartbeat_instance(self, instance_id: str, *, lease_seconds: int) -> None:
        """Renew an existing non-expired instance lease."""

        _validate_instance(instance_id, lease_seconds)
        async with self.transaction() as connection:
            cursor = await connection.execute(
                """
                UPDATE public.samsarix_instance_cursors
                SET lease_expires_at = clock_timestamp() + make_interval(secs => %s),
                    updated_at = clock_timestamp()
                WHERE instance_id = %s AND lease_expires_at > clock_timestamp()
                """,
                (lease_seconds, instance_id),
            )
            if cursor.rowcount != 1:
                raise InstanceLeaseError("instance lease is missing or expired")

    async def read_events(self, instance_id: str, *, limit: int = 100) -> list[RealtimeEvent]:
        """Read committed events after an active instance's durable cursor."""

        _validate_instance_id(instance_id)
        if not 1 <= limit <= 1_000:
            raise ValueError("event read limit must be between 1 and 1000")
        async with self.transaction() as connection:
            cursor = await connection.execute(
                """
                SELECT last_sequence
                FROM public.samsarix_instance_cursors
                WHERE instance_id = %s AND lease_expires_at > clock_timestamp()
                FOR UPDATE
                """,
                (instance_id,),
            )
            instance = await cursor.fetchone()
            if instance is None:
                raise InstanceLeaseError("instance lease is missing or expired")
            cursor = await connection.execute(
                """
                SELECT sequence, room_id, event_type, payload, created_at
                FROM public.samsarix_realtime_events
                WHERE sequence > %s
                ORDER BY sequence
                LIMIT %s
                """,
                (int(instance[0]), limit),
            )
            rows = await cursor.fetchall()
        return [
            RealtimeEvent(
                sequence=int(row[0]),
                room_id=str(row[1]),
                event_type=str(row[2]),
                payload=dict(row[3]),
                created_at=row[4],
            )
            for row in rows
        ]

    async def acknowledge_events(self, instance_id: str, *, through_sequence: int) -> int:
        """Advance an active instance cursor monotonically through a real event."""

        _validate_instance_id(instance_id)
        if through_sequence < 0:
            raise ValueError("event sequence cannot be negative")
        async with self.transaction() as connection:
            cursor = await connection.execute(
                """
                SELECT last_sequence
                FROM public.samsarix_instance_cursors
                WHERE instance_id = %s AND lease_expires_at > clock_timestamp()
                FOR UPDATE
                """,
                (instance_id,),
            )
            instance = await cursor.fetchone()
            if instance is None:
                raise InstanceLeaseError("instance lease is missing or expired")
            current = int(instance[0])
            if through_sequence <= current:
                return current
            cursor = await connection.execute("SELECT COALESCE(MAX(sequence), 0) FROM public.samsarix_realtime_events")
            head_row = await cursor.fetchone()
            head = int(head_row[0]) if head_row is not None else 0
            if through_sequence > head:
                raise InstanceLeaseError("instance cursor cannot advance beyond the event head")
            cursor = await connection.execute(
                "SELECT EXISTS (SELECT 1 FROM public.samsarix_realtime_events WHERE sequence = %s)",
                (through_sequence,),
            )
            event_row = await cursor.fetchone()
            if event_row is None or not bool(event_row[0]):
                raise InstanceLeaseError("instance cursor target is not a committed event")
            cursor = await connection.execute(
                """
                UPDATE public.samsarix_instance_cursors
                SET last_sequence = %s,
                    updated_at = clock_timestamp()
                WHERE instance_id = %s
                RETURNING last_sequence
                """,
                (through_sequence, instance_id),
            )
            row = await cursor.fetchone()
            if row is None:
                raise InstanceLeaseError("instance lease is missing or expired")
        return int(row[0])

    async def _initialize_schema(self) -> None:
        async with self._pool.connection() as connection:
            async with connection.transaction():
                await connection.execute("SET LOCAL search_path = pg_catalog, public")
                await connection.execute("SELECT pg_advisory_xact_lock(%s)", (POSTGRES_MIGRATION_LOCK_ID,))
                await connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS public.samsarix_schema_metadata (
                        singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
                        version INTEGER NOT NULL CHECK (version > 0),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
                    )
                    """
                )
                cursor = await connection.execute(
                    "SELECT version FROM public.samsarix_schema_metadata WHERE singleton = TRUE FOR UPDATE"
                )
                row = await cursor.fetchone()
                current_version = int(row[0]) if row is not None else 0
                if current_version > POSTGRES_SCHEMA_VERSION:
                    raise UnsupportedPostgresSchemaError(
                        f"PostgreSQL schema version {current_version} is newer than supported version "
                        f"{POSTGRES_SCHEMA_VERSION}"
                    )
                await connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS public.samsarix_realtime_events (
                        sequence BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        room_id TEXT NOT NULL CHECK (
                            char_length(room_id) BETWEEN 1 AND 64
                            AND room_id ~ '^[a-z0-9][a-z0-9_-]*$'
                        ),
                        event_type TEXT NOT NULL CHECK (
                            char_length(event_type) BETWEEN 1 AND 80
                            AND event_type ~ '^[a-z][a-z0-9_.-]*$'
                        ),
                        payload JSONB NOT NULL CHECK (octet_length(payload::text) <= 524288),
                        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
                    )
                    """
                )
                await connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS public.samsarix_instance_cursors (
                        instance_id TEXT PRIMARY KEY CHECK (
                            char_length(instance_id) BETWEEN 1 AND 128
                            AND instance_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]*$'
                        ),
                        generation UUID NOT NULL DEFAULT gen_random_uuid(),
                        last_sequence BIGINT NOT NULL CHECK (last_sequence >= 0),
                        lease_expires_at TIMESTAMPTZ NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
                    )
                    """
                )
                if current_version < 3:
                    await connection.execute(
                        """
                        ALTER TABLE public.samsarix_realtime_events
                        DROP CONSTRAINT IF EXISTS samsarix_realtime_events_payload_check
                        """
                    )
                    await connection.execute(
                        """
                        ALTER TABLE public.samsarix_realtime_events
                        ADD CONSTRAINT samsarix_realtime_events_payload_check
                        CHECK (octet_length(payload::text) <= 524288)
                        """
                    )
                await connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS public.samsarix_rooms (
                        id TEXT PRIMARY KEY CHECK (
                            char_length(id) BETWEEN 1 AND 64
                            AND id ~ '^[a-z0-9][a-z0-9_-]*$'
                        ),
                        name TEXT NOT NULL CHECK (char_length(name) BETWEEN 1 AND 80),
                        description TEXT NOT NULL DEFAULT '' CHECK (char_length(description) <= 500),
                        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                        archived_at TIMESTAMPTZ,
                        frozen_at TIMESTAMPTZ
                    )
                    """
                )
                await connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS public.samsarix_connection_leases (
                        connection_id TEXT PRIMARY KEY CHECK (
                            char_length(connection_id) BETWEEN 1 AND 128
                            AND connection_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]*$'
                        ),
                        instance_id TEXT NOT NULL REFERENCES public.samsarix_instance_cursors(instance_id)
                            ON DELETE CASCADE,
                        instance_generation UUID NOT NULL,
                        room_id TEXT NOT NULL REFERENCES public.samsarix_rooms(id) ON DELETE CASCADE,
                        username TEXT NOT NULL CHECK (char_length(username) BETWEEN 1 AND 64),
                        subject TEXT CHECK (subject IS NULL OR char_length(subject) BETWEEN 1 AND 64),
                        lease_expires_at TIMESTAMPTZ NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                        CHECK (lease_expires_at > created_at)
                    )
                    """
                )
                await connection.execute(
                    """
                    ALTER TABLE public.samsarix_instance_cursors
                    ADD COLUMN IF NOT EXISTS generation UUID
                    """
                )
                await connection.execute(
                    """
                    UPDATE public.samsarix_instance_cursors
                    SET generation = gen_random_uuid()
                    WHERE generation IS NULL
                    """
                )
                await connection.execute(
                    """
                    ALTER TABLE public.samsarix_instance_cursors
                    ALTER COLUMN generation SET DEFAULT gen_random_uuid(),
                    ALTER COLUMN generation SET NOT NULL
                    """
                )
                await connection.execute(
                    """
                    ALTER TABLE public.samsarix_connection_leases
                    ADD COLUMN IF NOT EXISTS instance_generation UUID
                    """
                )
                await connection.execute(
                    """
                    UPDATE public.samsarix_connection_leases AS lease
                    SET instance_generation = owner.generation
                    FROM public.samsarix_instance_cursors AS owner
                    WHERE owner.instance_id = lease.instance_id
                      AND lease.instance_generation IS NULL
                    """
                )
                await connection.execute(
                    """
                    ALTER TABLE public.samsarix_connection_leases
                    ALTER COLUMN instance_generation SET NOT NULL
                    """
                )
                await connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS samsarix_connection_leases_expiry
                    ON public.samsarix_connection_leases (lease_expires_at, connection_id)
                    """
                )
                await connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS samsarix_connection_leases_room
                    ON public.samsarix_connection_leases (room_id, lease_expires_at, connection_id)
                    """
                )
                await connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS samsarix_connection_leases_instance_generation
                    ON public.samsarix_connection_leases (
                        instance_id, instance_generation, lease_expires_at, connection_id
                    )
                    """
                )
                await connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS samsarix_connection_leases_member
                    ON public.samsarix_connection_leases (room_id, subject, lease_expires_at)
                    """
                )
                await connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS public.samsarix_typing_states (
                        connection_id TEXT PRIMARY KEY REFERENCES public.samsarix_connection_leases(connection_id)
                            ON DELETE CASCADE,
                        room_id TEXT NOT NULL REFERENCES public.samsarix_rooms(id) ON DELETE CASCADE,
                        username TEXT NOT NULL CHECK (char_length(username) BETWEEN 1 AND 64),
                        expires_at TIMESTAMPTZ NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                        CHECK (expires_at > created_at)
                    )
                    """
                )
                await connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS samsarix_typing_states_expiry
                    ON public.samsarix_typing_states (expires_at, connection_id)
                    """
                )
                await connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS samsarix_typing_states_room
                    ON public.samsarix_typing_states (room_id, expires_at, connection_id)
                    """
                )
                await connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS public.samsarix_rate_buckets (
                        scope TEXT NOT NULL CHECK (scope IN ('message', 'search', 'typing')),
                        key_digest BYTEA NOT NULL CHECK (octet_length(key_digest) = 32),
                        window_started_at TIMESTAMPTZ NOT NULL,
                        event_count INTEGER NOT NULL CHECK (event_count > 0),
                        expires_at TIMESTAMPTZ NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                        PRIMARY KEY (scope, key_digest, window_started_at),
                        CHECK (expires_at > window_started_at)
                    )
                    """
                )
                await connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS samsarix_rate_buckets_expiry
                    ON public.samsarix_rate_buckets (expires_at, scope, key_digest)
                    """
                )
                await connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS public.samsarix_messages (
                        id TEXT PRIMARY KEY CHECK (char_length(id) BETWEEN 1 AND 128),
                        room_id TEXT NOT NULL REFERENCES public.samsarix_rooms(id) ON DELETE CASCADE,
                        sender TEXT NOT NULL CHECK (char_length(sender) BETWEEN 1 AND 64),
                        author_subject TEXT CHECK (
                            author_subject IS NULL OR char_length(author_subject) BETWEEN 1 AND 64
                        ),
                        content TEXT NOT NULL CHECK (char_length(content) <= 100000),
                        search_content TEXT NOT NULL CHECK (char_length(search_content) <= 100000),
                        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                        client_message_id TEXT CHECK (
                            client_message_id IS NULL OR char_length(client_message_id) BETWEEN 1 AND 128
                        ),
                        edited_at TIMESTAMPTZ,
                        deleted_at TIMESTAMPTZ,
                        UNIQUE (room_id, client_message_id)
                    )
                    """
                )
                await connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS samsarix_messages_room_order
                    ON public.samsarix_messages (room_id, created_at DESC, id DESC)
                    """
                )
                await connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS samsarix_messages_global_order
                    ON public.samsarix_messages (created_at DESC, id DESC)
                    """
                )
                await connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS public.samsarix_room_member_controls (
                        room_id TEXT NOT NULL REFERENCES public.samsarix_rooms(id) ON DELETE CASCADE,
                        subject TEXT NOT NULL CHECK (char_length(subject) BETWEEN 1 AND 64),
                        muted_until TIMESTAMPTZ,
                        banned_until TIMESTAMPTZ,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                        PRIMARY KEY (room_id, subject)
                    )
                    """
                )
                await connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS samsarix_room_member_controls_subject
                    ON public.samsarix_room_member_controls (subject, room_id)
                    """
                )
                await connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS public.samsarix_audit_events (
                        id TEXT PRIMARY KEY CHECK (char_length(id) BETWEEN 1 AND 128),
                        action TEXT NOT NULL CHECK (char_length(action) BETWEEN 1 AND 80),
                        actor TEXT NOT NULL CHECK (char_length(actor) BETWEEN 1 AND 128),
                        room_id TEXT,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                        details JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (octet_length(details::text) <= 131072)
                    )
                    """
                )
                await connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS samsarix_audit_events_order
                    ON public.samsarix_audit_events (created_at DESC, id DESC)
                    """
                )
                await connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS public.samsarix_room_read_states (
                        room_id TEXT NOT NULL REFERENCES public.samsarix_rooms(id) ON DELETE CASCADE,
                        subject TEXT NOT NULL CHECK (char_length(subject) BETWEEN 1 AND 64),
                        message_id TEXT,
                        message_created_at TIMESTAMPTZ NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                        PRIMARY KEY (room_id, subject)
                    )
                    """
                )
                await connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS samsarix_room_read_states_subject
                    ON public.samsarix_room_read_states (subject, room_id)
                    """
                )
                await connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS public.samsarix_webhook_deliveries (
                        id TEXT PRIMARY KEY CHECK (char_length(id) BETWEEN 1 AND 128),
                        event_type TEXT NOT NULL CHECK (char_length(event_type) BETWEEN 1 AND 80),
                        room_id TEXT NOT NULL CHECK (char_length(room_id) BETWEEN 1 AND 64),
                        resource_id TEXT NOT NULL CHECK (char_length(resource_id) BETWEEN 1 AND 128),
                        created_at TIMESTAMPTZ NOT NULL,
                        payload BYTEA CHECK (payload IS NULL OR octet_length(payload) <= 524288),
                        attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
                        next_attempt_at TIMESTAMPTZ,
                        last_attempt_at TIMESTAMPTZ,
                        delivered_at TIMESTAMPTZ,
                        failed_at TIMESTAMPTZ,
                        last_status_code INTEGER,
                        last_error TEXT CHECK (last_error IS NULL OR char_length(last_error) <= 1000),
                        lease_owner TEXT CHECK (lease_owner IS NULL OR char_length(lease_owner) BETWEEN 1 AND 128),
                        lease_expires_at TIMESTAMPTZ,
                        CHECK ((lease_owner IS NULL) = (lease_expires_at IS NULL))
                    )
                    """
                )
                await connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS samsarix_webhook_deliveries_due
                    ON public.samsarix_webhook_deliveries (
                        delivered_at, failed_at, next_attempt_at, lease_expires_at, created_at, id
                    )
                    """
                )
                await connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS samsarix_webhook_deliveries_order
                    ON public.samsarix_webhook_deliveries (created_at DESC, id DESC)
                    """
                )
                await connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS samsarix_webhook_deliveries_resource
                    ON public.samsarix_webhook_deliveries (room_id, resource_id, event_type)
                    """
                )
                await connection.execute(
                    """
                    INSERT INTO public.samsarix_schema_metadata (singleton, version, updated_at)
                    VALUES (TRUE, %s, clock_timestamp())
                    ON CONFLICT (singleton) DO UPDATE SET
                        version = EXCLUDED.version,
                        updated_at = clock_timestamp()
                    """,
                    (POSTGRES_SCHEMA_VERSION,),
                )

    def _require_open(self) -> None:
        if not self._opened:
            raise PostgresFoundationError("PostgreSQL foundation is not open")


def _validate_event(room_id: str, event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    if not _ROOM_ID_PATTERN.fullmatch(room_id):
        raise InvalidRealtimeEventError("invalid realtime event room ID")
    if not _EVENT_TYPE_PATTERN.fullmatch(event_type):
        raise InvalidRealtimeEventError("invalid realtime event type")
    if not isinstance(payload, dict):
        raise InvalidRealtimeEventError("realtime event payload must be an object")
    try:
        encoded = json.dumps(payload, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")
    except (TypeError, ValueError):
        raise InvalidRealtimeEventError("realtime event payload must be finite JSON") from None
    if len(encoded) > MAX_EVENT_PAYLOAD_BYTES:
        raise InvalidRealtimeEventError("realtime event payload exceeds 512 KiB")
    return cast(dict[str, Any], json.loads(encoded))


def _validate_instance(instance_id: str, lease_seconds: int) -> None:
    _validate_instance_id(instance_id)
    if not 1 <= lease_seconds <= 300:
        raise ValueError("instance lease must be between 1 and 300 seconds")


def _validate_instance_id(instance_id: str) -> None:
    if not _INSTANCE_ID_PATTERN.fullmatch(instance_id):
        raise ValueError("invalid instance ID")


def _safe_database_error(operation: str, error: psycopg.DatabaseError) -> str:
    sqlstate = error.sqlstate or "unknown"
    return f"PostgreSQL {operation} failed (SQLSTATE {sqlstate})"
