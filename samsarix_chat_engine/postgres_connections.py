# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""PostgreSQL-owned expiring WebSocket connection leases."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, cast

from psycopg import AsyncConnection
from psycopg.errors import UniqueViolation

from .postgres import (
    _ROOM_ID_PATTERN,
    InstanceLeaseError,
    PostgresFoundation,
    _validate_instance_id,
)

POSTGRES_CONNECTION_CAP_LOCK_ID = 7_495_346_927_831_819_047
_CONNECTION_ID_MAX_CHARS = 128
_USERNAME_MAX_CHARS = 64
_SUBJECT_MAX_CHARS = 64


class ConnectionLeaseError(RuntimeError):
    """Raised when a connection lease is invalid, missing, expired, or owned elsewhere."""


class ConnectionRoomUnavailableError(ConnectionLeaseError):
    """Raised when a connection's room is missing or archived."""


@dataclass(frozen=True, slots=True)
class ConnectionLease:
    """One process-owned, expiring connection reservation."""

    connection_id: str
    instance_id: str
    room_id: str
    username: str
    subject: str | None
    lease_expires_at: datetime
    created_at: datetime


@dataclass(frozen=True, slots=True)
class ConnectionCounts:
    """Live global and selected-room occupancy at one database instant."""

    total: int
    room: int


class PostgresConnectionRegistry:
    """Atomically reserve global and per-room connection capacity across processes."""

    def __init__(
        self,
        foundation: PostgresFoundation,
        *,
        max_connections: int,
        max_connections_per_room: int,
        lease_seconds: int = 30,
    ) -> None:
        if max_connections < 1:
            raise ValueError("PostgreSQL connection capacity must be positive")
        if max_connections_per_room < 1:
            raise ValueError("PostgreSQL per-room connection capacity must be positive")
        if max_connections_per_room > max_connections:
            raise ValueError("per-room connection capacity cannot exceed global capacity")
        if not 3 <= lease_seconds <= 300:
            raise ValueError("PostgreSQL connection lease must be between 3 and 300 seconds")
        self.foundation = foundation
        self.max_connections = max_connections
        self.max_connections_per_room = max_connections_per_room
        self.lease_seconds = lease_seconds

    async def try_acquire(
        self,
        *,
        connection_id: str,
        instance_id: str,
        room_id: str,
        username: str,
        subject: str | None,
    ) -> ConnectionLease | None:
        """Reserve capacity, returning ``None`` when either configured cap is full."""

        _validate_connection(connection_id, instance_id, room_id, username, subject)
        async with self.foundation.transaction() as connection:
            cursor = await connection.execute(
                """
                SELECT 1
                FROM public.samsarix_instance_cursors
                WHERE instance_id = %s AND lease_expires_at > clock_timestamp()
                FOR SHARE
                """,
                (instance_id,),
            )
            if await cursor.fetchone() is None:
                raise InstanceLeaseError("instance lease is missing or expired")

            cursor = await connection.execute(
                """
                SELECT archived_at
                FROM public.samsarix_rooms
                WHERE id = %s
                FOR SHARE
                """,
                (room_id,),
            )
            room = await cursor.fetchone()
            if room is None or room[0] is not None:
                raise ConnectionRoomUnavailableError("connection room is missing or archived")

            await connection.execute("SELECT pg_advisory_xact_lock(%s)", (POSTGRES_CONNECTION_CAP_LOCK_ID,))
            await _delete_expired(connection)

            cursor = await connection.execute(
                """
                SELECT
                    COUNT(*)::BIGINT,
                    COUNT(*) FILTER (WHERE lease.room_id = %s)::BIGINT
                FROM public.samsarix_connection_leases AS lease
                JOIN public.samsarix_instance_cursors AS owner
                  ON owner.instance_id = lease.instance_id
                JOIN public.samsarix_rooms AS room
                  ON room.id = lease.room_id
                WHERE lease.lease_expires_at > clock_timestamp()
                  AND owner.lease_expires_at > clock_timestamp()
                  AND room.archived_at IS NULL
                """,
                (room_id,),
            )
            counts = await cursor.fetchone()
            total = int(counts[0]) if counts is not None else 0
            room_total = int(counts[1]) if counts is not None else 0
            if total >= self.max_connections or room_total >= self.max_connections_per_room:
                return None

            try:
                cursor = await connection.execute(
                    """
                    INSERT INTO public.samsarix_connection_leases (
                        connection_id, instance_id, room_id, username, subject, lease_expires_at
                    )
                    SELECT
                        %s, %s, %s, %s, %s,
                        clock_timestamp() + make_interval(secs => %s)
                    WHERE EXISTS (
                        SELECT 1
                        FROM public.samsarix_instance_cursors
                        WHERE instance_id = %s
                          AND lease_expires_at > clock_timestamp()
                    )
                    RETURNING
                        connection_id, instance_id, room_id, username, subject,
                        lease_expires_at, created_at
                    """,
                    (
                        connection_id,
                        instance_id,
                        room_id,
                        username,
                        subject,
                        self.lease_seconds,
                        instance_id,
                    ),
                )
            except UniqueViolation:
                raise ConnectionLeaseError("connection ID is already leased") from None
            row = await cursor.fetchone()
        if row is None:
            raise InstanceLeaseError("instance lease expired before connection reservation")
        return _lease_from_row(row)

    async def renew(self, *, connection_id: str, instance_id: str) -> datetime:
        """Extend one live connection only while its owner and room remain usable."""

        _validate_connection_id(connection_id)
        _validate_instance_id(instance_id)
        async with self.foundation.transaction() as connection:
            cursor = await connection.execute(
                """
                UPDATE public.samsarix_connection_leases AS lease
                SET lease_expires_at = clock_timestamp() + make_interval(secs => %s),
                    updated_at = clock_timestamp()
                FROM public.samsarix_instance_cursors AS owner,
                     public.samsarix_rooms AS room
                WHERE lease.connection_id = %s
                  AND lease.instance_id = %s
                  AND lease.lease_expires_at > clock_timestamp()
                  AND owner.instance_id = lease.instance_id
                  AND owner.lease_expires_at > clock_timestamp()
                  AND room.id = lease.room_id
                  AND room.archived_at IS NULL
                RETURNING lease.lease_expires_at
                """,
                (self.lease_seconds, connection_id, instance_id),
            )
            row = await cursor.fetchone()
        if row is None:
            raise ConnectionLeaseError("connection lease is missing, expired, or unavailable")
        return cast(datetime, row[0])

    async def release(self, *, connection_id: str, instance_id: str) -> bool:
        """Release an owned connection reservation; repeated release is harmless."""

        _validate_connection_id(connection_id)
        _validate_instance_id(instance_id)
        async with self.foundation.transaction() as connection:
            cursor = await connection.execute(
                """
                DELETE FROM public.samsarix_connection_leases
                WHERE connection_id = %s AND instance_id = %s
                """,
                (connection_id, instance_id),
            )
        return cursor.rowcount == 1

    async def counts(self, *, room_id: str) -> ConnectionCounts:
        """Return live occupancy without allowing stale rows to inflate it."""

        if not _ROOM_ID_PATTERN.fullmatch(room_id):
            raise ValueError("invalid room ID")
        async with self.foundation.transaction() as connection:
            cursor = await connection.execute(
                """
                SELECT
                    COUNT(*)::BIGINT,
                    COUNT(*) FILTER (WHERE lease.room_id = %s)::BIGINT
                FROM public.samsarix_connection_leases AS lease
                JOIN public.samsarix_instance_cursors AS owner
                  ON owner.instance_id = lease.instance_id
                JOIN public.samsarix_rooms AS room
                  ON room.id = lease.room_id
                WHERE lease.lease_expires_at > clock_timestamp()
                  AND owner.lease_expires_at > clock_timestamp()
                  AND room.archived_at IS NULL
                """,
                (room_id,),
            )
            row = await cursor.fetchone()
        return ConnectionCounts(total=int(row[0]), room=int(row[1])) if row is not None else ConnectionCounts(0, 0)

    async def reap_expired(self) -> list[ConnectionLease]:
        """Delete and return rows whose socket/owner expired or whose room was archived."""

        async with self.foundation.transaction() as connection:
            await connection.execute("SELECT pg_advisory_xact_lock(%s)", (POSTGRES_CONNECTION_CAP_LOCK_ID,))
            rows = await _delete_expired(connection)
        return [_lease_from_row(row) for row in rows]


async def _delete_expired(connection: AsyncConnection[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
    cursor = await connection.execute(
        """
        DELETE FROM public.samsarix_connection_leases AS lease
        WHERE lease.lease_expires_at <= clock_timestamp()
           OR EXISTS (
                SELECT 1
                FROM public.samsarix_instance_cursors AS owner
                WHERE owner.instance_id = lease.instance_id
                  AND owner.lease_expires_at <= clock_timestamp()
           )
           OR EXISTS (
                SELECT 1
                FROM public.samsarix_rooms AS room
                WHERE room.id = lease.room_id
                  AND room.archived_at IS NOT NULL
           )
        RETURNING
            lease.connection_id, lease.instance_id, lease.room_id, lease.username,
            lease.subject, lease.lease_expires_at, lease.created_at
        """
    )
    return list(await cursor.fetchall())


def _lease_from_row(row: tuple[Any, ...]) -> ConnectionLease:
    return ConnectionLease(
        connection_id=str(row[0]),
        instance_id=str(row[1]),
        room_id=str(row[2]),
        username=str(row[3]),
        subject=None if row[4] is None else str(row[4]),
        lease_expires_at=cast(datetime, row[5]),
        created_at=cast(datetime, row[6]),
    )


def _validate_connection(
    connection_id: str,
    instance_id: str,
    room_id: str,
    username: str,
    subject: str | None,
) -> None:
    _validate_connection_id(connection_id)
    _validate_instance_id(instance_id)
    if not _ROOM_ID_PATTERN.fullmatch(room_id):
        raise ValueError("invalid room ID")
    if not 1 <= len(username) <= _USERNAME_MAX_CHARS:
        raise ValueError("username must be between 1 and 64 characters")
    if subject is not None and not 1 <= len(subject) <= _SUBJECT_MAX_CHARS:
        raise ValueError("subject must be between 1 and 64 characters")


def _validate_connection_id(connection_id: str) -> None:
    if not 1 <= len(connection_id) <= _CONNECTION_ID_MAX_CHARS or not connection_id[0].isalnum():
        raise ValueError("invalid connection ID")
    allowed = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._:-"
    if any(character not in allowed for character in connection_id):
        raise ValueError("invalid connection ID")
