# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""PostgreSQL-owned expiring WebSocket connection leases."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, cast
from uuid import UUID

from psycopg import AsyncConnection
from psycopg.errors import UniqueViolation

from .models import Room
from .postgres import (
    _ROOM_ID_PATTERN,
    InstanceLeaseError,
    PostgresFoundation,
    _validate_instance_id,
)

POSTGRES_CONNECTION_CAP_LOCK_ID = 7_495_346_927_831_819_047
_ADMISSION_SWEEP_LIMIT = 16
_CONNECTION_ID_MAX_CHARS = 128
_USERNAME_MAX_CHARS = 64
_SUBJECT_MAX_CHARS = 64


class ConnectionLeaseError(RuntimeError):
    """Raised when a connection lease is invalid, missing, expired, or owned elsewhere."""


class ConnectionRoomUnavailableError(ConnectionLeaseError):
    """Raised when a connection's room is missing or archived."""

    def __init__(self, room: Room | None) -> None:
        super().__init__("connection room is missing" if room is None else "connection room is archived")
        self.room = room


@dataclass(frozen=True, slots=True)
class ConnectionLease:
    """One process-owned, expiring connection reservation."""

    connection_id: str
    instance_id: str
    instance_generation: UUID
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


@dataclass(frozen=True, slots=True)
class PresenceTransition:
    """One committed join/leave transition derived from a connection lease."""

    active: bool
    connection_id: str
    room_id: str
    username: str
    active_connections: int
    sequence: int


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
                SELECT generation
                FROM public.samsarix_instance_cursors
                WHERE instance_id = %s AND lease_expires_at > clock_timestamp()
                FOR SHARE
                """,
                (instance_id,),
            )
            owner = await cursor.fetchone()
            if owner is None:
                raise InstanceLeaseError("instance lease is missing or expired")
            instance_generation = cast(UUID, owner[0])

            cursor = await connection.execute(
                """
                SELECT id, name, description, created_at, archived_at, frozen_at
                FROM public.samsarix_rooms
                WHERE id = %s
                FOR SHARE
                """,
                (room_id,),
            )
            room = await cursor.fetchone()
            if room is None or room[4] is not None:
                raise ConnectionRoomUnavailableError(_room_snapshot(room) if room is not None else None)

            await connection.execute("SELECT pg_advisory_xact_lock(%s)", (POSTGRES_CONNECTION_CAP_LOCK_ID,))
            stale = await _delete_expired(
                connection,
                limit=_ADMISSION_SWEEP_LIMIT,
                prioritize_connection_id=connection_id,
            )
            await self._append_departures(connection, stale)

            cursor = await connection.execute(
                """
                SELECT
                    COUNT(*)::BIGINT,
                    COUNT(*) FILTER (WHERE lease.room_id = %s)::BIGINT
                FROM public.samsarix_connection_leases AS lease
                JOIN public.samsarix_instance_cursors AS owner
                  ON owner.instance_id = lease.instance_id
                 AND owner.generation = lease.instance_generation
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
                        connection_id, instance_id, instance_generation, room_id,
                        username, subject, lease_expires_at
                    )
                    SELECT
                        %s, %s, %s, %s, %s, %s,
                        clock_timestamp() + make_interval(secs => %s)
                    WHERE EXISTS (
                        SELECT 1
                        FROM public.samsarix_instance_cursors
                        WHERE instance_id = %s
                          AND generation = %s
                          AND lease_expires_at > clock_timestamp()
                    )
                    RETURNING
                        connection_id, instance_id, instance_generation, room_id, username, subject,
                        lease_expires_at, created_at
                    """,
                    (
                        connection_id,
                        instance_id,
                        instance_generation,
                        room_id,
                        username,
                        subject,
                        self.lease_seconds,
                        instance_id,
                        instance_generation,
                    ),
                )
            except UniqueViolation:
                raise ConnectionLeaseError("connection ID is already leased") from None
            row = await cursor.fetchone()
            if row is not None:
                active_connections = await _room_active_count(connection, room_id)
                await self._append_presence(
                    connection,
                    active=True,
                    connection_id=connection_id,
                    room_id=room_id,
                    username=username,
                    active_connections=active_connections,
                )
        if row is None:
            raise InstanceLeaseError("instance lease expired before connection reservation")
        return _lease_from_row(row)

    async def renew(self, *, connection_id: str, instance_id: str, room_id: str) -> datetime:
        """Extend one live connection only while its owner and room remain usable."""

        _validate_connection_id(connection_id)
        _validate_instance_id(instance_id)
        if not _ROOM_ID_PATTERN.fullmatch(room_id):
            raise ValueError("invalid room ID")
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
                  AND lease.room_id = %s
                  AND lease.lease_expires_at > clock_timestamp()
                  AND owner.instance_id = lease.instance_id
                  AND owner.generation = lease.instance_generation
                  AND owner.lease_expires_at > clock_timestamp()
                  AND room.id = lease.room_id
                  AND room.archived_at IS NULL
                RETURNING lease.lease_expires_at
                """,
                (self.lease_seconds, connection_id, instance_id, room_id),
            )
            row = await cursor.fetchone()
            if row is None:
                # Maintenance may already have removed an archived reservation.
                # Diagnose the authenticated room, not only the surviving lease row.
                cursor = await connection.execute(
                    """
                    SELECT id, name, description, created_at, archived_at, frozen_at
                    FROM public.samsarix_rooms WHERE id = %s
                    """,
                    (room_id,),
                )
                room = await cursor.fetchone()
                if room is None or room[4] is not None:
                    raise ConnectionRoomUnavailableError(_room_snapshot(room) if room is not None else None)
        if row is None:
            raise ConnectionLeaseError("connection lease is missing, expired, or unavailable")
        return cast(datetime, row[0])

    async def release(self, *, connection_id: str, instance_id: str) -> bool:
        """Release an owned connection reservation; repeated release is harmless."""

        _validate_connection_id(connection_id)
        _validate_instance_id(instance_id)
        async with self.foundation.transaction() as connection:
            await connection.execute("SELECT pg_advisory_xact_lock(%s)", (POSTGRES_CONNECTION_CAP_LOCK_ID,))
            cursor = await connection.execute(
                """
                SELECT lease.room_id, lease.username,
                       EXISTS (
                           SELECT 1 FROM public.samsarix_typing_states AS typing
                           WHERE typing.connection_id = lease.connection_id
                       )
                FROM public.samsarix_connection_leases AS lease
                WHERE lease.connection_id = %s AND lease.instance_id = %s
                FOR UPDATE
                """,
                (connection_id, instance_id),
            )
            row = await cursor.fetchone()
            if row is None:
                return False
            room_id, username, was_typing = str(row[0]), str(row[1]), bool(row[2])
            await connection.execute(
                "DELETE FROM public.samsarix_connection_leases WHERE connection_id = %s",
                (connection_id,),
            )
            if was_typing:
                await self._append_typing_stopped(connection, connection_id, room_id, username)
            active_connections = await _room_active_count(connection, room_id)
            await self._append_presence(
                connection,
                active=False,
                connection_id=connection_id,
                room_id=room_id,
                username=username,
                active_connections=active_connections,
            )
        return True

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
                 AND owner.generation = lease.instance_generation
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

    async def total_count(self) -> int:
        """Return live deployment-wide occupancy."""

        async with self.foundation.transaction() as connection:
            cursor = await connection.execute(
                """
                SELECT COUNT(*)::BIGINT
                FROM public.samsarix_connection_leases AS lease
                JOIN public.samsarix_instance_cursors AS owner
                  ON owner.instance_id = lease.instance_id
                 AND owner.generation = lease.instance_generation
                JOIN public.samsarix_rooms AS room
                  ON room.id = lease.room_id
                WHERE lease.lease_expires_at > clock_timestamp()
                  AND owner.lease_expires_at > clock_timestamp()
                  AND room.archived_at IS NULL
                """
            )
            row = await cursor.fetchone()
        return int(row[0]) if row is not None else 0

    async def reap_expired(self, *, limit: int = 100) -> list[PresenceTransition]:
        """Delete a bounded stale batch and emit best-effort leave transitions."""

        if not 1 <= limit <= 1_000:
            raise ValueError("connection expiry batch must be between 1 and 1000")
        async with self.foundation.transaction() as connection:
            await connection.execute("SELECT pg_advisory_xact_lock(%s)", (POSTGRES_CONNECTION_CAP_LOCK_ID,))
            rows = await _delete_expired(connection, limit=limit)
            transitions = await self._append_departures(connection, rows)
        return transitions

    async def _append_departures(
        self,
        connection: AsyncConnection[tuple[Any, ...]],
        rows: list[tuple[Any, ...]],
    ) -> list[PresenceTransition]:
        transitions: list[PresenceTransition] = []
        for row in sorted(rows, key=lambda item: str(item[0])):
            connection_id, room_id, username = str(row[0]), str(row[3]), str(row[4])
            if bool(row[8]):
                await self._append_typing_stopped(connection, connection_id, room_id, username)
            active_connections = await _room_active_count(connection, room_id)
            sequence = await self._append_presence(
                connection,
                active=False,
                connection_id=connection_id,
                room_id=room_id,
                username=username,
                active_connections=active_connections,
            )
            transitions.append(
                PresenceTransition(False, connection_id, room_id, username, active_connections, sequence)
            )
        return transitions

    async def _append_presence(
        self,
        connection: AsyncConnection[tuple[Any, ...]],
        *,
        active: bool,
        connection_id: str,
        room_id: str,
        username: str,
        active_connections: int,
    ) -> int:
        event_type = "presence.joined" if active else "presence.left"
        return await self.foundation.append_event(
            connection,
            room_id=room_id,
            event_type=event_type,
            payload={
                "type": event_type,
                "username": username,
                "active_connections": active_connections,
                "origin_connection_id": connection_id,
            },
        )

    async def _append_typing_stopped(
        self,
        connection: AsyncConnection[tuple[Any, ...]],
        connection_id: str,
        room_id: str,
        username: str,
    ) -> int:
        return await self.foundation.append_event(
            connection,
            room_id=room_id,
            event_type="typing.stopped",
            payload={
                "type": "typing.stopped",
                "username": username,
                "origin_connection_id": connection_id,
            },
        )


async def _delete_expired(
    connection: AsyncConnection[tuple[Any, ...]],
    *,
    limit: int,
    prioritize_connection_id: str | None = None,
) -> list[tuple[Any, ...]]:
    cursor = await connection.execute(
        """
        WITH stale AS MATERIALIZED (
            SELECT
                lease.connection_id, lease.instance_id, lease.instance_generation,
                lease.room_id, lease.username, lease.subject,
                lease.lease_expires_at, lease.created_at,
                EXISTS (
                    SELECT 1 FROM public.samsarix_typing_states AS typing
                    WHERE typing.connection_id = lease.connection_id
                ) AS was_typing
            FROM public.samsarix_connection_leases AS lease
            WHERE lease.lease_expires_at <= clock_timestamp()
               OR EXISTS (
                    SELECT 1
                    FROM public.samsarix_instance_cursors AS owner
                    WHERE owner.instance_id = lease.instance_id
                      AND (
                          owner.lease_expires_at <= clock_timestamp()
                          OR owner.generation <> lease.instance_generation
                      )
               )
               OR EXISTS (
                    SELECT 1
                    FROM public.samsarix_rooms AS room
                    WHERE room.id = lease.room_id
                      AND room.archived_at IS NOT NULL
               )
            ORDER BY (lease.connection_id = %s) DESC, lease.lease_expires_at, lease.connection_id
            LIMIT %s
            FOR UPDATE OF lease SKIP LOCKED
        ), deleted AS (
            DELETE FROM public.samsarix_connection_leases AS lease
            USING stale
            WHERE lease.connection_id = stale.connection_id
            RETURNING lease.connection_id
        )
        SELECT
            stale.connection_id, stale.instance_id, stale.instance_generation,
            stale.room_id, stale.username, stale.subject,
            stale.lease_expires_at, stale.created_at, stale.was_typing
        FROM stale
        JOIN deleted USING (connection_id)
        """,
        (prioritize_connection_id, limit),
    )
    return list(await cursor.fetchall())


async def _room_active_count(connection: AsyncConnection[tuple[Any, ...]], room_id: str) -> int:
    cursor = await connection.execute(
        """
        SELECT COUNT(*)
        FROM public.samsarix_connection_leases AS lease
        JOIN public.samsarix_instance_cursors AS owner
          ON owner.instance_id = lease.instance_id
         AND owner.generation = lease.instance_generation
        JOIN public.samsarix_rooms AS room ON room.id = lease.room_id
        WHERE lease.room_id = %s
          AND lease.lease_expires_at > clock_timestamp()
          AND owner.lease_expires_at > clock_timestamp()
          AND room.archived_at IS NULL
        """,
        (room_id,),
    )
    row = await cursor.fetchone()
    return int(row[0]) if row is not None else 0


def _room_snapshot(row: tuple[Any, ...]) -> Room:
    return Room(
        id=str(row[0]),
        name=str(row[1]),
        description=str(row[2]),
        created_at=row[3],
        archived_at=row[4],
        frozen_at=row[5],
    )


def _lease_from_row(row: tuple[Any, ...]) -> ConnectionLease:
    return ConnectionLease(
        connection_id=str(row[0]),
        instance_id=str(row[1]),
        instance_generation=cast(UUID, row[2]),
        room_id=str(row[3]),
        username=str(row[4]),
        subject=None if row[5] is None else str(row[5]),
        lease_expires_at=cast(datetime, row[6]),
        created_at=cast(datetime, row[7]),
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
