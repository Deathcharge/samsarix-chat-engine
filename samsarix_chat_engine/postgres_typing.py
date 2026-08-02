# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Connection-bound PostgreSQL typing transitions and expiry sweeping."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from psycopg import AsyncConnection

from .postgres import PostgresFoundation, _validate_instance_id
from .postgres_connections import _validate_connection_id


class TypingStateError(RuntimeError):
    """Raised when a typing transition has no usable owned connection."""


@dataclass(frozen=True, slots=True)
class TypingTransition:
    """One emitted typing transition and its durable event sequence."""

    active: bool
    connection_id: str
    room_id: str
    username: str
    sequence: int


class PostgresTypingRegistry:
    """Refresh transition-only typing state using database time."""

    def __init__(self, foundation: PostgresFoundation, *, timeout_seconds: float = 8.0) -> None:
        if not 1 <= timeout_seconds <= 30:
            raise ValueError("PostgreSQL typing timeout must be between 1 and 30 seconds")
        self.foundation = foundation
        self.timeout_seconds = timeout_seconds

    async def start(self, *, connection_id: str, instance_id: str) -> TypingTransition | None:
        """Start or refresh typing, emitting only on an inactive-to-active transition."""

        _validate_connection_id(connection_id)
        _validate_instance_id(instance_id)
        async with self.foundation.transaction() as connection:
            cursor = await connection.execute(
                """
                SELECT lease.room_id, lease.username
                FROM public.samsarix_connection_leases AS lease
                JOIN public.samsarix_instance_cursors AS owner
                  ON owner.instance_id = lease.instance_id
                JOIN public.samsarix_rooms AS room
                  ON room.id = lease.room_id
                WHERE lease.connection_id = %s
                  AND lease.instance_id = %s
                  AND lease.lease_expires_at > clock_timestamp()
                  AND owner.lease_expires_at > clock_timestamp()
                  AND room.archived_at IS NULL
                FOR UPDATE OF lease
                """,
                (connection_id, instance_id),
            )
            lease = await cursor.fetchone()
            if lease is None:
                raise TypingStateError("typing connection is missing, expired, or unavailable")
            room_id, username = str(lease[0]), str(lease[1])
            cursor = await connection.execute(
                """
                SELECT expires_at > clock_timestamp()
                FROM public.samsarix_typing_states
                WHERE connection_id = %s
                FOR UPDATE
                """,
                (connection_id,),
            )
            current = await cursor.fetchone()
            already_active = current is not None and bool(current[0])
            await connection.execute(
                """
                INSERT INTO public.samsarix_typing_states (
                    connection_id, room_id, username, expires_at
                )
                VALUES (
                    %s, %s, %s, clock_timestamp() + make_interval(secs => %s)
                )
                ON CONFLICT (connection_id) DO UPDATE SET
                    room_id = EXCLUDED.room_id,
                    username = EXCLUDED.username,
                    expires_at = EXCLUDED.expires_at,
                    updated_at = clock_timestamp()
                """,
                (connection_id, room_id, username, self.timeout_seconds),
            )
            if already_active:
                return None
            sequence = await self.foundation.append_event(
                connection,
                room_id=room_id,
                event_type="typing.started",
                payload={
                    "type": "typing.started",
                    "username": username,
                    "expires_in": self.timeout_seconds,
                    "origin_connection_id": connection_id,
                },
            )
        return TypingTransition(True, connection_id, room_id, username, sequence)

    async def stop(self, *, connection_id: str, instance_id: str) -> TypingTransition | None:
        """Stop owned typing state; repeated stops are harmless."""

        _validate_connection_id(connection_id)
        _validate_instance_id(instance_id)
        async with self.foundation.transaction() as connection:
            cursor = await connection.execute(
                """
                DELETE FROM public.samsarix_typing_states AS typing
                USING public.samsarix_connection_leases AS lease
                WHERE typing.connection_id = %s
                  AND lease.connection_id = typing.connection_id
                  AND lease.instance_id = %s
                RETURNING typing.room_id, typing.username
                """,
                (connection_id, instance_id),
            )
            row = await cursor.fetchone()
            if row is None:
                return None
            room_id, username = str(row[0]), str(row[1])
            sequence = await self._append_stopped(connection, connection_id, room_id, username)
        return TypingTransition(False, connection_id, room_id, username, sequence)

    async def reap_expired(self, *, limit: int = 100) -> list[TypingTransition]:
        """Delete and emit a bounded batch of database-expired typing states."""

        if not 1 <= limit <= 1_000:
            raise ValueError("typing expiry batch must be between 1 and 1000")
        transitions: list[TypingTransition] = []
        async with self.foundation.transaction() as connection:
            cursor = await connection.execute(
                """
                WITH expired AS MATERIALIZED (
                    SELECT connection_id
                    FROM public.samsarix_typing_states
                    WHERE expires_at <= clock_timestamp()
                    ORDER BY expires_at, connection_id
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                )
                DELETE FROM public.samsarix_typing_states AS typing
                USING expired
                WHERE typing.connection_id = expired.connection_id
                RETURNING typing.connection_id, typing.room_id, typing.username
                """,
                (limit,),
            )
            for row in await cursor.fetchall():
                connection_id, room_id, username = str(row[0]), str(row[1]), str(row[2])
                sequence = await self._append_stopped(connection, connection_id, room_id, username)
                transitions.append(TypingTransition(False, connection_id, room_id, username, sequence))
        return transitions

    async def _append_stopped(
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
