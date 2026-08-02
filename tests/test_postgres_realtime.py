# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Live multi-instance tests for durable PostgreSQL realtime relay behavior."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

pytest.importorskip("psycopg")

from samsarix_chat_engine.postgres import PostgresFoundation  # noqa: E402
from samsarix_chat_engine.postgres_realtime import PostgresRealtimeRelay  # noqa: E402


class RecordingTarget:
    def __init__(self, *, fail_broadcast_once: bool = False) -> None:
        self.broadcasts: list[tuple[str, dict[str, Any]]] = []
        self.closed_rooms: list[tuple[str, dict[str, Any], int, str]] = []
        self.closed_members: list[tuple[str, str, dict[str, Any], int, str]] = []
        self.close_all_calls = 0
        self.fail_broadcast_once = fail_broadcast_once
        self.broadcasted = asyncio.Event()
        self.fenced = asyncio.Event()

    async def broadcast(self, room_id: str, event: dict[str, Any]) -> None:
        if self.fail_broadcast_once:
            self.fail_broadcast_once = False
            raise RuntimeError("local dispatch failed")
        self.broadcasts.append((room_id, event))
        self.broadcasted.set()

    async def close_room(
        self,
        room_id: str,
        event: dict[str, Any],
        *,
        code: int = 4409,
        reason: str = "Room archived",
    ) -> None:
        self.closed_rooms.append((room_id, event, code, reason))

    async def close_member(
        self,
        room_id: str,
        subject: str,
        event: dict[str, Any],
        *,
        code: int = 4403,
        reason: str = "Room access revoked",
    ) -> int:
        self.closed_members.append((room_id, subject, event, code, reason))
        return 1

    async def close_all(self) -> None:
        self.close_all_calls += 1
        self.fenced.set()


@pytest.mark.asyncio
async def test_two_relays_receive_each_committed_public_event_once(clean_postgres_database: str) -> None:
    writer = PostgresFoundation(clean_postgres_database)
    first_foundation = PostgresFoundation(clean_postgres_database)
    second_foundation = PostgresFoundation(clean_postgres_database)
    await asyncio.gather(writer.open(), first_foundation.open(), second_foundation.open())
    first_target = RecordingTarget()
    second_target = RecordingTarget()
    first = PostgresRealtimeRelay(first_foundation, first_target, instance_id="relay-first")
    second = PostgresRealtimeRelay(second_foundation, second_target, instance_id="relay-second")
    try:
        assert await asyncio.gather(first.initialize(), second.initialize()) == [0, 0]
        async with writer.transaction() as connection:
            await writer.append_event(
                connection,
                room_id="general",
                event_type="message.created",
                payload={"type": "message.created", "message": {"id": "one"}},
            )
            await writer.append_event(
                connection,
                room_id="general",
                event_type="room.frozen",
                payload={"type": "room.frozen", "room": {"id": "general"}},
            )
            await writer.append_event(
                connection,
                room_id="general",
                event_type="member.moderation.updated",
                payload={
                    "type": "member.moderation.updated",
                    "moderation": {
                        "room_id": "general",
                        "subject": "alice",
                        "muted_until": None,
                        "banned_until": "2100-01-01T00:00:00+00:00",
                        "updated_at": "2026-08-02T00:00:00+00:00",
                    },
                },
            )
            archived_sequence = await writer.append_event(
                connection,
                room_id="general",
                event_type="room.archived",
                payload={"type": "room.archived", "room": {"id": "general"}},
            )
            await writer.append_event(
                connection,
                room_id="general",
                event_type="room.deleted",
                payload={"type": "room.deleted", "room_id": "general"},
            )

        assert await asyncio.gather(first.process_once(), second.process_once()) == [5, 5]
        assert await asyncio.gather(first.process_once(), second.process_once()) == [0, 0]
        for target in (first_target, second_target):
            assert [event[1]["type"] for event in target.broadcasts] == ["message.created", "room.frozen"]
            assert [(room_id, subject) for room_id, subject, *_rest in target.closed_members] == [("general", "alice")]
            assert [event[1]["type"] for event in target.closed_rooms] == ["room.archived"]
        assert await first_foundation.register_instance("relay-first", lease_seconds=30) > archived_sequence
        assert await second_foundation.register_instance("relay-second", lease_seconds=30) > archived_sequence
    finally:
        await asyncio.gather(writer.close(), first_foundation.close(), second_foundation.close())


@pytest.mark.asyncio
async def test_dispatch_failure_is_not_acknowledged_and_replays(clean_postgres_database: str) -> None:
    foundation = PostgresFoundation(clean_postgres_database)
    await foundation.open()
    target = RecordingTarget(fail_broadcast_once=True)
    relay = PostgresRealtimeRelay(foundation, target, instance_id="relay-retry")
    try:
        await relay.initialize()
        async with foundation.transaction() as connection:
            sequence = await foundation.append_event(
                connection,
                room_id="general",
                event_type="message.updated",
                payload={"type": "message.updated", "message": {"id": "one"}},
            )
        with pytest.raises(RuntimeError, match="local dispatch failed"):
            await relay.process_once()
        assert await foundation.register_instance("relay-retry", lease_seconds=30) < sequence
        assert await relay.process_once() == 1
        assert await foundation.register_instance("relay-retry", lease_seconds=30) == sequence
        assert [event[1]["type"] for event in target.broadcasts] == ["message.updated"]
    finally:
        await foundation.close()


@pytest.mark.asyncio
async def test_expired_relay_lease_fences_then_recovers_from_cursor(clean_postgres_database: str) -> None:
    foundation = PostgresFoundation(clean_postgres_database)
    await foundation.open()
    target = RecordingTarget()
    relay = PostgresRealtimeRelay(
        foundation,
        target,
        instance_id="relay-recovery",
        lease_seconds=3,
        poll_interval_seconds=0.01,
    )
    try:
        await relay.initialize()
        async with foundation.transaction() as connection:
            await connection.execute(
                """
                UPDATE public.samsarix_instance_cursors
                SET lease_expires_at = clock_timestamp() - interval '1 second'
                WHERE instance_id = %s
                """,
                (relay.instance_id,),
            )
            sequence = await foundation.append_event(
                connection,
                room_id="general",
                event_type="message.deleted",
                payload={"type": "message.deleted", "message": {"id": "one"}},
            )

        task = asyncio.create_task(relay.run())
        await asyncio.wait_for(target.fenced.wait(), timeout=2)
        await asyncio.wait_for(target.broadcasted.wait(), timeout=2)
        relay.stop()
        await asyncio.wait_for(task, timeout=2)
        assert target.close_all_calls == 1
        assert [event[1]["type"] for event in target.broadcasts] == ["message.deleted"]
        assert await foundation.register_instance("relay-recovery", lease_seconds=30) == sequence
    finally:
        relay.stop()
        await foundation.close()


def test_relay_configuration_is_bounded() -> None:
    foundation = PostgresFoundation("postgresql://unused")
    target = RecordingTarget()
    with pytest.raises(ValueError, match="lease"):
        PostgresRealtimeRelay(foundation, target, lease_seconds=2)
    with pytest.raises(ValueError, match="poll interval"):
        PostgresRealtimeRelay(foundation, target, poll_interval_seconds=0)
    with pytest.raises(ValueError, match="batch size"):
        PostgresRealtimeRelay(foundation, target, batch_size=0)
