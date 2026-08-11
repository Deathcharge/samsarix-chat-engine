# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Live multi-instance tests for durable PostgreSQL realtime relay behavior."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

import pytest

pytest.importorskip("psycopg")

from samsarix_chat_engine.postgres import PostgresFoundation, RealtimeEvent  # noqa: E402
from samsarix_chat_engine.postgres_realtime import PostgresRealtimeRelay  # noqa: E402

pytestmark = pytest.mark.postgres


class RecordingTarget:
    def __init__(
        self,
        *,
        fail_on_broadcast_number: int | None = None,
        fail_close_all_once: bool = False,
    ) -> None:
        self.broadcasts: list[tuple[str, dict[str, Any]]] = []
        self.closed_rooms: list[tuple[str, dict[str, Any], int, str]] = []
        self.closed_members: list[tuple[str, str, dict[str, Any], int, str]] = []
        self.close_all_calls = 0
        self.broadcast_attempts = 0
        self.fail_on_broadcast_number = fail_on_broadcast_number
        self.fail_close_all_once = fail_close_all_once
        self.broadcasted = asyncio.Event()
        self.fenced = asyncio.Event()

    async def broadcast(self, room_id: str, event: dict[str, Any]) -> None:
        self.broadcast_attempts += 1
        if self.broadcast_attempts == self.fail_on_broadcast_number:
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
        if self.fail_close_all_once:
            self.fail_close_all_once = False
            raise RuntimeError("local fence failed")
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
async def test_dispatch_failure_checkpoints_predecessor_and_retries_from_failed_event(
    clean_postgres_database: str,
) -> None:
    foundation = PostgresFoundation(clean_postgres_database)
    await foundation.open()
    target = RecordingTarget(fail_on_broadcast_number=2)
    relay = PostgresRealtimeRelay(foundation, target, instance_id="relay-retry")
    try:
        await relay.initialize()
        async with foundation.transaction() as connection:
            first_sequence = await foundation.append_event(
                connection,
                room_id="general",
                event_type="message.created",
                payload={"type": "message.created", "message": {"id": "one"}},
            )
            await foundation.append_event(
                connection,
                room_id="general",
                event_type="message.updated",
                payload={"type": "message.updated", "message": {"id": "one"}},
            )
            final_sequence = await foundation.append_event(
                connection,
                room_id="general",
                event_type="message.deleted",
                payload={"type": "message.deleted", "message": {"id": "one"}},
            )
        with pytest.raises(RuntimeError, match="local dispatch failed"):
            await relay.process_once()
        assert await foundation.register_instance("relay-retry", lease_seconds=30) == first_sequence
        assert await relay.process_once() == 2
        assert await foundation.register_instance("relay-retry", lease_seconds=30) == final_sequence
        assert [event[1]["type"] for event in target.broadcasts] == [
            "message.created",
            "message.updated",
            "message.deleted",
        ]
    finally:
        await foundation.close()


@pytest.mark.asyncio
async def test_run_survives_local_dispatch_failure(clean_postgres_database: str) -> None:
    foundation = PostgresFoundation(clean_postgres_database)
    await foundation.open()
    target = RecordingTarget(fail_on_broadcast_number=1)
    relay = PostgresRealtimeRelay(
        foundation,
        target,
        instance_id="relay-local-recovery",
        poll_interval_seconds=0.01,
    )
    task: asyncio.Task[None] | None = None
    try:
        await relay.initialize()
        async with foundation.transaction() as connection:
            sequence = await foundation.append_event(
                connection,
                room_id="general",
                event_type="message.created",
                payload={"type": "message.created", "message": {"id": "one"}},
            )
        task = asyncio.create_task(relay.run())
        await asyncio.wait_for(target.broadcasted.wait(), timeout=2)
        relay.stop()
        await asyncio.wait_for(task, timeout=2)
        task = None
        assert target.broadcast_attempts == 2
        assert await foundation.register_instance("relay-local-recovery", lease_seconds=30) == sequence
    finally:
        relay.stop()
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await foundation.close()


@pytest.mark.asyncio
async def test_expired_relay_lease_fences_then_recovers_from_cursor(clean_postgres_database: str) -> None:
    foundation = PostgresFoundation(clean_postgres_database)
    await foundation.open()
    target = RecordingTarget(fail_close_all_once=True)
    relay = PostgresRealtimeRelay(
        foundation,
        target,
        instance_id="relay-recovery",
        lease_seconds=3,
        poll_interval_seconds=0.01,
    )
    task: asyncio.Task[None] | None = None
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
        task = None
        assert target.close_all_calls == 2
        assert [event[1]["type"] for event in target.broadcasts] == ["message.deleted"]
        assert await foundation.register_instance("relay-recovery", lease_seconds=30) == sequence
    finally:
        relay.stop()
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await foundation.close()


@pytest.mark.asyncio
async def test_relay_fences_and_skips_to_head_after_retained_event_gap(
    clean_postgres_database: str,
) -> None:
    foundation = PostgresFoundation(clean_postgres_database)
    await foundation.open()
    target = RecordingTarget()
    relay = PostgresRealtimeRelay(
        foundation,
        target,
        instance_id="relay-retained-gap",
        lease_seconds=3,
        poll_interval_seconds=0.01,
    )
    task: asyncio.Task[None] | None = None
    try:
        await relay.initialize()
        async with foundation.transaction() as connection:
            for index in range(3):
                final_sequence = await foundation.append_event(
                    connection,
                    room_id="general",
                    event_type="message.created",
                    payload={"type": "message.created", "message": {"id": str(index)}},
                )
            await connection.execute(
                """
                UPDATE public.samsarix_instance_cursors
                SET lease_expires_at = clock_timestamp() - interval '1 second'
                WHERE instance_id = %s
                """,
                (relay.instance_id,),
            )
        pruned = await foundation.prune_events(max_events=1, max_age_seconds=31_536_000)
        assert pruned.pruned_events == 2
        assert await relay.initialize() == 0

        task = asyncio.create_task(relay.run())
        await asyncio.wait_for(target.fenced.wait(), timeout=2)
        for _attempt in range(100):
            if await foundation.register_instance(relay.instance_id, lease_seconds=3) == final_sequence:
                break
            await asyncio.sleep(0.01)
        else:
            pytest.fail("relay did not recover its retained event gap")
        relay.stop()
        await asyncio.wait_for(task, timeout=2)
        task = None
        assert target.close_all_calls == 1
        assert target.broadcasts == []
    finally:
        relay.stop()
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
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
    with pytest.raises(ValueError, match="lease"):
        PostgresRealtimeRelay(foundation, target, lease_seconds=301)
    with pytest.raises(ValueError, match="poll interval"):
        PostgresRealtimeRelay(foundation, target, poll_interval_seconds=5.1)
    with pytest.raises(ValueError, match="batch size"):
        PostgresRealtimeRelay(foundation, target, batch_size=1_001)
    with pytest.raises(ValueError, match="instance ID"):
        PostgresRealtimeRelay(foundation, target, instance_id="")
    with pytest.raises(ValueError, match="instance ID"):
        PostgresRealtimeRelay(foundation, target, instance_id="x" * 129)


@pytest.mark.asyncio
async def test_malformed_and_timezone_naive_moderation_events_are_discarded(
    caplog: pytest.LogCaptureFixture,
) -> None:
    foundation = PostgresFoundation("postgresql://unused")
    target = RecordingTarget()
    relay = PostgresRealtimeRelay(foundation, target)
    created_at = datetime.now(timezone.utc)
    await relay._dispatch(
        RealtimeEvent(
            sequence=1,
            room_id="general",
            event_type="member.moderation.updated",
            payload={"moderation": {"invalid": True}},
            created_at=created_at,
        )
    )
    await relay._dispatch(
        RealtimeEvent(
            sequence=2,
            room_id="general",
            event_type="member.moderation.updated",
            payload={
                "moderation": {
                    "room_id": "general",
                    "subject": "alice",
                    "muted_until": None,
                    "banned_until": "2100-01-01T00:00:00",
                    "updated_at": "2026-08-02T00:00:00+00:00",
                }
            },
            created_at=created_at,
        )
    )
    assert target.closed_members == []
    assert "sequence 1" in caplog.text
    assert "sequence 2" in caplog.text
