# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Contract tests for the internal PostgreSQL multi-instance foundation."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

psycopg = pytest.importorskip("psycopg")

from samsarix_chat_engine.postgres import (  # noqa: E402
    POSTGRES_SCHEMA_VERSION,
    EventLogGapError,
    InstanceLeaseError,
    InvalidRealtimeEventError,
    PostgresFoundation,
    PostgresFoundationError,
    UnsupportedPostgresSchemaError,
    _validate_event,
)
from samsarix_chat_engine.postgres_runtime import PostgresApplicationRuntime  # noqa: E402

pytestmark = pytest.mark.postgres


@pytest.mark.asyncio
async def test_runtime_maintenance_isolates_failures_and_throttles_event_pruning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    runtime = object.__new__(PostgresApplicationRuntime)
    runtime.typing = SimpleNamespace(reap_expired=AsyncMock(side_effect=RuntimeError("typing unavailable")))
    runtime.connections = SimpleNamespace(reap_expired=AsyncMock(return_value=0))
    runtime.message_limiter = SimpleNamespace(prune_expired=AsyncMock(return_value=0))
    runtime.search_limiter = SimpleNamespace(prune_expired=AsyncMock(return_value=0))
    runtime.typing_limiter = SimpleNamespace(prune_expired=AsyncMock(return_value=0))
    prune_events = AsyncMock(return_value=0)
    runtime.store = SimpleNamespace(foundation=SimpleNamespace(prune_events=prune_events))
    runtime.max_realtime_events = 1_000
    runtime.realtime_event_max_age_seconds = 3_600
    runtime._next_event_prune_at = 0.0

    await runtime.run_maintenance_once()
    await runtime.run_maintenance_once()

    assert runtime.connections.reap_expired.await_count == 2
    assert runtime.message_limiter.prune_expired.await_count == 2
    assert prune_events.await_count == 1
    assert "maintenance step typing failed: RuntimeError" in caplog.text


def test_event_validation_is_bounded_and_canonical() -> None:
    assert _validate_event("room-1", "message.created", {"emoji": "🌀"}) == {"emoji": "🌀"}
    assert len(_validate_event("room", "message.created", {"content": "🌀" * 100_000})["content"]) == 100_000

    with pytest.raises(InvalidRealtimeEventError):
        _validate_event("INVALID", "message.created", {})
    with pytest.raises(InvalidRealtimeEventError):
        _validate_event("room", "Message Created", {})
    with pytest.raises(InvalidRealtimeEventError):
        _validate_event("room", "message.created", {"invalid": float("nan")})
    with pytest.raises(InvalidRealtimeEventError):
        _validate_event("room", "message.created", {"content": "x" * (512 * 1024)})


def test_pool_configuration_rejects_invalid_bounds_without_echoing_conninfo() -> None:
    secret_conninfo = "postgresql://user:do-not-echo@example.invalid/db"
    with pytest.raises(ValueError, match="pool bounds") as error:
        PostgresFoundation(secret_conninfo, min_pool_size=2, max_pool_size=1)
    assert secret_conninfo not in str(error.value)


@pytest.fixture
async def foundation(clean_postgres_database: str) -> AsyncIterator[PostgresFoundation]:
    service = PostgresFoundation(clean_postgres_database)
    await service.open()
    try:
        yield service
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_schema_event_order_and_monotonic_cursor(foundation: PostgresFoundation) -> None:
    assert await foundation.schema_version() == POSTGRES_SCHEMA_VERSION
    assert await foundation.current_head() == 0
    assert await foundation.register_instance("worker-a", lease_seconds=30) == 0

    async with foundation.transaction() as connection:
        first = await foundation.append_event(
            connection,
            room_id="room-a",
            event_type="message.created",
            payload={"message_id": "one"},
        )
        second = await foundation.append_event(
            connection,
            room_id="room-a",
            event_type="message.updated",
            payload={"message_id": "one", "edited": True},
        )

    assert second == first + 1
    events = await foundation.read_events("worker-a")
    assert [event.sequence for event in events] == [first, second]
    assert [event.event_type for event in events] == ["message.created", "message.updated"]
    assert events[1].payload == {"edited": True, "message_id": "one"}
    assert await foundation.acknowledge_events("worker-a", through_sequence=second) == second
    assert await foundation.acknowledge_events("worker-a", through_sequence=first) == second
    assert await foundation.read_events("worker-a") == []
    with pytest.raises(InstanceLeaseError, match="beyond"):
        await foundation.acknowledge_events("worker-a", through_sequence=second + 1)


@pytest.mark.asyncio
async def test_event_row_rolls_back_with_caller_transaction(foundation: PostgresFoundation) -> None:
    await foundation.register_instance("worker-rollback", lease_seconds=30)
    rolled_back_sequence = 0

    with pytest.raises(RuntimeError, match="abort mutation"):
        async with foundation.transaction() as connection:
            rolled_back_sequence = await foundation.append_event(
                connection,
                room_id="room-a",
                event_type="message.created",
                payload={"message_id": "not-committed"},
            )
            raise RuntimeError("abort mutation")

    assert await foundation.current_head() == 0
    assert await foundation.read_events("worker-rollback") == []
    async with foundation.transaction() as connection:
        committed_sequence = await foundation.append_event(
            connection,
            room_id="room-a",
            event_type="message.created",
            payload={"message_id": "committed"},
        )
    assert committed_sequence > rolled_back_sequence
    with pytest.raises(InstanceLeaseError, match="not a committed event"):
        await foundation.acknowledge_events("worker-rollback", through_sequence=rolled_back_sequence)


@pytest.mark.asyncio
async def test_event_pruning_respects_live_cursors_and_stale_instances_recover(
    foundation: PostgresFoundation,
) -> None:
    assert await foundation.register_instance("worker-current", lease_seconds=30) == 0
    assert await foundation.register_instance("worker-stale", lease_seconds=30) == 0
    async with foundation.transaction() as connection:
        sequences = [
            await foundation.append_event(
                connection,
                room_id="room-a",
                event_type="message.created",
                payload={"message_id": str(index)},
            )
            for index in range(3)
        ]

    blocked = await foundation.prune_events(max_events=1, max_age_seconds=31_536_000)
    assert blocked.pruned_events == 0
    assert blocked.pruned_through_sequence == 0

    await foundation.acknowledge_events("worker-current", through_sequence=sequences[-1])
    async with foundation.transaction() as connection:
        cursor = await connection.execute(
            """
            UPDATE public.samsarix_instance_cursors
            SET lease_expires_at = clock_timestamp() - interval '1 second'
            WHERE instance_id = 'worker-stale'
            """
        )
        assert cursor.rowcount == 1

    pruned = await foundation.prune_events(max_events=1, max_age_seconds=31_536_000)
    assert pruned.pruned_events == 2
    assert pruned.pruned_through_sequence == sequences[1]
    assert await foundation.event_retention_floor() == sequences[1]

    assert await foundation.register_instance("worker-stale", lease_seconds=30) == 0
    with pytest.raises(EventLogGapError, match="predates"):
        await foundation.read_events("worker-stale")
    assert await foundation.recover_instance_after_gap("worker-stale", lease_seconds=30) == sequences[-1]
    assert await foundation.read_events("worker-stale") == []
    with pytest.raises(InstanceLeaseError, match="no retained event gap"):
        await foundation.recover_instance_after_gap("worker-stale", lease_seconds=30)

    async with foundation.transaction() as connection:
        await connection.execute(
            """
            UPDATE public.samsarix_realtime_events
            SET created_at = clock_timestamp() - interval '61 seconds'
            """
        )
    final_prune = await foundation.prune_events(max_events=1, max_age_seconds=60)
    assert final_prune.pruned_events == 1
    assert final_prune.pruned_through_sequence == sequences[-1]
    assert await foundation.current_head() == sequences[-1]
    assert await foundation.register_instance("worker-new", lease_seconds=30) == sequences[-1]
    assert await foundation.read_events("worker-new") == []


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"max_events": 0, "max_age_seconds": 60}, "retention count"),
        ({"max_events": 1, "max_age_seconds": 59}, "retention age"),
        ({"max_events": 1, "max_age_seconds": 60, "limit": 0}, "prune limit"),
    ],
)
@pytest.mark.asyncio
async def test_event_pruning_configuration_is_bounded(
    foundation: PostgresFoundation,
    kwargs: dict[str, int],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        await foundation.prune_events(**kwargs)


@pytest.mark.asyncio
async def test_registration_starts_at_head_and_heartbeat_requires_live_lease(
    foundation: PostgresFoundation,
) -> None:
    async with foundation.transaction() as connection:
        sequence = await foundation.append_event(
            connection,
            room_id="room-a",
            event_type="room.updated",
            payload={"archived": False},
        )
    assert await foundation.register_instance("worker-late", lease_seconds=30) == sequence
    assert await foundation.read_events("worker-late") == []
    await foundation.heartbeat_instance("worker-late", lease_seconds=30)

    async with foundation.transaction() as connection:
        await connection.execute(
            """
            UPDATE public.samsarix_instance_cursors
            SET lease_expires_at = clock_timestamp() - interval '1 second'
            """
        )
    with pytest.raises(InstanceLeaseError, match="expired"):
        await foundation.heartbeat_instance("worker-late", lease_seconds=30)
    with pytest.raises(InstanceLeaseError, match="expired"):
        await foundation.read_events("worker-late")


@pytest.mark.asyncio
async def test_instance_claims_reject_duplicate_owners_and_fence_old_generations(
    foundation: PostgresFoundation,
) -> None:
    first = await foundation.claim_instance("exclusive-worker", lease_seconds=30)
    with pytest.raises(InstanceLeaseError, match="already active"):
        await foundation.claim_instance("exclusive-worker", lease_seconds=30)

    renewed = await foundation.claim_instance(
        "exclusive-worker",
        lease_seconds=30,
        generation=first.generation,
    )
    assert renewed == first
    assert await foundation.release_instance("exclusive-worker", generation=first.generation)

    replacement = await foundation.claim_instance("exclusive-worker", lease_seconds=30)
    assert replacement.generation != first.generation
    with pytest.raises(InstanceLeaseError, match="generation"):
        await foundation.heartbeat_claimed_instance(
            "exclusive-worker",
            generation=first.generation,
            lease_seconds=30,
        )
    with pytest.raises(InstanceLeaseError, match="expired"):
        await foundation.read_events(
            "exclusive-worker",
            generation=first.generation,
        )


@pytest.mark.asyncio
async def test_concurrent_initialization_is_serialized(clean_postgres_database: str) -> None:
    first = PostgresFoundation(clean_postgres_database)
    second = PostgresFoundation(clean_postgres_database)
    try:
        await asyncio.gather(first.open(), second.open())
        assert await asyncio.gather(first.schema_version(), second.schema_version()) == [
            POSTGRES_SCHEMA_VERSION,
            POSTGRES_SCHEMA_VERSION,
        ]
    finally:
        await asyncio.gather(first.close(), second.close())


@pytest.mark.asyncio
async def test_event_sequence_cannot_commit_out_of_order(foundation: PostgresFoundation) -> None:
    first_inserted = asyncio.Event()
    release_first = asyncio.Event()
    second_inserted = asyncio.Event()

    async def first_writer() -> int:
        async with foundation.transaction() as connection:
            sequence = await foundation.append_event(
                connection,
                room_id="room-a",
                event_type="message.created",
                payload={"writer": "first"},
            )
            first_inserted.set()
            await release_first.wait()
            return sequence

    async def second_writer() -> int:
        await first_inserted.wait()
        async with foundation.transaction() as connection:
            sequence = await foundation.append_event(
                connection,
                room_id="room-b",
                event_type="message.created",
                payload={"writer": "second"},
            )
            second_inserted.set()
            return sequence

    first_task = asyncio.create_task(first_writer())
    second_task = asyncio.create_task(second_writer())
    await first_inserted.wait()
    second_was_blocked = False
    try:
        await asyncio.wait_for(asyncio.shield(second_inserted.wait()), timeout=0.25)
    except TimeoutError:
        second_was_blocked = True
    finally:
        release_first.set()
    first_sequence, second_sequence = await asyncio.gather(first_task, second_task)
    assert second_was_blocked
    assert second_sequence == first_sequence + 1


@pytest.mark.asyncio
async def test_newer_schema_fails_closed(clean_postgres_database: str) -> None:
    async with await psycopg.AsyncConnection.connect(clean_postgres_database, autocommit=True) as connection:
        await connection.execute(
            """
            CREATE TABLE public.samsarix_schema_metadata (
                singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
                version INTEGER NOT NULL CHECK (version > 0),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
            )
            """
        )
        await connection.execute(
            "INSERT INTO public.samsarix_schema_metadata (singleton, version) VALUES (TRUE, %s)",
            (POSTGRES_SCHEMA_VERSION + 1,),
        )

    service = PostgresFoundation(clean_postgres_database)
    with pytest.raises(UnsupportedPostgresSchemaError, match="newer"):
        await service.open()
    await service.close()
    with pytest.raises(PostgresFoundationError, match="not open"):
        await service.current_head()
