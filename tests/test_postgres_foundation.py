# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Contract tests for the internal PostgreSQL multi-instance foundation."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest

psycopg = pytest.importorskip("psycopg")

from samsarix_chat_engine.postgres import (  # noqa: E402
    POSTGRES_SCHEMA_VERSION,
    InstanceLeaseError,
    InvalidRealtimeEventError,
    PostgresFoundation,
    PostgresFoundationError,
    UnsupportedPostgresSchemaError,
    _validate_event,
)


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
