# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Live tests for connection-bound PostgreSQL typing transitions."""

from __future__ import annotations

import pytest

pytest.importorskip("psycopg")

from samsarix_chat_engine.models import RoomCreate  # noqa: E402
from samsarix_chat_engine.postgres_connections import PostgresConnectionRegistry  # noqa: E402
from samsarix_chat_engine.postgres_store import PostgresChatStore  # noqa: E402
from samsarix_chat_engine.postgres_typing import PostgresTypingRegistry, TypingStateError  # noqa: E402

pytestmark = pytest.mark.postgres


def _store(conninfo: str) -> PostgresChatStore:
    return PostgresChatStore(
        conninfo,
        max_rooms=5,
        max_stored_messages=10,
        max_stored_messages_per_room=10,
    )


@pytest.mark.asyncio
async def test_typing_refresh_is_transition_only_and_stop_is_owner_bound(clean_postgres_database: str) -> None:
    store = _store(clean_postgres_database)
    await store.initialize()
    try:
        await store.create_room(RoomCreate(id="general", name="General"))
        await store.foundation.register_instance("node-a", lease_seconds=30)
        connections = PostgresConnectionRegistry(
            store.foundation,
            max_connections=5,
            max_connections_per_room=5,
        )
        assert await connections.try_acquire(
            connection_id="socket-one",
            instance_id="node-a",
            room_id="general",
            username="alice",
            subject="alice-subject",
        )
        await store.foundation.register_instance("observer", lease_seconds=30)
        typing = PostgresTypingRegistry(store.foundation, timeout_seconds=8)

        started = await typing.start(connection_id="socket-one", instance_id="node-a")
        assert started is not None and started.active and started.username == "alice"
        assert await typing.start(connection_id="socket-one", instance_id="node-a") is None
        assert await typing.stop(connection_id="socket-one", instance_id="other-node") is None
        stopped = await typing.stop(connection_id="socket-one", instance_id="node-a")
        assert stopped is not None and not stopped.active
        assert await typing.stop(connection_id="socket-one", instance_id="node-a") is None

        events = await store.foundation.read_events("observer")
        assert [event.event_type for event in events] == ["typing.started", "typing.stopped"]
        assert events[0].payload == {
            "type": "typing.started",
            "username": "alice",
            "expires_in": 8.0,
            "origin_connection_id": "socket-one",
        }
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_expiry_sweep_is_bounded_and_emits_stops(clean_postgres_database: str) -> None:
    store = _store(clean_postgres_database)
    await store.initialize()
    try:
        await store.create_room(RoomCreate(id="general", name="General"))
        await store.foundation.register_instance("node-a", lease_seconds=30)
        connections = PostgresConnectionRegistry(
            store.foundation,
            max_connections=5,
            max_connections_per_room=5,
        )
        typing = PostgresTypingRegistry(store.foundation, timeout_seconds=8)
        for index in range(2):
            assert await connections.try_acquire(
                connection_id=f"socket-{index}",
                instance_id="node-a",
                room_id="general",
                username=f"user-{index}",
                subject=None,
            )
        await store.foundation.register_instance("observer", lease_seconds=30)
        for index in range(2):
            assert await typing.start(connection_id=f"socket-{index}", instance_id="node-a")
        async with store.foundation.transaction() as connection:
            await connection.execute(
                """
                UPDATE public.samsarix_typing_states
                SET created_at = clock_timestamp() - interval '2 seconds',
                    expires_at = clock_timestamp() - interval '1 second'
                """
            )

        first = await typing.reap_expired(limit=1)
        second = await typing.reap_expired(limit=1)
        assert len(first) == len(second) == 1
        assert {first[0].connection_id, second[0].connection_id} == {"socket-0", "socket-1"}
        assert await typing.reap_expired() == []
        events = await store.foundation.read_events("observer")
        assert [event.event_type for event in events] == [
            "typing.started",
            "typing.started",
            "typing.stopped",
            "typing.stopped",
        ]
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_start_requires_a_live_owned_connection(clean_postgres_database: str) -> None:
    store = _store(clean_postgres_database)
    await store.initialize()
    try:
        await store.create_room(RoomCreate(id="general", name="General"))
        typing = PostgresTypingRegistry(store.foundation)
        with pytest.raises(TypingStateError, match="missing"):
            await typing.start(connection_id="socket-missing", instance_id="node-missing")
        with pytest.raises(ValueError, match="between 1 and 1000"):
            await typing.reap_expired(limit=0)
    finally:
        await store.close()


def test_typing_timeout_is_bounded() -> None:
    store = PostgresChatStore(
        "postgresql://unused",
        max_rooms=1,
        max_stored_messages=1,
        max_stored_messages_per_room=1,
    )
    with pytest.raises(ValueError, match="between 1 and 30"):
        PostgresTypingRegistry(store.foundation, timeout_seconds=31)
