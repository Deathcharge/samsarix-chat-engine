# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Live tests for lease-derived PostgreSQL presence convergence."""

from __future__ import annotations

import pytest

pytest.importorskip("psycopg")

from samsarix_chat_engine.models import RoomCreate  # noqa: E402
from samsarix_chat_engine.postgres_connections import ConnectionCounts, PostgresConnectionRegistry  # noqa: E402
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
async def test_join_and_owned_release_emit_exact_room_counts(clean_postgres_database: str) -> None:
    store = _store(clean_postgres_database)
    await store.initialize()
    try:
        await store.create_room(RoomCreate(id="general", name="General"))
        await store.foundation.register_instance("node-a", lease_seconds=30)
        await store.foundation.register_instance("node-b", lease_seconds=30)
        await store.foundation.register_instance("observer", lease_seconds=30)
        registry = PostgresConnectionRegistry(
            store.foundation,
            max_connections=5,
            max_connections_per_room=5,
        )
        first = await registry.try_acquire(
            connection_id="socket-a",
            instance_id="node-a",
            room_id="general",
            username="alice",
            subject="alice",
        )
        second = await registry.try_acquire(
            connection_id="socket-b",
            instance_id="node-b",
            room_id="general",
            username="bob",
            subject="bob",
        )
        assert not await registry.release(connection_id="socket-a", instance_id="node-b")
        assert await registry.release(connection_id="socket-a", instance_id="node-a")

        events = await store.foundation.read_events("observer")
        assert [event.event_type for event in events] == [
            "presence.joined",
            "presence.joined",
            "presence.left",
        ]
        assert [event.payload["active_connections"] for event in events] == [1, 2, 1]
        assert [event.payload["username"] for event in events] == ["alice", "bob", "alice"]
        assert events[0].payload["origin_connection_id"] == "socket-a"
        assert first is not None and first.admission_sequence == events[0].sequence
        assert second is not None and second.admission_sequence == events[1].sequence
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_same_id_restart_rotates_generation_and_cannot_revive_stale_socket(
    clean_postgres_database: str,
) -> None:
    store = _store(clean_postgres_database)
    await store.initialize()
    try:
        await store.create_room(RoomCreate(id="general", name="General"))
        await store.foundation.register_instance("stable-node", lease_seconds=30)
        registry = PostgresConnectionRegistry(
            store.foundation,
            max_connections=1,
            max_connections_per_room=1,
        )
        old = await registry.try_acquire(
            connection_id="socket-old",
            instance_id="stable-node",
            room_id="general",
            username="alice",
            subject="alice",
        )
        assert old is not None
        await store.foundation.register_instance("observer", lease_seconds=30)
        async with store.foundation.transaction() as connection:
            await connection.execute(
                """
                UPDATE public.samsarix_instance_cursors
                SET lease_expires_at = clock_timestamp() - interval '1 second'
                WHERE instance_id = 'stable-node'
                """
            )
        await store.foundation.register_instance("stable-node", lease_seconds=30)
        async with store.foundation.transaction() as connection:
            cursor = await connection.execute(
                "SELECT generation FROM public.samsarix_instance_cursors WHERE instance_id = 'stable-node'"
            )
            row = await cursor.fetchone()
        assert row is not None and row[0] != old.instance_generation
        assert await registry.counts(room_id="general") == ConnectionCounts(0, 0)
        with pytest.raises(TypingStateError, match="unavailable"):
            await PostgresTypingRegistry(store.foundation).start(
                connection_id="socket-old",
                instance_id="stable-node",
            )

        assert await registry.try_acquire(
            connection_id="socket-new",
            instance_id="stable-node",
            room_id="general",
            username="alice",
            subject="alice",
        )
        events = await store.foundation.read_events("observer")
        assert [event.event_type for event in events] == ["presence.left", "presence.joined"]
        assert [event.payload["active_connections"] for event in events] == [0, 1]
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_crash_sweep_is_bounded_and_stops_typing_before_presence(clean_postgres_database: str) -> None:
    store = _store(clean_postgres_database)
    await store.initialize()
    try:
        await store.create_room(RoomCreate(id="general", name="General"))
        await store.foundation.register_instance("node-a", lease_seconds=30)
        registry = PostgresConnectionRegistry(
            store.foundation,
            max_connections=5,
            max_connections_per_room=5,
        )
        typing = PostgresTypingRegistry(store.foundation)
        for index in range(2):
            assert await registry.try_acquire(
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
                UPDATE public.samsarix_instance_cursors
                SET lease_expires_at = clock_timestamp() - interval '1 second'
                WHERE instance_id = 'node-a'
                """
            )

        transitions = await registry.reap_expired(limit=10)
        assert [transition.connection_id for transition in transitions] == ["socket-0", "socket-1"]
        assert await registry.reap_expired() == []
        events = await store.foundation.read_events("observer")
        assert [event.event_type for event in events] == [
            "typing.started",
            "typing.started",
            "typing.stopped",
            "presence.left",
            "typing.stopped",
            "presence.left",
        ]
        left_counts = [
            event.payload.get("active_connections") for event in events if event.event_type == "presence.left"
        ]
        assert left_counts == [0, 0]
        with pytest.raises(ValueError, match="between 1 and 1000"):
            await registry.reap_expired(limit=0)
    finally:
        await store.close()
