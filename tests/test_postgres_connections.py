# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Live tests for PostgreSQL-owned, crash-reclaimable connection capacity."""

from __future__ import annotations

import asyncio

import pytest

pytest.importorskip("psycopg")

from samsarix_chat_engine.models import RoomCreate  # noqa: E402
from samsarix_chat_engine.postgres import InstanceLeaseError, PostgresFoundation  # noqa: E402
from samsarix_chat_engine.postgres_connections import (  # noqa: E402
    ConnectionCounts,
    ConnectionLeaseError,
    ConnectionRoomUnavailableError,
    PostgresConnectionRegistry,
)
from samsarix_chat_engine.postgres_store import PostgresChatStore  # noqa: E402

pytestmark = pytest.mark.postgres


def _store(conninfo: str) -> PostgresChatStore:
    return PostgresChatStore(
        conninfo,
        max_rooms=10,
        max_stored_messages=20,
        max_stored_messages_per_room=10,
    )


@pytest.mark.asyncio
async def test_concurrent_registries_enforce_exact_global_capacity(clean_postgres_database: str) -> None:
    first = _store(clean_postgres_database)
    second_foundation = PostgresFoundation(clean_postgres_database)
    await first.initialize()
    await second_foundation.open()
    try:
        await first.create_room(RoomCreate(id="alpha", name="Alpha"))
        await first.create_room(RoomCreate(id="beta", name="Beta"))
        await first.foundation.register_instance("node-a", lease_seconds=30)
        await second_foundation.register_instance("node-b", lease_seconds=30)
        first_registry = PostgresConnectionRegistry(
            first.foundation,
            max_connections=4,
            max_connections_per_room=3,
        )
        second_registry = PostgresConnectionRegistry(
            second_foundation,
            max_connections=4,
            max_connections_per_room=3,
        )

        acquisitions = await asyncio.gather(
            *(
                (first_registry if index % 2 == 0 else second_registry).try_acquire(
                    connection_id=f"socket-{index}",
                    instance_id="node-a" if index % 2 == 0 else "node-b",
                    room_id="alpha" if index % 3 else "beta",
                    username=f"user-{index}",
                    subject=f"subject-{index}",
                )
                for index in range(12)
            )
        )

        assert sum(lease is not None for lease in acquisitions) == 4
        assert await first_registry.counts(room_id="alpha") == await second_registry.counts(room_id="alpha")
        assert (await first_registry.counts(room_id="alpha")).total == 4
    finally:
        await asyncio.gather(first.close(), second_foundation.close())


@pytest.mark.asyncio
async def test_concurrent_registries_enforce_exact_per_room_capacity(clean_postgres_database: str) -> None:
    store = _store(clean_postgres_database)
    await store.initialize()
    try:
        await store.create_room(RoomCreate(id="general", name="General"))
        await store.foundation.register_instance("node-a", lease_seconds=30)
        registry = PostgresConnectionRegistry(
            store.foundation,
            max_connections=10,
            max_connections_per_room=2,
        )

        acquisitions = await asyncio.gather(
            *(
                registry.try_acquire(
                    connection_id=f"room-socket-{index}",
                    instance_id="node-a",
                    room_id="general",
                    username=f"user-{index}",
                    subject=None,
                )
                for index in range(8)
            )
        )

        assert sum(lease is not None for lease in acquisitions) == 2
        assert (await registry.counts(room_id="general")).room == 2
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_renew_release_and_room_lifecycle_fail_closed(clean_postgres_database: str) -> None:
    store = _store(clean_postgres_database)
    await store.initialize()
    try:
        await store.create_room(RoomCreate(id="general", name="General"))
        await store.foundation.register_instance("node-a", lease_seconds=30)
        await store.foundation.register_instance("node-b", lease_seconds=30)
        registry = PostgresConnectionRegistry(
            store.foundation,
            max_connections=5,
            max_connections_per_room=5,
        )
        lease = await registry.try_acquire(
            connection_id="socket-one",
            instance_id="node-a",
            room_id="general",
            username="Andrew",
            subject="user-1",
        )
        assert lease is not None
        assert await registry.release(connection_id="socket-one", instance_id="node-b") is False
        assert (
            await registry.renew(connection_id="socket-one", instance_id="node-a", room_id="general")
            > lease.lease_expires_at
        )

        await store.set_room_state("general", archived=True, frozen=None, actor="operator")
        with pytest.raises(ConnectionRoomUnavailableError, match="archived") as archived:
            await registry.renew(connection_id="socket-one", instance_id="node-a", room_id="general")
        assert archived.value.room == await store.get_room("general")
        assert await registry.counts(room_id="general") == ConnectionCounts(0, 0)
        assert [item.connection_id for item in await registry.reap_expired()] == ["socket-one"]
        with pytest.raises(ConnectionRoomUnavailableError, match="archived"):
            await registry.try_acquire(
                connection_id="socket-two",
                instance_id="node-a",
                room_id="general",
                username="Andrew",
                subject=None,
            )
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_expired_socket_and_crashed_instance_occupancy_are_reclaimed(
    clean_postgres_database: str,
) -> None:
    store = _store(clean_postgres_database)
    await store.initialize()
    try:
        await store.create_room(RoomCreate(id="general", name="General"))
        await store.foundation.register_instance("node-a", lease_seconds=30)
        registry = PostgresConnectionRegistry(
            store.foundation,
            max_connections=1,
            max_connections_per_room=1,
        )
        assert await registry.try_acquire(
            connection_id="socket-old",
            instance_id="node-a",
            room_id="general",
            username="Andrew",
            subject=None,
        )
        async with store.foundation.transaction() as connection:
            await connection.execute(
                """
                UPDATE public.samsarix_connection_leases
                SET created_at = clock_timestamp() - interval '2 seconds',
                    lease_expires_at = clock_timestamp() - interval '1 second'
                WHERE connection_id = 'socket-old'
                """
            )
        assert await registry.try_acquire(
            connection_id="socket-new",
            instance_id="node-a",
            room_id="general",
            username="Andrew",
            subject=None,
        )

        async with store.foundation.transaction() as connection:
            await connection.execute(
                """
                UPDATE public.samsarix_instance_cursors
                SET lease_expires_at = clock_timestamp() - interval '1 second'
                WHERE instance_id = 'node-a'
                """
            )
        assert await registry.counts(room_id="general") == ConnectionCounts(0, 0)
        with pytest.raises(InstanceLeaseError, match="expired"):
            await registry.try_acquire(
                connection_id="socket-blocked",
                instance_id="node-a",
                room_id="general",
                username="Andrew",
                subject=None,
            )

        await store.foundation.register_instance("node-a", lease_seconds=30)
        assert await registry.try_acquire(
            connection_id="socket-after-restart",
            instance_id="node-a",
            room_id="general",
            username="Andrew",
            subject=None,
        )
        assert await registry.release(connection_id="socket-after-restart", instance_id="node-a")
        assert not await registry.release(connection_id="socket-after-restart", instance_id="node-a")
    finally:
        await store.close()


def test_registry_rejects_unsafe_configuration_and_identifiers() -> None:
    foundation = PostgresFoundation("postgresql://unused")
    with pytest.raises(ValueError, match="cannot exceed"):
        PostgresConnectionRegistry(foundation, max_connections=2, max_connections_per_room=3)
    with pytest.raises(ValueError, match="between 3 and 300"):
        PostgresConnectionRegistry(
            foundation,
            max_connections=2,
            max_connections_per_room=2,
            lease_seconds=2,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["archived", "reaped", "deleted", "reopened", "expired", "wrong_room"])
async def test_renewal_diagnoses_current_room_even_after_lease_cleanup(clean_postgres_database, state):
    store = _store(clean_postgres_database)
    await store.initialize()
    try:
        await store.create_room(RoomCreate(id="general", name="General"))
        await store.create_room(RoomCreate(id="other", name="Other"))
        await store.foundation.register_instance("node-a", lease_seconds=30)
        registry = PostgresConnectionRegistry(store.foundation, max_connections=5, max_connections_per_room=5)
        await registry.try_acquire(
            connection_id="socket", instance_id="node-a", room_id="general", username="A", subject=None
        )
        if state in {"archived", "reaped", "deleted", "reopened"}:
            await store.set_room_state("general", archived=True, frozen=None, actor="operator")
        if state in {"reaped", "reopened"}:
            assert len(await registry.reap_expired()) == 1
        if state == "deleted":
            await store.delete_room("general", actor="operator")
        if state == "reopened":
            await store.set_room_state("general", archived=False, frozen=None, actor="operator")
        if state == "expired":
            async with store.foundation.transaction() as connection:
                await connection.execute(
                    "UPDATE public.samsarix_connection_leases "
                    "SET created_at = clock_timestamp() - interval '2 seconds', "
                    "lease_expires_at = clock_timestamp() - interval '1 second'"
                )
        room_id = "other" if state == "wrong_room" else "general"
        with pytest.raises(ConnectionLeaseError) as failure:
            await registry.renew(connection_id="socket", instance_id="node-a", room_id=room_id)
        if state in {"archived", "reaped", "deleted"}:
            assert isinstance(failure.value, ConnectionRoomUnavailableError)
            assert failure.value.room == await store.get_room("general")
            if state != "deleted":
                assert failure.value.room.archived_at is not None
        else:
            assert not isinstance(failure.value, ConnectionRoomUnavailableError)
        if state == "wrong_room":
            # Supplying a different active room must not renew someone else's lease.
            assert await registry.renew(connection_id="socket", instance_id="node-a", room_id="general")
    finally:
        await store.close()
