# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Live parity tests for the internal PostgreSQL room and message store."""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator

import pytest

psycopg = pytest.importorskip("psycopg")

from samsarix_chat_engine.models import MemberModerationUpdate, Room, RoomCreate  # noqa: E402
from samsarix_chat_engine.postgres import POSTGRES_SCHEMA_VERSION  # noqa: E402
from samsarix_chat_engine.postgres_store import PostgresChatStore  # noqa: E402
from samsarix_chat_engine.store import (  # noqa: E402
    InvalidCursorError,
    MemberBannedError,
    MemberMutedError,
    MessageOwnershipError,
    RoomArchivedError,
    RoomCapacityError,
    RoomNotArchivedError,
)

TEST_POSTGRES_URL = os.getenv("SAMSARIX_TEST_POSTGRES_URL")


def _store(conninfo: str, **overrides: int) -> PostgresChatStore:
    return PostgresChatStore(
        conninfo,
        max_rooms=overrides.get("max_rooms", 10),
        max_stored_messages=overrides.get("max_stored_messages", 20),
        max_stored_messages_per_room=overrides.get("max_stored_messages_per_room", 10),
        max_audit_events=overrides.get("max_audit_events", 50),
    )


@pytest.fixture
async def clean_test_database() -> AsyncIterator[str]:
    if TEST_POSTGRES_URL is None:
        pytest.skip("SAMSARIX_TEST_POSTGRES_URL is not configured")
    await _reset_test_database(TEST_POSTGRES_URL)
    try:
        yield TEST_POSTGRES_URL
    finally:
        await _reset_test_database(TEST_POSTGRES_URL)


@pytest.fixture
async def store(clean_test_database: str) -> AsyncIterator[PostgresChatStore]:
    service = _store(clean_test_database)
    await service.initialize()
    try:
        yield service
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_schema_v1_migrates_transactionally(clean_test_database: str) -> None:
    async with await psycopg.AsyncConnection.connect(clean_test_database, autocommit=True) as connection:
        await connection.execute(
            """
            CREATE TABLE public.samsarix_schema_metadata (
                singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
                version INTEGER NOT NULL CHECK (version > 0),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
            )
            """
        )
        await connection.execute("INSERT INTO public.samsarix_schema_metadata (singleton, version) VALUES (TRUE, 1)")

    service = _store(clean_test_database)
    await service.initialize()
    try:
        assert await service.foundation.schema_version() == POSTGRES_SCHEMA_VERSION == 2
        assert await service.check_ready()
        assert await service.list_rooms() == []
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_room_lifecycle_audit_and_events_are_atomic(store: PostgresChatStore) -> None:
    await store.foundation.register_instance("room-observer", lease_seconds=30)
    room = await store.create_room(RoomCreate(id="general", name="General"), actor="operator")
    assert room.id == "general"
    assert await store.get_room("general") == room
    assert await store.list_rooms() == [room]

    frozen, changes = await store.set_room_state(
        "general",
        archived=None,
        frozen=True,
        actor="operator",
    )
    assert changes == frozenset({"frozen"})
    assert frozen.frozen_at is not None
    unchanged, changes = await store.set_room_state(
        "general",
        archived=None,
        frozen=True,
        actor="operator",
    )
    assert unchanged == frozen
    assert changes == frozenset()

    events = await store.foundation.read_events("room-observer")
    assert [event.event_type for event in events] == ["room.created", "room.frozen"]
    audits, cursor = await store.list_audit_events()
    assert cursor is None
    assert [event.action for event in audits] == ["room.created", "room.frozen"]


@pytest.mark.asyncio
async def test_room_capacity_is_global_across_store_instances(clean_test_database: str) -> None:
    first = _store(clean_test_database, max_rooms=1)
    second = _store(clean_test_database, max_rooms=1)
    await asyncio.gather(first.initialize(), second.initialize())
    try:
        results = await asyncio.gather(
            first.create_room(RoomCreate(id="one", name="One")),
            second.create_room(RoomCreate(id="two", name="Two")),
            return_exceptions=True,
        )
        assert len([result for result in results if isinstance(result, Room)]) == 1
        assert len([result for result in results if isinstance(result, RoomCapacityError)]) == 1
        assert len(await first.list_rooms()) == 1
    finally:
        await asyncio.gather(first.close(), second.close())


@pytest.mark.asyncio
async def test_concurrent_message_idempotency_and_event_delivery(clean_test_database: str) -> None:
    first = _store(clean_test_database)
    second = _store(clean_test_database)
    await asyncio.gather(first.initialize(), second.initialize())
    try:
        await first.create_room(RoomCreate(id="general", name="General"))
        await first.foundation.register_instance("message-observer", lease_seconds=30)
        results = await asyncio.gather(
            first.create_message(
                room_id="general",
                sender="alice",
                content="hello",
                client_message_id="client-1",
                allow_frozen=False,
                member_subject="alice",
                author_subject="alice",
            ),
            second.create_message(
                room_id="general",
                sender="alice",
                content="hello",
                client_message_id="client-1",
                allow_frozen=False,
                member_subject="alice",
                author_subject="alice",
            ),
        )
        assert results[0][0].id == results[1][0].id
        assert sorted(result[1] for result in results) == [False, True]
        events = await first.foundation.read_events("message-observer")
        assert [event.event_type for event in events] == ["message.created"]
        assert events[0].payload["message"]["content"] == "hello"
    finally:
        await asyncio.gather(first.close(), second.close())


@pytest.mark.asyncio
async def test_message_mutation_search_pagination_and_retention(clean_test_database: str) -> None:
    service = _store(
        clean_test_database,
        max_stored_messages=2,
        max_stored_messages_per_room=2,
    )
    await service.initialize()
    try:
        await service.create_room(RoomCreate(id="general", name="General"))
        await service.foundation.register_instance("privacy-observer", lease_seconds=30)
        first, _ = await service.create_message(
            room_id="general",
            sender="alice",
            content="Straße first",
            client_message_id=None,
            allow_frozen=False,
            author_subject="alice",
        )
        second, _ = await service.create_message(
            room_id="general",
            sender="alice",
            content="second",
            client_message_id=None,
            allow_frozen=False,
            author_subject="alice",
        )
        third, _ = await service.create_message(
            room_id="general",
            sender="alice",
            content="third",
            client_message_id=None,
            allow_frozen=False,
            author_subject="alice",
        )
        messages, next_before = await service.list_messages("general", limit=1)
        assert [message.id for message in messages] == [third.id]
        assert next_before == third.id
        older, no_more = await service.list_messages("general", limit=2, before=next_before)
        assert [message.id for message in older] == [second.id]
        assert no_more is None
        with pytest.raises(InvalidCursorError):
            await service.list_messages("general", before=first.id)

        updated = await service.update_message(
            room_id="general",
            message_id=second.id,
            actor="alice",
            content="STRASSE revised",
            is_admin=False,
        )
        assert updated.edited_at is not None
        found, _ = await service.search_messages("general", "straße")
        assert [message.id for message in found] == [second.id]
        with pytest.raises(MessageOwnershipError):
            await service.update_message(
                room_id="general",
                message_id=second.id,
                actor="bob",
                content="unauthorized",
                is_admin=False,
            )
        deleted, changed = await service.delete_message(
            room_id="general",
            message_id=second.id,
            actor="alice",
            is_admin=False,
        )
        assert changed and deleted.content == "" and deleted.deleted_at is not None
        assert await service.search_messages("general", "revised") == ([], None)
        events = await service.foundation.read_events("privacy-observer")
        second_events = [event for event in events if event.payload.get("message", {}).get("id") == second.id]
        assert [event.event_type for event in second_events] == [
            "message.created",
            "message.updated",
            "message.deleted",
        ]
        assert all(event.payload["message"]["content"] == "" for event in second_events)
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_moderation_and_archived_room_rules(store: PostgresChatStore) -> None:
    await store.create_room(RoomCreate(id="general", name="General"))
    banned = await store.set_member_moderation(
        "general",
        "alice",
        MemberModerationUpdate(banned_for_seconds=60),
        actor="operator",
    )
    assert banned.banned_until is not None
    assert await store.get_member_moderation("general", "alice") == banned
    with pytest.raises(MemberBannedError):
        await store.create_message(
            room_id="general",
            sender="alice",
            content="blocked",
            client_message_id=None,
            allow_frozen=False,
            member_subject="alice",
        )
    await store.set_member_moderation(
        "general",
        "alice",
        MemberModerationUpdate(banned_for_seconds=0, muted_for_seconds=60),
        actor="operator",
    )
    with pytest.raises(MemberMutedError):
        await store.create_message(
            room_id="general",
            sender="alice",
            content="muted",
            client_message_id=None,
            allow_frozen=False,
            member_subject="alice",
        )
    await store.set_member_moderation(
        "general",
        "alice",
        MemberModerationUpdate(muted_for_seconds=0),
        actor="operator",
    )
    message, _ = await store.create_message(
        room_id="general",
        sender="alice",
        content="allowed",
        client_message_id=None,
        allow_frozen=False,
        member_subject="alice",
    )
    with pytest.raises(RoomNotArchivedError):
        await store.delete_room("general", actor="operator")
    await store.set_room_state("general", archived=True, frozen=None, actor="operator")
    with pytest.raises(RoomArchivedError):
        await store.create_message(
            room_id="general",
            sender="alice",
            content="archived",
            client_message_id=None,
            allow_frozen=False,
        )
    assert await store.delete_room("general", actor="operator") == 1
    assert await store.get_room("general") is None
    assert message.id


@pytest.mark.asyncio
async def test_event_failure_rolls_back_domain_and_audit_rows(
    store: PostgresChatStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fail_event(*_args: object, **_kwargs: object) -> int:
        raise RuntimeError("event append failed")

    monkeypatch.setattr(store.foundation, "append_event", fail_event)
    with pytest.raises(RuntimeError, match="event append failed"):
        await store.create_room(RoomCreate(id="rollback", name="Rollback"), actor="operator")
    assert await store.get_room("rollback") is None
    assert await store.list_audit_events() == ([], None)


async def _reset_test_database(conninfo: str) -> None:
    async with await psycopg.AsyncConnection.connect(conninfo, autocommit=True) as connection:
        cursor = await connection.execute("SELECT current_database()")
        row = await cursor.fetchone()
        if row is None or row[0] != "samsarix_test":
            raise RuntimeError("live PostgreSQL tests require the dedicated samsarix_test database")
        await connection.execute("DROP TABLE IF EXISTS public.samsarix_instance_cursors")
        await connection.execute("DROP TABLE IF EXISTS public.samsarix_realtime_events")
        await connection.execute("DROP TABLE IF EXISTS public.samsarix_room_member_controls")
        await connection.execute("DROP TABLE IF EXISTS public.samsarix_messages")
        await connection.execute("DROP TABLE IF EXISTS public.samsarix_rooms")
        await connection.execute("DROP TABLE IF EXISTS public.samsarix_audit_events")
        await connection.execute("DROP TABLE IF EXISTS public.samsarix_schema_metadata")
