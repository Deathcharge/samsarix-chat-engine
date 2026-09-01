# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Live parity tests for the internal PostgreSQL room and message store."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

psycopg = pytest.importorskip("psycopg")

from samsarix_chat_engine.models import AttachmentReference, MemberModerationUpdate, Room, RoomCreate  # noqa: E402
from samsarix_chat_engine.postgres import POSTGRES_SCHEMA_VERSION  # noqa: E402
from samsarix_chat_engine.postgres_store import PostgresChatStore  # noqa: E402
from samsarix_chat_engine.store import (  # noqa: E402
    InvalidCursorError,
    MemberBannedError,
    MemberMutedError,
    MessageDeletedError,
    MessageNotFoundError,
    MessageOwnershipError,
    ReactionCapacityError,
    ReadStateCapacityError,
    RetentionNotConfiguredError,
    RoomArchivedError,
    RoomCapacityError,
    RoomNotArchivedError,
    RoomNotFoundError,
    ThreadDepthError,
    WebhookCapacityError,
    WebhookDeliveryNotFoundError,
    WebhookPayloadUnavailableError,
)

pytestmark = pytest.mark.postgres


def _store(conninfo: str, **overrides: Any) -> PostgresChatStore:
    return PostgresChatStore(
        conninfo,
        max_rooms=overrides.get("max_rooms", 10),
        max_stored_messages=overrides.get("max_stored_messages", 20),
        max_stored_messages_per_room=overrides.get("max_stored_messages_per_room", 10),
        max_read_states_per_room=overrides.get("max_read_states_per_room", 10),
        message_retention_days=overrides.get("message_retention_days"),
        max_audit_events=overrides.get("max_audit_events", 50),
        webhook_events=overrides.get("webhook_events", ()),
        max_webhook_deliveries=overrides.get("max_webhook_deliveries", 50),
        webhook_worker_id=overrides.get("webhook_worker_id"),
        webhook_lease_seconds=overrides.get("webhook_lease_seconds", 60),
    )


@pytest.fixture
async def store(clean_postgres_database: str) -> AsyncIterator[PostgresChatStore]:
    service = _store(clean_postgres_database)
    await service.initialize()
    try:
        yield service
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_schema_v2_migrates_transactionally_and_widens_event_payloads(
    clean_postgres_database: str,
) -> None:
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
        await connection.execute("INSERT INTO public.samsarix_schema_metadata (singleton, version) VALUES (TRUE, 2)")
        await connection.execute(
            """
            CREATE TABLE public.samsarix_realtime_events (
                sequence BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                room_id TEXT NOT NULL CHECK (
                    char_length(room_id) BETWEEN 1 AND 64
                    AND room_id ~ '^[a-z0-9][a-z0-9_-]*$'
                ),
                event_type TEXT NOT NULL CHECK (
                    char_length(event_type) BETWEEN 1 AND 80
                    AND event_type ~ '^[a-z][a-z0-9_.-]*$'
                ),
                payload JSONB NOT NULL CHECK (octet_length(payload::text) <= 262144),
                created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
            )
            """
        )

    service = _store(clean_postgres_database)
    await service.initialize()
    try:
        assert await service.foundation.schema_version() == POSTGRES_SCHEMA_VERSION == 14
        assert await service.check_ready()
        assert await service.list_rooms() == []
        async with service.foundation.transaction() as connection:
            await service.foundation.append_event(
                connection,
                room_id="migration",
                event_type="message.created",
                payload={"content": "x" * (300 * 1024)},
            )
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_schema_v6_backfills_matching_instance_generations(
    clean_postgres_database: str,
) -> None:
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
        await connection.execute("INSERT INTO public.samsarix_schema_metadata (singleton, version) VALUES (TRUE, 6)")
        await connection.execute(
            """
            CREATE TABLE public.samsarix_instance_cursors (
                instance_id TEXT PRIMARY KEY,
                last_sequence BIGINT NOT NULL CHECK (last_sequence >= 0),
                lease_expires_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
            )
            """
        )
        await connection.execute(
            """
            CREATE TABLE public.samsarix_rooms (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                archived_at TIMESTAMPTZ,
                frozen_at TIMESTAMPTZ
            )
            """
        )
        await connection.execute(
            """
            CREATE TABLE public.samsarix_connection_leases (
                connection_id TEXT PRIMARY KEY,
                instance_id TEXT NOT NULL REFERENCES public.samsarix_instance_cursors(instance_id)
                    ON DELETE CASCADE,
                room_id TEXT NOT NULL REFERENCES public.samsarix_rooms(id) ON DELETE CASCADE,
                username TEXT NOT NULL,
                subject TEXT,
                lease_expires_at TIMESTAMPTZ NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                CHECK (lease_expires_at > created_at)
            )
            """
        )
        await connection.execute(
            """
            CREATE INDEX samsarix_connection_leases_instance
            ON public.samsarix_connection_leases (instance_id, lease_expires_at, connection_id)
            """
        )
        await connection.execute(
            """
            INSERT INTO public.samsarix_instance_cursors (
                instance_id, last_sequence, lease_expires_at
            ) VALUES ('legacy-node', 0, clock_timestamp() + interval '30 seconds')
            """
        )
        await connection.execute("INSERT INTO public.samsarix_rooms (id, name) VALUES ('general', 'General')")
        await connection.execute(
            """
            INSERT INTO public.samsarix_connection_leases (
                connection_id, instance_id, room_id, username, subject, lease_expires_at
            ) VALUES (
                'legacy-socket', 'legacy-node', 'general', 'alice', 'alice',
                clock_timestamp() + interval '30 seconds'
            )
            """
        )

    service = _store(clean_postgres_database)
    await service.initialize()
    try:
        assert await service.foundation.schema_version() == POSTGRES_SCHEMA_VERSION == 14
        async with service.foundation.transaction() as connection:
            cursor = await connection.execute(
                """
                SELECT owner.generation, lease.instance_generation
                FROM public.samsarix_connection_leases AS lease
                JOIN public.samsarix_instance_cursors AS owner
                  ON owner.instance_id = lease.instance_id
                WHERE lease.connection_id = 'legacy-socket'
                """
            )
            generations = await cursor.fetchone()
            cursor = await connection.execute(
                """
                SELECT indexname
                FROM pg_indexes
                WHERE schemaname = 'public'
                  AND indexname IN (
                      'samsarix_connection_leases_instance',
                      'samsarix_connection_leases_instance_generation'
                  )
                ORDER BY indexname
                """
            )
            lease_indexes = [str(row[0]) for row in await cursor.fetchall()]
        assert generations is not None
        assert generations[0] is not None and generations[0] == generations[1]
        assert lease_indexes == ["samsarix_connection_leases_instance_generation"]
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_schema_v8_adds_thread_parent_column_and_index(clean_postgres_database: str) -> None:
    initial = _store(clean_postgres_database)
    await initial.initialize()
    await initial.close()
    async with await psycopg.AsyncConnection.connect(clean_postgres_database, autocommit=True) as connection:
        await connection.execute("DROP INDEX public.samsarix_messages_thread_order")
        await connection.execute("ALTER TABLE public.samsarix_messages DROP COLUMN parent_message_id")
        await connection.execute("UPDATE public.samsarix_schema_metadata SET version = 8")

    migrated = _store(clean_postgres_database)
    await migrated.initialize()
    try:
        assert await migrated.foundation.schema_version() == POSTGRES_SCHEMA_VERSION == 14
        async with migrated.foundation.transaction() as connection:
            cursor = await connection.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'samsarix_messages'
                  AND column_name = 'parent_message_id'
                """
            )
            assert await cursor.fetchone() == ("parent_message_id",)
            cursor = await connection.execute(
                """
                SELECT indexname FROM pg_indexes
                WHERE schemaname = 'public' AND indexname = 'samsarix_messages_thread_order'
                """
            )
            assert await cursor.fetchone() == ("samsarix_messages_thread_order",)
    finally:
        await migrated.close()


@pytest.mark.asyncio
async def test_schema_v9_adds_reaction_storage(clean_postgres_database: str) -> None:
    initial = _store(clean_postgres_database)
    await initial.initialize()
    await initial.close()
    async with await psycopg.AsyncConnection.connect(clean_postgres_database, autocommit=True) as connection:
        await connection.execute("DROP TABLE public.samsarix_message_reactions")
        await connection.execute("ALTER TABLE public.samsarix_messages DROP COLUMN reaction_summaries")
        await connection.execute("UPDATE public.samsarix_schema_metadata SET version = 9")

    migrated = _store(clean_postgres_database)
    await migrated.initialize()
    try:
        assert await migrated.foundation.schema_version() == POSTGRES_SCHEMA_VERSION == 14
        async with migrated.foundation.transaction() as connection:
            cursor = await connection.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'samsarix_messages'
                  AND column_name = 'reaction_summaries'
                """
            )
            assert await cursor.fetchone() == ("reaction_summaries",)
            cursor = await connection.execute("SELECT to_regclass('public.samsarix_message_reactions')")
            assert await cursor.fetchone() == ("samsarix_message_reactions",)
    finally:
        await migrated.close()


@pytest.mark.asyncio
async def test_schema_v11_adds_empty_application_metadata(clean_postgres_database: str) -> None:
    initial = _store(clean_postgres_database)
    await initial.initialize()
    await initial.close()
    async with await psycopg.AsyncConnection.connect(clean_postgres_database, autocommit=True) as connection:
        await connection.execute("ALTER TABLE public.samsarix_messages DROP COLUMN application_metadata")
        await connection.execute("UPDATE public.samsarix_schema_metadata SET version = 11")

    migrated = _store(clean_postgres_database)
    await migrated.initialize()
    try:
        assert await migrated.foundation.schema_version() == POSTGRES_SCHEMA_VERSION == 14
        async with migrated.foundation.transaction() as connection:
            cursor = await connection.execute(
                """
                SELECT column_default, is_nullable FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'samsarix_messages'
                  AND column_name = 'application_metadata'
                """
            )
            row = await cursor.fetchone()
        assert row is not None and "{}" in str(row[0]) and row[1] == "NO"
    finally:
        await migrated.close()


@pytest.mark.asyncio
async def test_schema_v12_adds_empty_attachment_references(clean_postgres_database: str) -> None:
    initial = _store(clean_postgres_database)
    await initial.initialize()
    await initial.close()
    async with await psycopg.AsyncConnection.connect(clean_postgres_database, autocommit=True) as connection:
        await connection.execute("ALTER TABLE public.samsarix_messages DROP COLUMN attachment_references")
        await connection.execute("UPDATE public.samsarix_schema_metadata SET version = 12")

    migrated = _store(clean_postgres_database)
    await migrated.initialize()
    try:
        assert await migrated.foundation.schema_version() == POSTGRES_SCHEMA_VERSION == 14
        async with migrated.foundation.transaction() as connection:
            cursor = await connection.execute(
                """
                SELECT column_default, is_nullable FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'samsarix_messages'
                  AND column_name = 'attachment_references'
                """
            )
            row = await cursor.fetchone()
        assert row is not None and "[]" in str(row[0]) and row[1] == "NO"
    finally:
        await migrated.close()


@pytest.mark.asyncio
async def test_attachment_references_have_postgres_parity_and_are_scrubbed(store: PostgresChatStore) -> None:
    await store.create_room(RoomCreate(id="general", name="General"))
    await store.foundation.register_instance("attachment-observer", lease_seconds=30)
    attachment = AttachmentReference(
        id="upload:SUP-42:trace",
        name="trace.json",
        media_type="application/json",
        size_bytes=256,
        sha256="b" * 64,
    )
    created, was_created = await store.create_message(
        room_id="general",
        sender="alice",
        content="",
        attachments=[attachment],
        client_message_id="attachment-1",
        allow_frozen=False,
        member_subject="alice",
        author_subject="alice",
    )
    replay, replay_created = await store.create_message(
        room_id="general",
        sender="alice",
        content="ignored",
        attachments=[],
        client_message_id="attachment-1",
        allow_frozen=False,
        member_subject="alice",
        author_subject="alice",
    )
    deleted, changed = await store.delete_message(
        room_id="general",
        message_id=created.id,
        actor="alice",
        is_admin=False,
        member_subject="alice",
    )
    events = await store.foundation.read_events("attachment-observer")

    assert was_created and not replay_created and replay == created
    assert created.content == "" and created.attachments == [attachment]
    assert changed and deleted.attachments == []
    assert events and all(event.payload["message"]["attachments"] == [] for event in events)


@pytest.mark.asyncio
async def test_schema_v13_adds_empty_mentioned_subjects(clean_postgres_database: str) -> None:
    initial = _store(clean_postgres_database)
    await initial.initialize()
    await initial.close()
    async with await psycopg.AsyncConnection.connect(clean_postgres_database, autocommit=True) as connection:
        await connection.execute("ALTER TABLE public.samsarix_messages DROP COLUMN mentioned_subjects")
        await connection.execute("UPDATE public.samsarix_schema_metadata SET version = 13")

    migrated = _store(clean_postgres_database)
    await migrated.initialize()
    try:
        assert await migrated.foundation.schema_version() == POSTGRES_SCHEMA_VERSION == 14
        async with migrated.foundation.transaction() as connection:
            cursor = await connection.execute(
                """
                SELECT column_default, is_nullable FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'samsarix_messages'
                  AND column_name = 'mentioned_subjects'
                """
            )
            row = await cursor.fetchone()
        assert row is not None and "[]" in str(row[0]) and row[1] == "NO"
    finally:
        await migrated.close()


@pytest.mark.asyncio
async def test_message_mentions_have_postgres_parity_and_are_scrubbed(store: PostgresChatStore) -> None:
    await store.create_room(RoomCreate(id="general", name="General"))
    await store.foundation.register_instance("mention-observer", lease_seconds=30)
    created, was_created = await store.create_message(
        room_id="general",
        sender="alice",
        content="Escalating",
        mentioned_subjects=["oncall", "lead"],
        client_message_id="mention-1",
        allow_frozen=False,
        member_subject="alice",
        author_subject="alice",
    )
    replay, replay_created = await store.create_message(
        room_id="general",
        sender="alice",
        content="ignored",
        mentioned_subjects=["other"],
        client_message_id="mention-1",
        allow_frozen=False,
        member_subject="alice",
        author_subject="alice",
    )
    updated = await store.update_message(
        room_id="general",
        message_id=created.id,
        actor="alice",
        content="Manager engaged",
        mentioned_subjects=["manager"],
        is_admin=False,
        member_subject="alice",
    )
    deleted, changed = await store.delete_message(
        room_id="general",
        message_id=created.id,
        actor="alice",
        is_admin=False,
        member_subject="alice",
    )
    events = await store.foundation.read_events("mention-observer")

    assert was_created and not replay_created and replay == created
    assert created.mentioned_subjects == ["oncall", "lead"]
    assert updated.mentioned_subjects == ["manager"]
    assert changed and deleted.mentioned_subjects == []
    assert events and all(event.payload["message"]["mentioned_subjects"] == [] for event in events)


@pytest.mark.asyncio
async def test_threaded_replies_have_postgres_parity(store: PostgresChatStore) -> None:
    await store.create_room(RoomCreate(id="general", name="General"))
    parent, _ = await store.create_message(
        room_id="general",
        sender="alice",
        content="Question",
        client_message_id=None,
        allow_frozen=False,
        member_subject="alice",
        author_subject="alice",
    )
    first, _ = await store.create_message(
        room_id="general",
        sender="bob",
        content="First answer",
        client_message_id="reply-1",
        parent_message_id=parent.id,
        allow_frozen=False,
        member_subject="bob",
        author_subject="bob",
    )
    second, _ = await store.create_message(
        room_id="general",
        sender="carol",
        content="Second answer",
        client_message_id=None,
        parent_message_id=parent.id,
        allow_frozen=False,
        member_subject="carol",
        author_subject="carol",
    )

    page, before = await store.list_replies("general", parent.id, limit=1)
    assert page == [second]
    assert before == second.id
    older, before = await store.list_replies("general", parent.id, limit=1, before=before)
    assert older == [first]
    assert before is None
    with pytest.raises(ThreadDepthError):
        await store.create_message(
            room_id="general",
            sender="alice",
            content="Nested",
            client_message_id=None,
            parent_message_id=first.id,
            allow_frozen=False,
        )
    with pytest.raises(MessageNotFoundError):
        await store.create_message(
            room_id="general",
            sender="alice",
            content="Missing",
            client_message_id=None,
            parent_message_id="unknown",
            allow_frozen=False,
        )

    await store.delete_message(
        room_id="general",
        message_id=parent.id,
        actor="alice",
        is_admin=False,
        member_subject="alice",
    )
    with pytest.raises(MessageDeletedError):
        await store.create_message(
            room_id="general",
            sender="alice",
            content="Too late",
            client_message_id=None,
            parent_message_id=parent.id,
            allow_frozen=False,
        )
    replay, created = await store.create_message(
        room_id="general",
        sender="bob",
        content="Ignored",
        client_message_id="reply-1",
        parent_message_id=parent.id,
        allow_frozen=False,
    )
    assert not created
    assert replay == first


@pytest.mark.asyncio
async def test_message_metadata_has_postgres_parity_and_tombstone_scrubs_events(
    store: PostgresChatStore,
) -> None:
    await store.create_room(RoomCreate(id="general", name="General"))
    await store.foundation.register_instance("metadata-observer", lease_seconds=30)
    created, was_created = await store.create_message(
        room_id="general",
        sender="alice",
        content="Investigating",
        metadata={"ticket.id": "SUP-42", "priority": 2},
        client_message_id="ticket-42",
        allow_frozen=False,
        member_subject="alice",
        author_subject="alice",
    )
    replay, was_replayed = await store.create_message(
        room_id="general",
        sender="alice",
        content="Ignored",
        metadata={"ticket.id": "OTHER"},
        client_message_id="ticket-42",
        allow_frozen=False,
        member_subject="alice",
        author_subject="alice",
    )
    preserved = await store.update_message(
        room_id="general",
        message_id=created.id,
        actor="alice",
        content="Still investigating",
        is_admin=False,
        member_subject="alice",
    )
    replaced = await store.update_message(
        room_id="general",
        message_id=created.id,
        actor="alice",
        content="Resolved",
        metadata={"resolution": "cache-flush"},
        is_admin=False,
        member_subject="alice",
    )
    cleared = await store.update_message(
        room_id="general",
        message_id=created.id,
        actor="alice",
        content="Closed",
        metadata={},
        is_admin=False,
        member_subject="alice",
    )
    await store.update_message(
        room_id="general",
        message_id=created.id,
        actor="alice",
        content="Closed",
        metadata={"ticket.id": "SUP-42"},
        is_admin=False,
        member_subject="alice",
    )
    deleted, changed = await store.delete_message(
        room_id="general",
        message_id=created.id,
        actor="alice",
        is_admin=False,
        member_subject="alice",
    )
    events = await store.foundation.read_events("metadata-observer")

    assert was_created and not was_replayed and replay == created
    assert preserved.metadata == {"priority": 2, "ticket.id": "SUP-42"}
    assert replaced.metadata == {"resolution": "cache-flush"}
    assert cleared.metadata == {}
    assert changed and deleted.content == "" and deleted.metadata == {}
    assert events and all(event.payload["message"]["metadata"] == {} for event in events)


@pytest.mark.asyncio
async def test_postgres_retention_promotes_reply_when_parent_expires(clean_postgres_database: str) -> None:
    service = _store(clean_postgres_database, max_stored_messages=2, max_stored_messages_per_room=2)
    await service.initialize()
    try:
        await service.create_room(RoomCreate(id="general", name="General"))
        parent, _ = await service.create_message(
            room_id="general",
            sender="alice",
            content="Parent",
            client_message_id=None,
            allow_frozen=False,
        )
        reply, _ = await service.create_message(
            room_id="general",
            sender="bob",
            content="Reply",
            client_message_id=None,
            parent_message_id=parent.id,
            allow_frozen=False,
        )
        await service.create_message(
            room_id="general",
            sender="carol",
            content="Newest",
            client_message_id=None,
            allow_frozen=False,
        )

        history, _ = await service.list_messages("general")
        retained_reply = next(message for message in history if message.id == reply.id)
        assert retained_reply.parent_message_id is None
        with pytest.raises(MessageNotFoundError):
            await service.list_replies("general", parent.id)
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_postgres_reactions_are_atomic_bounded_and_tombstone_safe(store: PostgresChatStore) -> None:
    await store.foundation.register_instance("reaction-observer", lease_seconds=30)
    await store.create_room(RoomCreate(id="general", name="General"))
    message, _ = await store.create_message(
        room_id="general",
        sender="alice",
        content="Acknowledge",
        client_message_id=None,
        allow_frozen=False,
        author_subject="alice",
    )

    first = await store.set_message_reaction(
        room_id="general",
        message_id=message.id,
        reactor="alice",
        key="ack",
        present=True,
        allow_frozen=False,
        member_subject="alice",
    )
    replay = await store.set_message_reaction(
        room_id="general",
        message_id=message.id,
        reactor="alice",
        key="ack",
        present=True,
        allow_frozen=False,
        member_subject="alice",
    )
    second = await store.set_message_reaction(
        room_id="general",
        message_id=message.id,
        reactor="bob",
        key="ack",
        present=True,
        allow_frozen=False,
        member_subject="bob",
    )
    assert first.changed and not replay.changed
    assert second.message.reactions[0].count == 2

    for index in range(19):
        await store.set_message_reaction(
            room_id="general",
            message_id=message.id,
            reactor=f"user-{index}",
            key=f"k{index}",
            present=True,
            allow_frozen=False,
        )
    with pytest.raises(ReactionCapacityError):
        await store.set_message_reaction(
            room_id="general",
            message_id=message.id,
            reactor="overflow",
            key="overflow",
            present=True,
            allow_frozen=False,
        )

    events = await store.foundation.read_events("reaction-observer")
    reaction_events = [event for event in events if event.event_type == "message.reaction.updated"]
    assert len(reaction_events) == 21
    assert reaction_events[0].payload["message"]["reactions"] == [{"key": "ack", "count": 1}]

    deleted, changed = await store.delete_message(
        room_id="general",
        message_id=message.id,
        actor="alice",
        is_admin=False,
        member_subject="alice",
    )
    assert changed and deleted.reactions == []
    with pytest.raises(MessageDeletedError):
        await store.set_message_reaction(
            room_id="general",
            message_id=message.id,
            reactor="alice",
            key="ack",
            present=True,
            allow_frozen=False,
        )
    scrubbed_events = await store.foundation.read_events("reaction-observer")
    scrubbed_reactions = [event for event in scrubbed_events if event.event_type == "message.reaction.updated"]
    assert scrubbed_reactions
    assert all(event.payload["reactor"] == "[deleted]" for event in scrubbed_reactions)
    assert all(event.payload["message"]["reactions"] == [] for event in scrubbed_reactions)


@pytest.mark.asyncio
async def test_postgres_pins_are_atomic_paginated_and_tombstone_safe(store: PostgresChatStore) -> None:
    await store.foundation.register_instance("pin-observer", lease_seconds=30)
    await store.create_room(RoomCreate(id="general", name="General"))
    messages = []
    for content in ("Runbook", "Decision", "Resolution"):
        message, _ = await store.create_message(
            room_id="general",
            sender="author",
            content=content,
            client_message_id=None,
            allow_frozen=False,
            author_subject="author",
        )
        messages.append(message)

    pinned_messages = []
    for index, message in enumerate(messages):
        mutation = await store.set_message_pin(
            room_id="general",
            message_id=message.id,
            pinner=f"lead-{index}",
            actor=f"lead-{index}",
            pinned=True,
            allow_frozen=False,
            member_subject=f"lead-{index}",
        )
        assert mutation.changed and mutation.message.pinned_by == f"lead-{index}"
        pinned_messages.append(mutation.message)
    replay = await store.set_message_pin(
        room_id="general",
        message_id=messages[-1].id,
        pinner="lead-2",
        actor="lead-2",
        pinned=True,
        allow_frozen=False,
        member_subject="lead-2",
    )
    assert not replay.changed

    expected_ids = [
        message.id
        for message in sorted(
            pinned_messages,
            key=lambda item: (item.pinned_at, item.id),
            reverse=True,
        )
    ]
    first_page, cursor = await store.list_pinned_messages("general", limit=2)
    assert [message.id for message in first_page] == expected_ids[:2]
    assert cursor == expected_ids[1]
    second_page, cursor = await store.list_pinned_messages("general", limit=2, before=cursor)
    assert [message.id for message in second_page] == expected_ids[2:]
    assert cursor is None

    events = await store.foundation.read_events("pin-observer")
    assert len([event for event in events if event.event_type == "message.pin.updated"]) == 3
    deleted, changed = await store.delete_message(
        room_id="general",
        message_id=messages[0].id,
        actor="author",
        is_admin=False,
        member_subject="author",
    )
    assert changed and deleted.pinned_at is None and deleted.pinned_by is None
    with pytest.raises(MessageDeletedError):
        await store.set_message_pin(
            room_id="general",
            message_id=messages[0].id,
            pinner="lead-0",
            actor="lead-0",
            pinned=True,
            allow_frozen=False,
        )
    scrubbed = await store.foundation.read_events("pin-observer")
    pin_events = [event for event in scrubbed if event.event_type == "message.pin.updated"]
    deleted_pin = next(event for event in pin_events if event.payload["message"]["id"] == messages[0].id)
    assert deleted_pin.payload["pinner"] == "[deleted]"
    assert deleted_pin.payload["message"]["content"] == ""
    assert deleted_pin.payload["message"]["pinned_at"] is None
    assert deleted_pin.payload["message"]["pinned_by"] is None


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
async def test_room_capacity_is_global_across_store_instances(clean_postgres_database: str) -> None:
    first = _store(clean_postgres_database, max_rooms=1)
    second = _store(clean_postgres_database, max_rooms=1)
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
async def test_concurrent_message_idempotency_and_event_delivery(clean_postgres_database: str) -> None:
    first = _store(clean_postgres_database)
    second = _store(clean_postgres_database)
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
async def test_message_mutation_search_pagination_and_retention(clean_postgres_database: str) -> None:
    service = _store(
        clean_postgres_database,
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
        first_events = [event for event in events if event.payload.get("message", {}).get("id") == first.id]
        assert [event.event_type for event in first_events] == ["message.created"]
        assert first_events[0].payload["message"]["content"] == ""
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
async def test_age_retention_scrubs_evicted_event_body(clean_postgres_database: str) -> None:
    service = _store(clean_postgres_database, message_retention_days=1)
    await service.initialize()
    try:
        await service.create_room(RoomCreate(id="general", name="General"))
        await service.foundation.register_instance("age-observer", lease_seconds=30)
        expired, _ = await service.create_message(
            room_id="general",
            sender="alice",
            content="expired secret",
            metadata={"ticket.id": "RETENTION-1"},
            client_message_id=None,
            allow_frozen=False,
        )
        async with service.foundation.transaction() as connection:
            await connection.execute(
                """
                UPDATE public.samsarix_messages
                SET created_at = clock_timestamp() - interval '2 days'
                WHERE id = %s
                """,
                (expired.id,),
            )
        retained, _ = await service.create_message(
            room_id="general",
            sender="alice",
            content="retained",
            client_message_id=None,
            allow_frozen=False,
        )
        messages, _ = await service.list_messages("general")
        assert [message.id for message in messages] == [retained.id]
        events = await service.foundation.read_events("age-observer")
        expired_event = next(event for event in events if event.payload["message"]["id"] == expired.id)
        assert expired_event.payload["message"]["content"] == ""
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_audit_history_enforces_configured_cap(clean_postgres_database: str) -> None:
    service = _store(clean_postgres_database, max_audit_events=3)
    await service.initialize()
    try:
        await service.create_room(RoomCreate(id="general", name="General"), actor="operator")
        for frozen in (True, False, True, False):
            await service.set_room_state(
                "general",
                archived=None,
                frozen=frozen,
                actor="operator",
            )
        audits, next_before = await service.list_audit_events()
        assert len(audits) == 3
        assert next_before is None
        assert [audit.action for audit in audits] == ["room.unfrozen", "room.frozen", "room.unfrozen"]
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_moderation_and_archived_room_rules(store: PostgresChatStore) -> None:
    await store.create_room(RoomCreate(id="general", name="General"))
    await store.foundation.register_instance("room-delete-observer", lease_seconds=30)
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
    events = await store.foundation.read_events("room-delete-observer")
    created_event = next(event for event in events if event.payload.get("message", {}).get("id") == message.id)
    assert created_event.payload["message"]["content"] == ""


@pytest.mark.asyncio
async def test_read_state_is_monotonic_subject_scoped_and_bounded(clean_postgres_database: str) -> None:
    service = _store(clean_postgres_database, max_read_states_per_room=1)
    await service.initialize()
    try:
        await service.create_room(RoomCreate(id="general", name="General"))
        own, _ = await service.create_message(
            room_id="general",
            sender="alice",
            content="own message",
            client_message_id=None,
            allow_frozen=False,
            author_subject="alice",
        )
        other, _ = await service.create_message(
            room_id="general",
            sender="bob",
            content="other message",
            client_message_id=None,
            allow_frozen=False,
            author_subject="bob",
        )

        initial = await service.get_read_state("general", "alice")
        assert initial.last_read_message_id is None
        assert initial.unread_count == 1
        assert (await service.mark_read("general", "alice", own.id)).state.unread_count == 1
        current = await service.mark_read("general", "alice", other.id)
        assert current.state.last_read_message_id == other.id
        assert current.state.unread_count == 0
        assert current.changed is True
        regressed = await service.mark_read("general", "alice", own.id)
        assert regressed.state.last_read_message_id == other.id
        assert regressed.changed is False
        receipts = await service.query_read_receipts("general", ["missing", "alice"])
        assert [receipt.subject for receipt in receipts] == ["missing", "alice"]
        assert receipts[0].last_read_message_id is None
        assert receipts[0].last_read_at is None
        assert receipts[1].last_read_message_id == other.id
        assert receipts[1].last_read_message_at == other.created_at
        assert receipts[1].last_read_at is not None
        with pytest.raises(ReadStateCapacityError):
            await service.mark_read("general", "bob", None)

        assert await service.clear_read_state("general", "alice") is True
        assert await service.clear_read_state("general", "alice") is False
        assert (await service.query_read_receipts("general", ["alice"]))[0].last_read_at is None
        async with service.foundation.transaction() as connection:
            cursor = await connection.execute(
                "SELECT COUNT(*) FROM public.samsarix_realtime_events WHERE event_type = 'read.updated'"
            )
            assert (await cursor.fetchone())[0] == 3
        reset = await service.get_read_state("general", "alice")
        assert reset.last_read_message_id is None
        assert reset.unread_count == 1
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_cross_room_read_state_query_has_postgres_parity(clean_postgres_database: str) -> None:
    service = _store(clean_postgres_database)
    await service.initialize()
    try:
        for room_id in ("support", "incident", "empty"):
            await service.create_room(RoomCreate(id=room_id, name=room_id.title()))
        own, _ = await service.create_message(
            room_id="support",
            sender="alice",
            content="own",
            client_message_id=None,
            allow_frozen=False,
            author_subject="alice",
        )
        first_support, _ = await service.create_message(
            room_id="support",
            sender="bob",
            content="first support",
            client_message_id=None,
            allow_frozen=False,
            author_subject="bob",
        )
        incident, _ = await service.create_message(
            room_id="incident",
            sender="bob",
            content="incident",
            client_message_id=None,
            allow_frozen=False,
            author_subject="bob",
        )
        assert own.id != first_support.id
        assert (await service.mark_read("support", "alice", first_support.id)).changed is True
        latest_support, _ = await service.create_message(
            room_id="support",
            sender="bob",
            content="latest support",
            client_message_id=None,
            allow_frozen=False,
            author_subject="bob",
        )

        summaries = await service.query_read_states(["incident", "support", "empty"], "alice")
        assert [summary.room_id for summary in summaries] == ["incident", "support", "empty"]
        assert [summary.unread_count for summary in summaries] == [1, 1, 0]
        assert summaries[0].latest_message_id == incident.id
        assert summaries[0].latest_message_at == incident.created_at
        assert summaries[1].last_read_message_id == first_support.id
        assert summaries[1].last_read_at is not None
        assert summaries[1].latest_message_id == latest_support.id
        assert summaries[1].latest_message_at == latest_support.created_at
        assert summaries[2].latest_message_id is None
        assert summaries[2].latest_message_at is None

        await service.delete_message(
            room_id="incident",
            message_id=incident.id,
            actor="bob",
            is_admin=False,
            member_subject="bob",
        )
        deleted = (await service.query_read_states(["incident"], "alice"))[0]
        assert deleted.unread_count == 0
        assert deleted.latest_message_id is None
        assert deleted.latest_message_at is None

        with pytest.raises(RoomNotFoundError):
            await service.query_read_states(["support", "missing-room"], "alice")
        await service.set_member_moderation(
            "incident",
            "alice",
            MemberModerationUpdate(banned_for_seconds=60),
            actor="operator",
        )
        with pytest.raises(MemberBannedError):
            await service.query_read_states(["support", "incident"], "alice")
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_export_is_a_stable_closable_snapshot(store: PostgresChatStore) -> None:
    await store.create_room(RoomCreate(id="general", name="General"))
    first, _ = await store.create_message(
        room_id="general",
        sender="alice",
        content="snapshot secret",
        client_message_id=None,
        allow_frozen=False,
        author_subject="alice",
    )
    snapshot = await store.prepare_export("general", actor="operator")
    await store.delete_message(
        room_id="general",
        message_id=first.id,
        actor="alice",
        is_admin=False,
    )

    exported = list(snapshot)
    snapshot.close()
    assert [(message.id, message.content) for message in exported] == [(first.id, "snapshot secret")]
    audits, _ = await store.list_audit_events()
    assert "room.export_requested" in [audit.action for audit in audits]


@pytest.mark.asyncio
async def test_webhook_lease_recovery_stable_id_and_payload_scrubbing(clean_postgres_database: str) -> None:
    settings = {
        "webhook_events": ("message.created", "message.deleted"),
        "webhook_worker_id": "worker-one",
    }
    first = _store(clean_postgres_database, **settings)
    second = _store(
        clean_postgres_database,
        webhook_events=settings["webhook_events"],
        webhook_worker_id="worker-two",
    )
    await asyncio.gather(first.initialize(), second.initialize())
    try:
        await first.create_room(RoomCreate(id="general", name="General"))
        message, _ = await first.create_message(
            room_id="general",
            sender="alice",
            content="deliver me",
            client_message_id=None,
            allow_frozen=False,
            author_subject="alice",
        )
        claims = await asyncio.gather(
            first.next_webhook_delivery(datetime.min.replace(tzinfo=timezone.utc)),
            second.next_webhook_delivery(datetime.max.replace(tzinfo=timezone.utc)),
        )
        assert sum(claim is not None for claim in claims) == 1
        winner = first if claims[0] is not None else second
        loser = second if winner is first else first
        claimed = claims[0] or claims[1]
        assert claimed is not None
        delivery_id = claimed.delivery.id
        assert claimed.delivery.event_type == "message.created"
        assert b'"content":"deliver me"' in claimed.payload

        async with first.foundation.transaction() as connection:
            await connection.execute(
                """
                UPDATE public.samsarix_webhook_deliveries
                SET lease_expires_at = clock_timestamp() - interval '1 second'
                WHERE id = %s
                """,
                (delivery_id,),
            )
        reclaimed = await loser.next_webhook_delivery(datetime.now(timezone.utc))
        assert reclaimed is not None and reclaimed.delivery.id == delivery_id
        with pytest.raises(WebhookDeliveryNotFoundError):
            await winner.record_webhook_attempt(
                delivery_id,
                attempted_at=datetime.now(timezone.utc),
                status_code=200,
                error=None,
                next_attempt_at=None,
                delivered=True,
                failed=False,
            )
        attempted_at = datetime(2000, 1, 1, tzinfo=timezone.utc)
        await loser.record_webhook_attempt(
            delivery_id,
            attempted_at=attempted_at,
            status_code=204,
            error=None,
            next_attempt_at=None,
            delivered=True,
            failed=False,
        )
        delivered, _ = await first.list_webhook_deliveries(status="delivered")
        assert [item.id for item in delivered] == [delivery_id]
        assert delivered[0].last_attempt_at is not None and delivered[0].last_attempt_at.year >= 2026
        assert delivered[0].delivered_at is not None and delivered[0].delivered_at.year >= 2026

        await first.create_message(
            room_id="general",
            sender="bob",
            content="retry later",
            client_message_id=None,
            allow_frozen=False,
            author_subject="bob",
        )
        retry_claim = await first.next_webhook_delivery(datetime.now(timezone.utc))
        assert retry_claim is not None
        skewed_attempt = datetime(2000, 1, 1, tzinfo=timezone.utc)
        await first.record_webhook_attempt(
            retry_claim.delivery.id,
            attempted_at=skewed_attempt,
            status_code=503,
            error="temporary",
            next_attempt_at=skewed_attempt + timedelta(seconds=45),
            delivered=False,
            failed=False,
        )
        pending, _ = await first.list_webhook_deliveries(status="pending")
        scheduled = next(item for item in pending if item.id == retry_claim.delivery.id)
        assert scheduled.last_attempt_at is not None and scheduled.next_attempt_at is not None
        assert 44.9 <= (scheduled.next_attempt_at - scheduled.last_attempt_at).total_seconds() <= 45.1

        await first.delete_message(
            room_id="general",
            message_id=message.id,
            actor="alice",
            is_admin=False,
        )
        deliveries, _ = await first.list_webhook_deliveries()
        original = next(item for item in deliveries if item.id == delivery_id)
        assert not original.replayable
        with pytest.raises(WebhookPayloadUnavailableError):
            await first.retry_webhook_delivery(delivery_id)
    finally:
        await asyncio.gather(first.close(), second.close())


@pytest.mark.asyncio
async def test_webhook_capacity_failure_rolls_back_message_update(clean_postgres_database: str) -> None:
    service = _store(
        clean_postgres_database,
        webhook_events=("message.created", "message.updated"),
        max_webhook_deliveries=1,
    )
    await service.initialize()
    try:
        await service.create_room(RoomCreate(id="general", name="General"))
        message, _ = await service.create_message(
            room_id="general",
            sender="alice",
            content="original",
            client_message_id=None,
            allow_frozen=False,
            author_subject="alice",
        )
        with pytest.raises(WebhookCapacityError):
            await service.update_message(
                room_id="general",
                message_id=message.id,
                actor="alice",
                content="must roll back",
                is_admin=False,
            )
        messages, _ = await service.list_messages("general")
        assert [(item.id, item.content) for item in messages] == [(message.id, "original")]
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_explicit_retention_scrubs_terminal_webhook_and_event_payload(clean_postgres_database: str) -> None:
    unconfigured = _store(clean_postgres_database)
    await unconfigured.initialize()
    try:
        with pytest.raises(RetentionNotConfiguredError):
            await unconfigured.run_retention(actor="operator")
    finally:
        await unconfigured.close()

    service = _store(
        clean_postgres_database,
        message_retention_days=1,
        webhook_events=("message.created",),
    )
    await service.initialize()
    try:
        await service.create_room(RoomCreate(id="general", name="General"))
        await service.foundation.register_instance("retention-observer", lease_seconds=30)
        message, _ = await service.create_message(
            room_id="general",
            sender="alice",
            content="expired secret",
            client_message_id=None,
            allow_frozen=False,
            author_subject="alice",
        )
        pending = await service.next_webhook_delivery(datetime.now(timezone.utc))
        assert pending is not None
        await service.record_webhook_attempt(
            pending.delivery.id,
            attempted_at=datetime.now(timezone.utc),
            status_code=200,
            error=None,
            next_attempt_at=None,
            delivered=True,
            failed=False,
        )
        async with service.foundation.transaction() as connection:
            await connection.execute(
                "UPDATE public.samsarix_messages SET created_at = clock_timestamp() - interval '2 days' WHERE id = %s",
                (message.id,),
            )

        deleted, cutoff = await service.run_retention(actor="operator")
        assert deleted == 1
        assert cutoff < datetime.now(timezone.utc) - timedelta(hours=23)
        deliveries, _ = await service.list_webhook_deliveries()
        assert len(deliveries) == 1 and not deliveries[0].replayable
        events = await service.foundation.read_events("retention-observer")
        assert events[0].payload["message"]["content"] == ""
        assert events[0].payload["message"]["metadata"] == {}
        audits, _ = await service.list_audit_events()
        assert audits[-1].action == "retention.executed"
    finally:
        await service.close()


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
