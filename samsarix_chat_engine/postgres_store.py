# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Internal authoritative PostgreSQL room and message store for v0.13."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from typing import Any

from psycopg import AsyncConnection
from psycopg.errors import UniqueViolation
from psycopg.types.json import Jsonb

from .models import AuditEvent, MemberModeration, MemberModerationUpdate, Message, Room, RoomCreate
from .postgres import PostgresFoundation, PostgresFoundationError
from .store import (
    InvalidAuditCursorError,
    InvalidCursorError,
    MemberBannedError,
    MemberMutedError,
    MessageDeletedError,
    MessageNotFoundError,
    MessageOwnershipError,
    RoomAlreadyExistsError,
    RoomArchivedError,
    RoomCapacityError,
    RoomFrozenError,
    RoomNotArchivedError,
    RoomNotFoundError,
    _normalize_search_text,
    normalize_search_query,
)

POSTGRES_ROOM_CAP_LOCK_ID = 7_495_346_927_831_819_043
POSTGRES_MESSAGE_CAP_LOCK_ID = 7_495_346_927_831_819_044
POSTGRES_AUDIT_CAP_LOCK_ID = 7_495_346_927_831_819_045


class PostgresChatStore:
    """PostgreSQL core store; public configuration waits for full protocol parity."""

    def __init__(
        self,
        conninfo: str,
        *,
        max_rooms: int,
        max_stored_messages: int,
        max_stored_messages_per_room: int,
        message_retention_days: int | None = None,
        max_audit_events: int = 100_000,
        min_pool_size: int = 1,
        max_pool_size: int = 5,
        pool_timeout_seconds: float = 10.0,
    ) -> None:
        if max_rooms < 1:
            raise ValueError("PostgreSQL room capacity must be positive")
        if max_stored_messages < 1 or max_stored_messages_per_room < 1:
            raise ValueError("PostgreSQL message capacities must be positive")
        if max_stored_messages_per_room > max_stored_messages:
            raise ValueError("per-room message capacity cannot exceed global capacity")
        if message_retention_days is not None and message_retention_days < 1:
            raise ValueError("message retention days must be positive")
        if max_audit_events < 1:
            raise ValueError("PostgreSQL audit capacity must be positive")
        self.max_rooms = max_rooms
        self.max_stored_messages = max_stored_messages
        self.max_stored_messages_per_room = max_stored_messages_per_room
        self.message_retention_days = message_retention_days
        self.max_audit_events = max_audit_events
        self.foundation = PostgresFoundation(
            conninfo,
            min_pool_size=min_pool_size,
            max_pool_size=max_pool_size,
            pool_timeout_seconds=pool_timeout_seconds,
        )

    async def initialize(self) -> None:
        await self.foundation.open()

    async def close(self) -> None:
        await self.foundation.close()

    async def check_ready(self) -> bool:
        try:
            return await self.foundation.schema_version() >= 2
        except PostgresFoundationError:
            return False

    async def create_room(self, payload: RoomCreate, *, actor: str = "local-operator") -> Room:
        room_id = payload.id or uuid.uuid4().hex
        async with self.foundation.transaction() as connection:
            await connection.execute("SELECT pg_advisory_xact_lock(%s)", (POSTGRES_ROOM_CAP_LOCK_ID,))
            cursor = await connection.execute("SELECT COUNT(*) FROM public.samsarix_rooms")
            count_row = await cursor.fetchone()
            if count_row is not None and int(count_row[0]) >= self.max_rooms:
                raise RoomCapacityError("room capacity reached")
            try:
                cursor = await connection.execute(
                    """
                    INSERT INTO public.samsarix_rooms (id, name, description)
                    VALUES (%s, %s, %s)
                    RETURNING id, name, description, created_at, archived_at, frozen_at
                    """,
                    (room_id, payload.name, payload.description),
                )
            except UniqueViolation:
                raise RoomAlreadyExistsError(room_id) from None
            row = await cursor.fetchone()
            room = _room_from_row(_required_row(row, "room creation"))
            await self._insert_audit(connection, action="room.created", actor=actor, room_id=room_id)
            await self.foundation.append_event(
                connection,
                room_id=room_id,
                event_type="room.created",
                payload={"type": "room.created", "room": room.model_dump(mode="json")},
            )
        return room

    async def get_room(self, room_id: str) -> Room | None:
        async with self.foundation.transaction() as connection:
            cursor = await connection.execute(
                """
                SELECT id, name, description, created_at, archived_at, frozen_at
                FROM public.samsarix_rooms WHERE id = %s
                """,
                (room_id,),
            )
            row = await cursor.fetchone()
        return _room_from_row(row) if row is not None else None

    async def list_rooms(self, *, limit: int = 100) -> list[Room]:
        if not 1 <= limit <= 100:
            raise ValueError("room list limit must be between 1 and 100")
        async with self.foundation.transaction() as connection:
            cursor = await connection.execute(
                """
                SELECT id, name, description, created_at, archived_at, frozen_at
                FROM public.samsarix_rooms ORDER BY created_at, id LIMIT %s
                """,
                (limit,),
            )
            rows = await cursor.fetchall()
        return [_room_from_row(row) for row in rows]

    async def set_room_state(
        self,
        room_id: str,
        *,
        archived: bool | None,
        frozen: bool | None,
        actor: str,
    ) -> tuple[Room, frozenset[str]]:
        async with self.foundation.transaction() as connection:
            cursor = await connection.execute(
                """
                SELECT id, name, description, created_at, archived_at, frozen_at
                FROM public.samsarix_rooms WHERE id = %s FOR UPDATE
                """,
                (room_id,),
            )
            original = await cursor.fetchone()
            if original is None:
                raise RoomNotFoundError(room_id)
            changes: set[str] = set()
            if archived is not None and (original[4] is not None) != archived:
                await connection.execute(
                    """
                    UPDATE public.samsarix_rooms
                    SET archived_at = CASE WHEN %s THEN clock_timestamp() END WHERE id = %s
                    """,
                    (archived, room_id),
                )
                await self._insert_audit(
                    connection,
                    action="room.archived" if archived else "room.unarchived",
                    actor=actor,
                    room_id=room_id,
                )
                changes.add("archived")
            if frozen is not None and (original[5] is not None) != frozen:
                await connection.execute(
                    """
                    UPDATE public.samsarix_rooms
                    SET frozen_at = CASE WHEN %s THEN clock_timestamp() END WHERE id = %s
                    """,
                    (frozen, room_id),
                )
                await self._insert_audit(
                    connection,
                    action="room.frozen" if frozen else "room.unfrozen",
                    actor=actor,
                    room_id=room_id,
                )
                changes.add("frozen")
            cursor = await connection.execute(
                """
                SELECT id, name, description, created_at, archived_at, frozen_at
                FROM public.samsarix_rooms WHERE id = %s
                """,
                (room_id,),
            )
            row = await cursor.fetchone()
            room = _room_from_row(_required_row(row, "room state update"))
            for change in sorted(changes):
                enabled = archived if change == "archived" else frozen
                event_type = f"room.{change}" if enabled else f"room.un{change}"
                await self.foundation.append_event(
                    connection,
                    room_id=room_id,
                    event_type=event_type,
                    payload={"type": event_type, "room": room.model_dump(mode="json")},
                )
        return room, frozenset(changes)

    async def delete_room(self, room_id: str, *, actor: str) -> int:
        async with self.foundation.transaction() as connection:
            cursor = await connection.execute(
                "SELECT archived_at FROM public.samsarix_rooms WHERE id = %s FOR UPDATE",
                (room_id,),
            )
            row = await cursor.fetchone()
            if row is None:
                raise RoomNotFoundError(room_id)
            if row[0] is None:
                raise RoomNotArchivedError(room_id)
            await connection.execute("SELECT pg_advisory_xact_lock(%s)", (POSTGRES_MESSAGE_CAP_LOCK_ID,))
            cursor = await connection.execute(
                "SELECT id, room_id FROM public.samsarix_messages WHERE room_id = %s",
                (room_id,),
            )
            message_rows = await cursor.fetchall()
            deleted_messages = len(message_rows)
            await self._scrub_message_event_payloads(connection, message_rows)
            await self._insert_audit(
                connection,
                action="room.deleted",
                actor=actor,
                room_id=room_id,
                details={"deleted_messages": deleted_messages},
            )
            await connection.execute("DELETE FROM public.samsarix_rooms WHERE id = %s", (room_id,))
            await self.foundation.append_event(
                connection,
                room_id=room_id,
                event_type="room.deleted",
                payload={"type": "room.deleted", "room_id": room_id, "deleted_messages": deleted_messages},
            )
        return deleted_messages

    async def create_message(
        self,
        *,
        room_id: str,
        sender: str,
        content: str,
        client_message_id: str | None,
        allow_frozen: bool,
        member_subject: str | None = None,
        author_subject: str | None = None,
    ) -> tuple[Message, bool]:
        async with self.foundation.transaction() as connection:
            await self._lock_writable_room(
                connection,
                room_id,
                allow_frozen=allow_frozen,
                member_subject=member_subject,
            )
            if client_message_id is not None:
                cursor = await connection.execute(
                    f"{_MESSAGE_SELECT} WHERE room_id = %s AND client_message_id = %s",
                    (room_id, client_message_id),
                )
                existing = await cursor.fetchone()
                if existing is not None:
                    return _message_from_row(existing), False
            await connection.execute("SELECT pg_advisory_xact_lock(%s)", (POSTGRES_MESSAGE_CAP_LOCK_ID,))
            message_id = uuid.uuid4().hex
            cursor = await connection.execute(
                """
                INSERT INTO public.samsarix_messages (
                    id, room_id, sender, author_subject, content, search_content, client_message_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id, room_id, sender, content, created_at, client_message_id, edited_at, deleted_at
                """,
                (
                    message_id,
                    room_id,
                    sender,
                    author_subject,
                    content,
                    _normalize_search_text(content),
                    client_message_id,
                ),
            )
            row = await cursor.fetchone()
            message = _message_from_row(_required_row(row, "message creation"))
            await self._trim_messages(connection, room_id=room_id, now=message.created_at)
            await self.foundation.append_event(
                connection,
                room_id=room_id,
                event_type="message.created",
                payload={"type": "message.created", "message": message.model_dump(mode="json")},
            )
        return message, True

    async def update_message(
        self,
        *,
        room_id: str,
        message_id: str,
        actor: str,
        content: str,
        is_admin: bool,
        member_subject: str | None = None,
    ) -> Message:
        async with self.foundation.transaction() as connection:
            await self._lock_writable_room(
                connection,
                room_id,
                allow_frozen=is_admin,
                member_subject=member_subject,
            )
            await connection.execute("SELECT pg_advisory_xact_lock(%s)", (POSTGRES_MESSAGE_CAP_LOCK_ID,))
            row = await self._lock_message(connection, room_id, message_id)
            if row[7] is not None:
                raise MessageDeletedError(message_id)
            if not is_admin and str(row[2]) != actor:
                raise MessageOwnershipError(message_id)
            cursor = await connection.execute(
                """
                UPDATE public.samsarix_messages
                SET content = %s, search_content = %s, edited_at = clock_timestamp()
                WHERE id = %s
                RETURNING id, room_id, sender, content, created_at, client_message_id, edited_at, deleted_at
                """,
                (content, _normalize_search_text(content), message_id),
            )
            updated = await cursor.fetchone()
            message = _message_from_row(_required_row(updated, "message update"))
            await self._insert_audit(
                connection,
                action="message.updated",
                actor=actor,
                room_id=room_id,
                details={"message_id": message_id},
            )
            await self.foundation.append_event(
                connection,
                room_id=room_id,
                event_type="message.updated",
                payload={"type": "message.updated", "message": message.model_dump(mode="json")},
            )
        return message

    async def delete_message(
        self,
        *,
        room_id: str,
        message_id: str,
        actor: str,
        is_admin: bool,
        member_subject: str | None = None,
    ) -> tuple[Message, bool]:
        async with self.foundation.transaction() as connection:
            await self._lock_writable_room(
                connection,
                room_id,
                allow_archived=is_admin,
                allow_frozen=is_admin,
                member_subject=member_subject,
            )
            await connection.execute("SELECT pg_advisory_xact_lock(%s)", (POSTGRES_MESSAGE_CAP_LOCK_ID,))
            row = await self._lock_message(connection, room_id, message_id)
            if not is_admin and str(row[2]) != actor:
                raise MessageOwnershipError(message_id)
            if row[7] is not None:
                return _message_from_row(row), False
            cursor = await connection.execute(
                """
                UPDATE public.samsarix_messages
                SET content = '', search_content = '', deleted_at = clock_timestamp()
                WHERE id = %s
                RETURNING id, room_id, sender, content, created_at, client_message_id, edited_at, deleted_at
                """,
                (message_id,),
            )
            deleted = await cursor.fetchone()
            message = _message_from_row(_required_row(deleted, "message deletion"))
            await self._scrub_message_event_payloads(connection, [(message.id, message.room_id)])
            await self._insert_audit(
                connection,
                action="message.deleted",
                actor=actor,
                room_id=room_id,
                details={"message_id": message_id},
            )
            await self.foundation.append_event(
                connection,
                room_id=room_id,
                event_type="message.deleted",
                payload={"type": "message.deleted", "message": message.model_dump(mode="json")},
            )
        return message, True

    async def get_member_moderation(self, room_id: str, subject: str) -> MemberModeration | None:
        async with self.foundation.transaction() as connection:
            cursor = await connection.execute(
                """
                SELECT room_id, subject, muted_until, banned_until, updated_at
                FROM public.samsarix_room_member_controls WHERE room_id = %s AND subject = %s
                """,
                (room_id, subject),
            )
            row = await cursor.fetchone()
        return _moderation_from_row(row) if row is not None else None

    async def set_member_moderation(
        self,
        room_id: str,
        subject: str,
        payload: MemberModerationUpdate,
        *,
        actor: str,
    ) -> MemberModeration:
        async with self.foundation.transaction() as connection:
            cursor = await connection.execute(
                "SELECT 1 FROM public.samsarix_rooms WHERE id = %s FOR UPDATE",
                (room_id,),
            )
            if await cursor.fetchone() is None:
                raise RoomNotFoundError(room_id)
            cursor = await connection.execute(
                """
                SELECT muted_until, banned_until
                FROM public.samsarix_room_member_controls
                WHERE room_id = %s AND subject = %s FOR UPDATE
                """,
                (room_id, subject),
            )
            existing = await cursor.fetchone()
            cursor = await connection.execute("SELECT clock_timestamp()")
            now_row = await cursor.fetchone()
            now = _required_row(now_row, "database clock")[0]
            muted_until = existing[0] if existing is not None else None
            banned_until = existing[1] if existing is not None else None
            if payload.muted_for_seconds is not None:
                muted_until = now + timedelta(seconds=payload.muted_for_seconds) if payload.muted_for_seconds else None
            if payload.banned_for_seconds is not None:
                banned_until = (
                    now + timedelta(seconds=payload.banned_for_seconds) if payload.banned_for_seconds else None
                )
            if muted_until is None and banned_until is None:
                await connection.execute(
                    "DELETE FROM public.samsarix_room_member_controls WHERE room_id = %s AND subject = %s",
                    (room_id, subject),
                )
            else:
                await connection.execute(
                    """
                    INSERT INTO public.samsarix_room_member_controls (
                        room_id, subject, muted_until, banned_until, updated_at
                    ) VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (room_id, subject) DO UPDATE SET
                        muted_until = EXCLUDED.muted_until,
                        banned_until = EXCLUDED.banned_until,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (room_id, subject, muted_until, banned_until, now),
                )
            moderation = MemberModeration(
                room_id=room_id,
                subject=subject,
                muted_until=muted_until,
                banned_until=banned_until,
                updated_at=now,
            )
            moderation_json = moderation.model_dump(mode="json")
            await self._insert_audit(
                connection,
                action="member.moderation_updated",
                actor=actor,
                room_id=room_id,
                details={
                    "subject": subject,
                    "muted_until": moderation_json["muted_until"],
                    "banned_until": moderation_json["banned_until"],
                },
            )
            await self.foundation.append_event(
                connection,
                room_id=room_id,
                event_type="member.moderation.updated",
                payload={"type": "member.moderation.updated", "actor": actor, "moderation": moderation_json},
            )
        return moderation

    async def list_messages(
        self,
        room_id: str,
        *,
        limit: int = 50,
        before: str | None = None,
    ) -> tuple[list[Message], str | None]:
        _validate_page_limit(limit)
        async with self.foundation.transaction() as connection:
            await _require_room(connection, room_id)
            cursor_key = await _message_cursor(connection, room_id, before)
            if cursor_key is None:
                cursor = await connection.execute(
                    f"{_MESSAGE_SELECT} WHERE room_id = %s ORDER BY created_at DESC, id DESC LIMIT %s",
                    (room_id, limit + 1),
                )
            else:
                cursor = await connection.execute(
                    f"""
                    {_MESSAGE_SELECT}
                    WHERE room_id = %s AND (created_at < %s OR (created_at = %s AND id < %s))
                    ORDER BY created_at DESC, id DESC LIMIT %s
                    """,
                    (room_id, cursor_key[0], cursor_key[0], cursor_key[1], limit + 1),
                )
            rows = await cursor.fetchall()
        page_rows, next_before = _page_rows(rows, limit, chronological=True)
        return [_message_from_row(row) for row in page_rows], next_before

    async def search_messages(
        self,
        room_id: str,
        query: str,
        *,
        limit: int = 50,
        before: str | None = None,
    ) -> tuple[list[Message], str | None]:
        _validate_page_limit(limit)
        normalized_query = normalize_search_query(query)
        async with self.foundation.transaction() as connection:
            await _require_room(connection, room_id)
            cursor_key = await _message_cursor(connection, room_id, before)
            cursor = await connection.execute(
                f"""
                {_MESSAGE_SELECT}
                WHERE room_id = %s
                  AND deleted_at IS NULL
                  AND strpos(search_content, %s) > 0
                  AND (%s::timestamptz IS NULL OR created_at < %s OR (created_at = %s AND id < %s))
                ORDER BY created_at DESC, id DESC LIMIT %s
                """,
                (
                    room_id,
                    normalized_query,
                    cursor_key[0] if cursor_key else None,
                    cursor_key[0] if cursor_key else None,
                    cursor_key[0] if cursor_key else None,
                    cursor_key[1] if cursor_key else None,
                    limit + 1,
                ),
            )
            rows = await cursor.fetchall()
        page_rows, next_before = _page_rows(rows, limit, chronological=True)
        return [_message_from_row(row) for row in page_rows], next_before

    async def list_audit_events(
        self,
        *,
        limit: int = 50,
        before: str | None = None,
    ) -> tuple[list[AuditEvent], str | None]:
        _validate_page_limit(limit)
        async with self.foundation.transaction() as connection:
            cursor_key: tuple[datetime, str] | None = None
            if before is not None:
                cursor = await connection.execute(
                    "SELECT created_at, id FROM public.samsarix_audit_events WHERE id = %s",
                    (before,),
                )
                row = await cursor.fetchone()
                if row is None:
                    raise InvalidAuditCursorError(before)
                cursor_key = (row[0], str(row[1]))
            if cursor_key is None:
                cursor = await connection.execute(
                    """
                    SELECT id, action, actor, room_id, created_at, details
                    FROM public.samsarix_audit_events
                    ORDER BY created_at DESC, id DESC LIMIT %s
                    """,
                    (limit + 1,),
                )
            else:
                cursor = await connection.execute(
                    """
                    SELECT id, action, actor, room_id, created_at, details
                    FROM public.samsarix_audit_events
                    WHERE created_at < %s OR (created_at = %s AND id < %s)
                    ORDER BY created_at DESC, id DESC LIMIT %s
                    """,
                    (cursor_key[0], cursor_key[0], cursor_key[1], limit + 1),
                )
            rows = await cursor.fetchall()
        page_rows, next_before = _page_rows(rows, limit, chronological=True)
        return [_audit_from_row(row) for row in page_rows], next_before

    async def _lock_writable_room(
        self,
        connection: AsyncConnection[tuple[Any, ...]],
        room_id: str,
        *,
        allow_archived: bool = False,
        allow_frozen: bool,
        member_subject: str | None,
    ) -> None:
        cursor = await connection.execute(
            "SELECT archived_at, frozen_at FROM public.samsarix_rooms WHERE id = %s FOR UPDATE",
            (room_id,),
        )
        room = await cursor.fetchone()
        if room is None:
            raise RoomNotFoundError(room_id)
        if room[0] is not None and not allow_archived:
            raise RoomArchivedError(room_id)
        if room[1] is not None and not allow_frozen:
            raise RoomFrozenError(room_id)
        if member_subject is None:
            return
        cursor = await connection.execute(
            """
            SELECT muted_until > clock_timestamp(), banned_until > clock_timestamp()
            FROM public.samsarix_room_member_controls WHERE room_id = %s AND subject = %s
            """,
            (room_id, member_subject),
        )
        moderation = await cursor.fetchone()
        if moderation is None:
            return
        if bool(moderation[1]):
            raise MemberBannedError(member_subject)
        if bool(moderation[0]):
            raise MemberMutedError(member_subject)

    async def _lock_message(
        self,
        connection: AsyncConnection[tuple[Any, ...]],
        room_id: str,
        message_id: str,
    ) -> tuple[Any, ...]:
        cursor = await connection.execute(
            f"{_MESSAGE_SELECT} WHERE room_id = %s AND id = %s FOR UPDATE",
            (room_id, message_id),
        )
        row = await cursor.fetchone()
        if row is None:
            raise MessageNotFoundError(message_id)
        return row

    async def _trim_messages(
        self,
        connection: AsyncConnection[tuple[Any, ...]],
        *,
        room_id: str,
        now: datetime,
    ) -> None:
        age_deleted = 0
        if self.message_retention_days is not None:
            cursor = await connection.execute(
                "DELETE FROM public.samsarix_messages WHERE created_at < %s RETURNING id, room_id",
                (now - timedelta(days=self.message_retention_days),),
            )
            age_rows = await cursor.fetchall()
            age_deleted = len(age_rows)
            await self._scrub_message_event_payloads(connection, age_rows)
        cursor = await connection.execute(
            """
            DELETE FROM public.samsarix_messages WHERE id IN (
                SELECT id FROM public.samsarix_messages WHERE room_id = %s
                ORDER BY created_at DESC, id DESC OFFSET %s
            ) RETURNING id, room_id
            """,
            (room_id, self.max_stored_messages_per_room),
        )
        room_cap_rows = await cursor.fetchall()
        room_cap_deleted = len(room_cap_rows)
        await self._scrub_message_event_payloads(connection, room_cap_rows)
        cursor = await connection.execute(
            """
            DELETE FROM public.samsarix_messages WHERE id IN (
                SELECT id FROM public.samsarix_messages
                ORDER BY created_at DESC, id DESC OFFSET %s
            ) RETURNING id, room_id
            """,
            (self.max_stored_messages,),
        )
        global_cap_rows = await cursor.fetchall()
        global_cap_deleted = len(global_cap_rows)
        await self._scrub_message_event_payloads(connection, global_cap_rows)
        if age_deleted or room_cap_deleted or global_cap_deleted:
            details: dict[str, Any] = {
                "age_deleted": age_deleted,
                "global_cap_deleted": global_cap_deleted,
                "room_cap_deleted": room_cap_deleted,
                "trigger_room_id": room_id,
            }
            if self.message_retention_days is not None:
                details["cutoff"] = (now - timedelta(days=self.message_retention_days)).isoformat()
            await self._insert_audit(
                connection,
                action="retention.automatic",
                actor="system:retention",
                details=details,
            )

    @staticmethod
    async def _scrub_message_event_payloads(
        connection: AsyncConnection[tuple[Any, ...]],
        messages: list[tuple[Any, ...]],
    ) -> None:
        by_room: dict[str, list[str]] = {}
        for message_id, room_id, *_rest in messages:
            by_room.setdefault(str(room_id), []).append(str(message_id))
        for room_id, message_ids in by_room.items():
            await connection.execute(
                """
                UPDATE public.samsarix_realtime_events
                SET payload = jsonb_set(payload, '{message,content}', %s, false)
                WHERE room_id = %s
                  AND event_type LIKE 'message.%'
                  AND payload #>> '{message,id}' = ANY(%s)
                """,
                (Jsonb(""), room_id, message_ids),
            )

    async def _insert_audit(
        self,
        connection: AsyncConnection[tuple[Any, ...]],
        *,
        action: str,
        actor: str,
        room_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        await connection.execute("SELECT pg_advisory_xact_lock(%s)", (POSTGRES_AUDIT_CAP_LOCK_ID,))
        await connection.execute(
            """
            INSERT INTO public.samsarix_audit_events (id, action, actor, room_id, details)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (uuid.uuid4().hex, action, actor, room_id, Jsonb(details or {})),
        )
        await connection.execute(
            """
            DELETE FROM public.samsarix_audit_events WHERE id IN (
                SELECT id FROM public.samsarix_audit_events
                ORDER BY created_at DESC, id DESC OFFSET %s
            )
            """,
            (self.max_audit_events,),
        )


_MESSAGE_SELECT = """
SELECT id, room_id, sender, content, created_at, client_message_id, edited_at, deleted_at
FROM public.samsarix_messages
"""


async def _require_room(connection: AsyncConnection[tuple[Any, ...]], room_id: str) -> None:
    cursor = await connection.execute("SELECT 1 FROM public.samsarix_rooms WHERE id = %s", (room_id,))
    if await cursor.fetchone() is None:
        raise RoomNotFoundError(room_id)


async def _message_cursor(
    connection: AsyncConnection[tuple[Any, ...]],
    room_id: str,
    before: str | None,
) -> tuple[datetime, str] | None:
    if before is None:
        return None
    cursor = await connection.execute(
        "SELECT created_at, id FROM public.samsarix_messages WHERE room_id = %s AND id = %s",
        (room_id, before),
    )
    row = await cursor.fetchone()
    if row is None:
        raise InvalidCursorError(before)
    return row[0], str(row[1])


def _validate_page_limit(limit: int) -> None:
    if not 1 <= limit <= 100:
        raise ValueError("page limit must be between 1 and 100")


def _page_rows(
    rows: list[tuple[Any, ...]], limit: int, *, chronological: bool
) -> tuple[list[tuple[Any, ...]], str | None]:
    page = rows[:limit]
    next_before = str(page[-1][0]) if len(rows) > limit and page else None
    return (list(reversed(page)) if chronological else page), next_before


def _required_row(row: tuple[Any, ...] | None, operation: str) -> tuple[Any, ...]:
    if row is None:  # pragma: no cover - PostgreSQL guarantees these RETURNING rows
        raise RuntimeError(f"PostgreSQL returned no row for {operation}")
    return row


def _room_from_row(row: tuple[Any, ...]) -> Room:
    return Room(
        id=str(row[0]),
        name=str(row[1]),
        description=str(row[2]),
        created_at=row[3],
        archived_at=row[4],
        frozen_at=row[5],
    )


def _message_from_row(row: tuple[Any, ...]) -> Message:
    return Message(
        id=str(row[0]),
        room_id=str(row[1]),
        sender=str(row[2]),
        content=str(row[3]),
        created_at=row[4],
        client_message_id=str(row[5]) if row[5] is not None else None,
        edited_at=row[6],
        deleted_at=row[7],
    )


def _moderation_from_row(row: tuple[Any, ...]) -> MemberModeration:
    return MemberModeration(
        room_id=str(row[0]),
        subject=str(row[1]),
        muted_until=row[2],
        banned_until=row[3],
        updated_at=row[4],
    )


def _audit_from_row(row: tuple[Any, ...]) -> AuditEvent:
    return AuditEvent(
        id=str(row[0]),
        action=str(row[1]),
        actor=str(row[2]),
        room_id=str(row[3]) if row[3] is not None else None,
        created_at=row[4],
        details=dict(row[5]),
    )
