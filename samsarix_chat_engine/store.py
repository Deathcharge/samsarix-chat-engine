# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Bounded SQLite persistence for rooms, messages, and lifecycle audit metadata."""

from __future__ import annotations

import asyncio
import importlib
import json
import logging
import os
import sqlite3
import tempfile
import uuid
from collections.abc import Callable, Iterator
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import IO, Any, TypeVar

from .models import AuditEvent, Message, Room, RoomCreate

T = TypeVar("T")
SCHEMA_VERSION = 2
logger = logging.getLogger(__name__)


def copy_sqlite_database(source: Path, target: Path, *, replace: bool = False) -> None:
    """Create an integrity-checked SQLite snapshot and atomically place it at target."""

    source = source.resolve()
    target = target.resolve()
    if source == target:
        raise ValueError("source and target database paths must differ")
    if not source.is_file():
        raise FileNotFoundError(f"source database does not exist: {source}")
    if target.exists() and not replace:
        raise FileExistsError(f"target already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary_handle = tempfile.NamedTemporaryFile(
        prefix=f".{target.name}.",
        suffix=".tmp",
        dir=target.parent,
        delete=False,
    )
    temporary = Path(temporary_handle.name)
    temporary_handle.close()
    try:
        with (
            closing(sqlite3.connect(source)) as source_connection,
            closing(sqlite3.connect(temporary)) as target_connection,
        ):
            source_connection.backup(target_connection)
            integrity = target_connection.execute("PRAGMA integrity_check").fetchone()[0]
            if integrity != "ok":
                raise sqlite3.DatabaseError(f"snapshot integrity check failed: {integrity}")
        if replace:
            os.replace(temporary, target)
        else:
            # A same-directory hard link gives no-clobber placement on both
            # POSIX and Windows, including if another process creates the
            # target after the earlier existence check.
            os.link(temporary, target)
        # Restoring only the main file while retaining an earlier WAL can
        # replay stale pages into the new snapshot on its next open.
        Path(f"{target}-wal").unlink(missing_ok=True)
        Path(f"{target}-shm").unlink(missing_ok=True)
    finally:
        temporary.unlink(missing_ok=True)


class StoreError(RuntimeError):
    """Base error for storage operations."""


class RoomNotFoundError(StoreError):
    """Raised when a requested room does not exist."""


class RoomAlreadyExistsError(StoreError):
    """Raised when a caller-selected room ID is already present."""


class RoomCapacityError(StoreError):
    """Raised when the configured room cap has been reached."""


class RoomArchivedError(StoreError):
    """Raised when a write targets an archived room."""


class RoomNotArchivedError(StoreError):
    """Raised when irreversible deletion targets an active room."""


class InvalidCursorError(StoreError):
    """Raised when a message pagination cursor is unknown for a room."""


class InvalidAuditCursorError(StoreError):
    """Raised when an administrative audit cursor is unknown."""


class RetentionNotConfiguredError(StoreError):
    """Raised when a retention pass runs without a configured maximum age."""


class DatabaseInUseError(StoreError):
    """Raised when another process holds the database lifecycle lock."""


class UnsupportedSchemaVersionError(StoreError):
    """Raised rather than mutating a database written by a newer engine."""


class DatabaseLifecycleLock:
    """Cross-process exclusive lock used to coordinate service and restore."""

    def __init__(self, database_path: Path) -> None:
        resolved = database_path.resolve()
        self.database_path = resolved
        self.path = resolved.with_name(f"{resolved.name}.lock")
        self._handle: IO[bytes] | None = None

    def acquire(self) -> None:
        if self._handle is not None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+b")
        try:
            handle.seek(0, os.SEEK_END)
            if handle.tell() == 0:
                handle.write(b"\0")
                handle.flush()
            handle.seek(0)
            if os.name == "nt":
                msvcrt: Any = importlib.import_module("msvcrt")
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                fcntl: Any = importlib.import_module("fcntl")
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            handle.close()
            raise DatabaseInUseError(f"database is in use: {self.database_path}") from exc
        self._handle = handle

    def release(self) -> None:
        handle = self._handle
        if handle is None:
            return
        self._handle = None
        try:
            handle.seek(0)
            if os.name == "nt":
                msvcrt: Any = importlib.import_module("msvcrt")
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl: Any = importlib.import_module("fcntl")
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()

    def __enter__(self) -> DatabaseLifecycleLock:
        self.acquire()
        return self

    def __exit__(self, _exc_type: object, _exc: object, _traceback: object) -> None:
        self.release()


class MessageSnapshot(Iterator[Message]):
    """Serializable iterator over one eager SQLite read snapshot."""

    def __init__(
        self,
        connection: sqlite3.Connection,
        cursor: sqlite3.Cursor,
        *,
        batch_size: int,
        initial_rows: list[sqlite3.Row],
    ) -> None:
        self._connection = connection
        self._cursor = cursor
        self._batch_size = batch_size
        self._buffer: Iterator[sqlite3.Row] = iter(initial_rows)
        self._closed = False

    def __next__(self) -> Message:
        if self._closed:
            raise StopIteration
        try:
            row = next(self._buffer)
        except StopIteration:
            rows = self._cursor.fetchmany(self._batch_size)
            if not rows:
                self.close()
                raise
            self._buffer = iter(rows)
            row = next(self._buffer)
        return ChatStore._message_from_row(row)

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._cursor.close()
            self._connection.close()


class ChatStore:
    """Small asynchronous facade over a per-operation SQLite connection."""

    def __init__(
        self,
        database_path: Path,
        *,
        max_rooms: int,
        max_stored_messages: int,
        max_stored_messages_per_room: int,
        message_retention_days: int | None = None,
        max_audit_events: int = 100_000,
    ) -> None:
        self.database_path = database_path
        self.max_rooms = max_rooms
        self.max_stored_messages = max_stored_messages
        self.max_stored_messages_per_room = max_stored_messages_per_room
        self.message_retention_days = message_retention_days
        self.max_audit_events = max_audit_events
        self._write_lock = asyncio.Lock()

    async def initialize(self) -> None:
        """Create or migrate the database schema without discarding v0.4 data."""

        await asyncio.to_thread(self._initialize_sync)

    async def check_ready(self) -> bool:
        """Return whether the database can execute a trivial query."""

        try:
            await asyncio.to_thread(self._run, lambda connection: connection.execute("SELECT 1").fetchone())
        except (OSError, sqlite3.Error):
            return False
        return True

    async def create_room(self, payload: RoomCreate, *, actor: str = "local-operator") -> Room:
        room_id = payload.id or uuid.uuid4().hex
        async with self._write_lock:
            return await asyncio.to_thread(self._create_room_sync, room_id, payload, actor)

    async def get_room(self, room_id: str) -> Room | None:
        row = await asyncio.to_thread(
            self._run,
            lambda connection: connection.execute(
                "SELECT id, name, description, created_at, archived_at FROM rooms WHERE id = ?", (room_id,)
            ).fetchone(),
        )
        return self._room_from_row(row) if row else None

    async def list_rooms(self, *, limit: int = 100) -> list[Room]:
        rows = await asyncio.to_thread(
            self._run,
            lambda connection: connection.execute(
                """
                SELECT id, name, description, created_at, archived_at
                FROM rooms ORDER BY created_at, id LIMIT ?
                """,
                (limit,),
            ).fetchall(),
        )
        return [self._room_from_row(row) for row in rows]

    async def set_room_archived(self, room_id: str, *, archived: bool, actor: str) -> tuple[Room, bool]:
        async with self._write_lock:
            return await asyncio.to_thread(self._set_room_archived_sync, room_id, archived, actor)

    async def delete_room(self, room_id: str, *, actor: str) -> int:
        """Delete an already archived room and return its deleted message count."""

        async with self._write_lock:
            return await asyncio.to_thread(self._delete_room_sync, room_id, actor)

    async def prepare_export(self, room_id: str, *, actor: str) -> MessageSnapshot:
        """Audit and open one room snapshot without interleaving an application write."""

        async with self._write_lock:
            await asyncio.to_thread(self._record_export_requested_sync, room_id, actor)
            return await asyncio.to_thread(self.iter_messages, room_id)

    async def create_message(
        self,
        *,
        room_id: str,
        sender: str,
        content: str,
        client_message_id: str | None,
    ) -> tuple[Message, bool]:
        async with self._write_lock:
            return await asyncio.to_thread(
                self._create_message_sync,
                room_id,
                sender,
                content,
                client_message_id,
            )

    async def list_messages(
        self,
        room_id: str,
        *,
        limit: int = 50,
        before: str | None = None,
    ) -> tuple[list[Message], str | None]:
        return await asyncio.to_thread(self._list_messages_sync, room_id, limit, before)

    def iter_messages(self, room_id: str, *, batch_size: int = 500) -> MessageSnapshot:
        """Stream a consistent room-history snapshot without retaining it all in memory."""

        # Starlette may advance one synchronous iterator on different worker
        # threads. The snapshot itself is consumed serially, so disabling the
        # thread-affinity check here preserves one SQLite snapshot safely.
        connection = self._connect(check_same_thread=False)
        try:
            connection.execute("BEGIN")
            cursor = connection.execute(
                """
                SELECT id, room_id, sender, content, created_at, client_message_id
                FROM messages WHERE room_id = ? ORDER BY created_at, id
                """,
                (room_id,),
            )
            # Step the statement before releasing the application write lock,
            # establishing the SQLite read snapshot for concurrent deletion.
            initial_rows = cursor.fetchmany(batch_size)
        except Exception:
            connection.close()
            raise
        return MessageSnapshot(connection, cursor, batch_size=batch_size, initial_rows=initial_rows)

    async def list_audit_events(
        self,
        *,
        limit: int = 50,
        before: str | None = None,
    ) -> tuple[list[AuditEvent], str | None]:
        return await asyncio.to_thread(self._list_audit_events_sync, limit, before)

    async def run_retention(self, *, actor: str) -> tuple[int, datetime]:
        if self.message_retention_days is None:
            raise RetentionNotConfiguredError("message retention is not configured")
        cutoff = datetime.now(timezone.utc) - timedelta(days=self.message_retention_days)
        async with self._write_lock:
            deleted = await asyncio.to_thread(self._run_retention_sync, cutoff, actor)
        return deleted, cutoff

    def _connect(self, *, check_same_thread: bool = True) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path, timeout=5.0, check_same_thread=check_same_thread)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 5000")
        return connection

    def _run(self, operation: Callable[[sqlite3.Connection], T]) -> T:
        with closing(self._connect()) as connection, connection:
            return operation(connection)

    def _initialize_sync(self) -> None:
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as connection, connection:
            current_version = connection.execute("PRAGMA user_version").fetchone()[0]
            if current_version > SCHEMA_VERSION:
                raise UnsupportedSchemaVersionError(
                    f"database schema version {current_version} is newer than supported version {SCHEMA_VERSION}"
                )
            if current_version < SCHEMA_VERSION:
                logger.info("Migrating database schema from version %s to %s", current_version, SCHEMA_VERSION)
            connection.execute("PRAGMA journal_mode = WAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS rooms (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS messages (
                    id TEXT PRIMARY KEY,
                    room_id TEXT NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
                    sender TEXT NOT NULL,
                    content TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    client_message_id TEXT,
                    UNIQUE(room_id, client_message_id)
                );

                CREATE INDEX IF NOT EXISTS messages_room_order
                    ON messages(room_id, created_at DESC, id DESC);
                CREATE INDEX IF NOT EXISTS messages_global_order
                    ON messages(created_at DESC, id DESC);
                """
            )
            columns = {row[1] for row in connection.execute("PRAGMA table_info(rooms)")}
            if "archived_at" not in columns:
                connection.execute("ALTER TABLE rooms ADD COLUMN archived_at TEXT")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS audit_events (
                    id TEXT PRIMARY KEY,
                    action TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    room_id TEXT,
                    created_at TEXT NOT NULL,
                    details_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE INDEX IF NOT EXISTS audit_events_order
                    ON audit_events(created_at DESC, id DESC);
                """
            )
            connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")  # noqa: S608 - constant PRAGMA literal

    def _create_room_sync(self, room_id: str, payload: RoomCreate, actor: str) -> Room:
        created_at = datetime.now(timezone.utc)
        with closing(self._connect()) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            room_count = connection.execute("SELECT COUNT(*) FROM rooms").fetchone()[0]
            if room_count >= self.max_rooms:
                raise RoomCapacityError("room capacity reached")
            try:
                connection.execute(
                    "INSERT INTO rooms (id, name, description, created_at) VALUES (?, ?, ?, ?)",
                    (room_id, payload.name, payload.description, created_at.isoformat()),
                )
            except sqlite3.IntegrityError as exc:
                raise RoomAlreadyExistsError(room_id) from exc
            self._insert_audit(connection, action="room.created", actor=actor, room_id=room_id)
        return Room(id=room_id, name=payload.name, description=payload.description, created_at=created_at)

    def _set_room_archived_sync(self, room_id: str, archived: bool, actor: str) -> tuple[Room, bool]:
        with closing(self._connect()) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT id, name, description, created_at, archived_at FROM rooms WHERE id = ?", (room_id,)
            ).fetchone()
            if row is None:
                raise RoomNotFoundError(room_id)
            changed = (row["archived_at"] is not None) != archived
            if changed:
                archived_at = datetime.now(timezone.utc).isoformat() if archived else None
                connection.execute("UPDATE rooms SET archived_at = ? WHERE id = ?", (archived_at, room_id))
                self._insert_audit(
                    connection,
                    action="room.archived" if archived else "room.unarchived",
                    actor=actor,
                    room_id=room_id,
                )
                row = connection.execute(
                    "SELECT id, name, description, created_at, archived_at FROM rooms WHERE id = ?", (room_id,)
                ).fetchone()
        return self._room_from_row(row), changed

    def _delete_room_sync(self, room_id: str, actor: str) -> int:
        with closing(self._connect()) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT archived_at FROM rooms WHERE id = ?", (room_id,)).fetchone()
            if row is None:
                raise RoomNotFoundError(room_id)
            if row["archived_at"] is None:
                raise RoomNotArchivedError(room_id)
            message_count = connection.execute(
                "SELECT COUNT(*) FROM messages WHERE room_id = ?", (room_id,)
            ).fetchone()[0]
            self._insert_audit(
                connection,
                action="room.deleted",
                actor=actor,
                room_id=room_id,
                details={"deleted_messages": message_count},
            )
            connection.execute("DELETE FROM rooms WHERE id = ?", (room_id,))
        return int(message_count)

    def _record_export_requested_sync(self, room_id: str, actor: str) -> None:
        with closing(self._connect()) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            if connection.execute("SELECT 1 FROM rooms WHERE id = ?", (room_id,)).fetchone() is None:
                raise RoomNotFoundError(room_id)
            self._insert_audit(connection, action="room.export_requested", actor=actor, room_id=room_id)

    def _create_message_sync(
        self,
        room_id: str,
        sender: str,
        content: str,
        client_message_id: str | None,
    ) -> tuple[Message, bool]:
        with closing(self._connect()) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            room = connection.execute("SELECT archived_at FROM rooms WHERE id = ?", (room_id,)).fetchone()
            if room is None:
                raise RoomNotFoundError(room_id)
            if room["archived_at"] is not None:
                raise RoomArchivedError(room_id)

            if client_message_id:
                existing = connection.execute(
                    """
                    SELECT id, room_id, sender, content, created_at, client_message_id
                    FROM messages WHERE room_id = ? AND client_message_id = ?
                    """,
                    (room_id, client_message_id),
                ).fetchone()
                if existing:
                    return self._message_from_row(existing), False

            message = Message(
                id=uuid.uuid4().hex,
                room_id=room_id,
                sender=sender,
                content=content,
                created_at=datetime.now(timezone.utc),
                client_message_id=client_message_id,
            )
            connection.execute(
                """
                INSERT INTO messages (id, room_id, sender, content, created_at, client_message_id)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    message.id,
                    message.room_id,
                    message.sender,
                    message.content,
                    message.created_at.isoformat(),
                    message.client_message_id,
                ),
            )
            self._trim_messages(connection, room_id, now=message.created_at)
            return message, True

    def _trim_messages(self, connection: sqlite3.Connection, room_id: str, *, now: datetime) -> None:
        age_deleted = 0
        if self.message_retention_days is not None:
            cutoff = now - timedelta(days=self.message_retention_days)
            age_deleted = connection.execute(
                "DELETE FROM messages WHERE created_at < ?", (cutoff.isoformat(),)
            ).rowcount
        room_cap_deleted = connection.execute(
            """
            DELETE FROM messages
            WHERE id IN (
                SELECT id FROM messages WHERE room_id = ?
                ORDER BY created_at, id
                LIMIT MAX(0, (SELECT COUNT(*) FROM messages WHERE room_id = ?) - ?)
            )
            """,
            (room_id, room_id, self.max_stored_messages_per_room),
        ).rowcount
        global_cap_deleted = connection.execute(
            """
            DELETE FROM messages
            WHERE id IN (
                SELECT id FROM messages ORDER BY created_at, id
                LIMIT MAX(0, (SELECT COUNT(*) FROM messages) - ?)
            )
            """,
            (self.max_stored_messages,),
        ).rowcount
        if age_deleted or room_cap_deleted or global_cap_deleted:
            details: dict[str, Any] = {
                "age_deleted": age_deleted,
                "global_cap_deleted": global_cap_deleted,
                "room_cap_deleted": room_cap_deleted,
                "trigger_room_id": room_id,
            }
            if self.message_retention_days is not None:
                details["cutoff"] = (now - timedelta(days=self.message_retention_days)).isoformat()
            self._insert_audit(
                connection,
                action="retention.automatic",
                actor="system:retention",
                details=details,
            )

    def _list_messages_sync(
        self,
        room_id: str,
        limit: int,
        before: str | None,
    ) -> tuple[list[Message], str | None]:
        with closing(self._connect()) as connection, connection:
            room = connection.execute("SELECT 1 FROM rooms WHERE id = ?", (room_id,)).fetchone()
            if room is None:
                raise RoomNotFoundError(room_id)

            if before:
                cursor = connection.execute(
                    "SELECT created_at, id FROM messages WHERE room_id = ? AND id = ?", (room_id, before)
                ).fetchone()
                if cursor is None:
                    raise InvalidCursorError(before)
                rows = connection.execute(
                    """
                    SELECT id, room_id, sender, content, created_at, client_message_id
                    FROM messages
                    WHERE room_id = ? AND (created_at < ? OR (created_at = ? AND id < ?))
                    ORDER BY created_at DESC, id DESC
                    LIMIT ?
                    """,
                    (room_id, cursor["created_at"], cursor["created_at"], cursor["id"], limit + 1),
                ).fetchall()
            else:
                rows = connection.execute(
                    """
                    SELECT id, room_id, sender, content, created_at, client_message_id
                    FROM messages
                    WHERE room_id = ?
                    ORDER BY created_at DESC, id DESC
                    LIMIT ?
                    """,
                    (room_id, limit + 1),
                ).fetchall()

        has_more = len(rows) > limit
        page_rows = rows[:limit]
        messages = [self._message_from_row(row) for row in reversed(page_rows)]
        next_before = page_rows[-1]["id"] if has_more and page_rows else None
        return messages, next_before

    def _list_audit_events_sync(self, limit: int, before: str | None) -> tuple[list[AuditEvent], str | None]:
        with closing(self._connect()) as connection, connection:
            if before:
                cursor = connection.execute(
                    "SELECT created_at, id FROM audit_events WHERE id = ?", (before,)
                ).fetchone()
                if cursor is None:
                    raise InvalidAuditCursorError(before)
                rows = connection.execute(
                    """
                    SELECT id, action, actor, room_id, created_at, details_json
                    FROM audit_events
                    WHERE created_at < ? OR (created_at = ? AND id < ?)
                    ORDER BY created_at DESC, id DESC LIMIT ?
                    """,
                    (cursor["created_at"], cursor["created_at"], cursor["id"], limit + 1),
                ).fetchall()
            else:
                rows = connection.execute(
                    """
                    SELECT id, action, actor, room_id, created_at, details_json
                    FROM audit_events ORDER BY created_at DESC, id DESC LIMIT ?
                    """,
                    (limit + 1,),
                ).fetchall()
        has_more = len(rows) > limit
        page_rows = rows[:limit]
        events = [self._audit_from_row(row) for row in reversed(page_rows)]
        next_before = page_rows[-1]["id"] if has_more and page_rows else None
        return events, next_before

    def _run_retention_sync(self, cutoff: datetime, actor: str) -> int:
        with closing(self._connect()) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute("DELETE FROM messages WHERE created_at < ?", (cutoff.isoformat(),))
            deleted = cursor.rowcount
            self._insert_audit(
                connection,
                action="retention.executed",
                actor=actor,
                details={"deleted_messages": deleted, "cutoff": cutoff.isoformat()},
            )
        return int(deleted)

    def _insert_audit(
        self,
        connection: sqlite3.Connection,
        *,
        action: str,
        actor: str,
        room_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        connection.execute(
            """
            INSERT INTO audit_events (id, action, actor, room_id, created_at, details_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                uuid.uuid4().hex,
                action,
                actor,
                room_id,
                datetime.now(timezone.utc).isoformat(),
                json.dumps(details or {}, separators=(",", ":"), sort_keys=True),
            ),
        )
        connection.execute(
            """
            DELETE FROM audit_events WHERE id IN (
                SELECT id FROM audit_events ORDER BY created_at DESC, id DESC LIMIT -1 OFFSET ?
            )
            """,
            (self.max_audit_events,),
        )

    @staticmethod
    def _room_from_row(row: sqlite3.Row) -> Room:
        return Room(
            id=row["id"],
            name=row["name"],
            description=row["description"],
            created_at=datetime.fromisoformat(row["created_at"]),
            archived_at=datetime.fromisoformat(row["archived_at"]) if row["archived_at"] else None,
        )

    @staticmethod
    def _message_from_row(row: sqlite3.Row) -> Message:
        return Message(
            id=row["id"],
            room_id=row["room_id"],
            sender=row["sender"],
            content=row["content"],
            created_at=datetime.fromisoformat(row["created_at"]),
            client_message_id=row["client_message_id"],
        )

    @staticmethod
    def _audit_from_row(row: sqlite3.Row) -> AuditEvent:
        return AuditEvent(
            id=row["id"],
            action=row["action"],
            actor=row["actor"],
            room_id=row["room_id"],
            created_at=datetime.fromisoformat(row["created_at"]),
            details=json.loads(row["details_json"]),
        )
