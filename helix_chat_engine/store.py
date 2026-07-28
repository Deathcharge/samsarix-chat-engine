"""Bounded SQLite persistence for rooms and messages."""

from __future__ import annotations

import asyncio
import sqlite3
import uuid
from collections.abc import Callable
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import TypeVar

from .models import Message, Room, RoomCreate

T = TypeVar("T")


class StoreError(RuntimeError):
    """Base error for storage operations."""


class RoomNotFoundError(StoreError):
    """Raised when a requested room does not exist."""


class RoomAlreadyExistsError(StoreError):
    """Raised when a caller-selected room ID is already present."""


class RoomCapacityError(StoreError):
    """Raised when the configured room cap has been reached."""


class InvalidCursorError(StoreError):
    """Raised when a message pagination cursor is unknown for a room."""


class ChatStore:
    """Small asynchronous facade over a per-operation SQLite connection."""

    def __init__(
        self,
        database_path: Path,
        *,
        max_rooms: int,
        max_stored_messages: int,
        max_stored_messages_per_room: int,
    ) -> None:
        self.database_path = database_path
        self.max_rooms = max_rooms
        self.max_stored_messages = max_stored_messages
        self.max_stored_messages_per_room = max_stored_messages_per_room
        self._write_lock = asyncio.Lock()

    async def initialize(self) -> None:
        """Create the database directory and idempotent schema."""

        await asyncio.to_thread(self._initialize_sync)

    async def check_ready(self) -> bool:
        """Return whether the database can execute a trivial query."""

        try:
            await asyncio.to_thread(self._run, lambda connection: connection.execute("SELECT 1").fetchone())
        except (OSError, sqlite3.Error):
            return False
        return True

    async def create_room(self, payload: RoomCreate) -> Room:
        room_id = payload.id or uuid.uuid4().hex
        async with self._write_lock:
            return await asyncio.to_thread(self._create_room_sync, room_id, payload)

    async def get_room(self, room_id: str) -> Room | None:
        row = await asyncio.to_thread(
            self._run,
            lambda connection: connection.execute(
                "SELECT id, name, description, created_at FROM rooms WHERE id = ?", (room_id,)
            ).fetchone(),
        )
        return self._room_from_row(row) if row else None

    async def list_rooms(self, *, limit: int = 100) -> list[Room]:
        rows = await asyncio.to_thread(
            self._run,
            lambda connection: connection.execute(
                "SELECT id, name, description, created_at FROM rooms ORDER BY created_at, id LIMIT ?", (limit,)
            ).fetchall(),
        )
        return [self._room_from_row(row) for row in rows]

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

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path, timeout=5.0)
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
                PRAGMA user_version = 1;
                """
            )

    def _create_room_sync(self, room_id: str, payload: RoomCreate) -> Room:
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
        return Room(id=room_id, name=payload.name, description=payload.description, created_at=created_at)

    def _create_message_sync(
        self,
        room_id: str,
        sender: str,
        content: str,
        client_message_id: str | None,
    ) -> tuple[Message, bool]:
        with closing(self._connect()) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            room = connection.execute("SELECT 1 FROM rooms WHERE id = ?", (room_id,)).fetchone()
            if room is None:
                raise RoomNotFoundError(room_id)

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
            self._trim_messages(connection, room_id)
            return message, True

    def _trim_messages(self, connection: sqlite3.Connection, room_id: str) -> None:
        connection.execute(
            """
            DELETE FROM messages
            WHERE id IN (
                SELECT id FROM messages WHERE room_id = ?
                ORDER BY created_at, id
                LIMIT MAX(0, (SELECT COUNT(*) FROM messages WHERE room_id = ?) - ?)
            )
            """,
            (room_id, room_id, self.max_stored_messages_per_room),
        )
        connection.execute(
            """
            DELETE FROM messages
            WHERE id IN (
                SELECT id FROM messages ORDER BY created_at, id
                LIMIT MAX(0, (SELECT COUNT(*) FROM messages) - ?)
            )
            """,
            (self.max_stored_messages,),
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

    @staticmethod
    def _room_from_row(row: sqlite3.Row) -> Room:
        return Room(
            id=row["id"],
            name=row["name"],
            description=row["description"],
            created_at=datetime.fromisoformat(row["created_at"]),
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
