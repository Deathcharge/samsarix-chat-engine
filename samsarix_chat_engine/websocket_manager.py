# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""In-process, room-scoped WebSocket connection management."""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from collections.abc import Callable, Coroutine
from typing import Any, NamedTuple

from anyio import CancelScope
from fastapi import WebSocket

logger = logging.getLogger(__name__)


async def _finish_connection_cleanup(cleanup: Coroutine[Any, Any, object]) -> None:
    """Finish bounded cleanup under ASGI scopes or repeated direct cancellation."""

    with CancelScope(shield=True):
        task = asyncio.create_task(cleanup, name="samsarix-connection-cleanup")
        cancelled = False
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                cancelled = True
        task.result()
        if cancelled:
            raise asyncio.CancelledError


class ConnectionMetadata(NamedTuple):
    room_id: str
    username: str
    subject: str | None
    connection_id: str | None
    operation_lock: asyncio.Lock
    broadcast_ready: bool


class ConnectionManager:
    """Track bounded connections and broadcast without blocking on slow peers."""

    def __init__(self, *, max_connections: int, max_per_room: int, send_timeout: float) -> None:
        self.max_connections = max_connections
        self.max_per_room = max_per_room
        self.send_timeout = send_timeout
        self._rooms: dict[str, set[WebSocket]] = defaultdict(set)
        self._metadata: dict[WebSocket, ConnectionMetadata] = {}
        self._lock = asyncio.Lock()

    async def register(
        self,
        websocket: WebSocket,
        room_id: str,
        username: str,
        subject: str | None = None,
        *,
        connection_id: str | None = None,
        broadcast_ready: bool = True,
        admission_check: Callable[[], bool] | None = None,
    ) -> bool:
        """Register an accepted socket; reject duplicates, failed guards, or capacity."""

        async with self._lock:
            if websocket in self._metadata:
                return False
            # Check under the same lock that fencing uses to detach sockets.
            if admission_check is not None and not admission_check():
                return False
            if len(self._metadata) >= self.max_connections or len(self._rooms[room_id]) >= self.max_per_room:
                if not self._rooms[room_id]:
                    self._rooms.pop(room_id, None)
                return False
            self._rooms[room_id].add(websocket)
            self._metadata[websocket] = ConnectionMetadata(
                room_id,
                username,
                subject,
                connection_id,
                asyncio.Lock(),
                broadcast_ready,
            )
            return True

    async def activate(self, websocket: WebSocket) -> bool:
        """Allow broadcasts only after the socket's initial handshake is sent."""

        async with self._lock:
            metadata = self._metadata.get(websocket)
            if metadata is None:
                return False
            self._metadata[websocket] = metadata._replace(broadcast_ready=True)
            return True

    async def unregister(self, websocket: WebSocket) -> tuple[str, str] | None:
        """Remove a socket and return its room/user metadata once."""

        async with self._lock:
            metadata = self._detach_connection(websocket)
        return (metadata.room_id, metadata.username) if metadata is not None else None

    def _detach_connection(self, websocket: WebSocket) -> ConnectionMetadata | None:
        """Remove one socket while the caller holds the manager lock."""

        metadata = self._metadata.pop(websocket, None)
        if metadata is None:
            return None
        room_connections = self._rooms.get(metadata.room_id)
        if room_connections is not None:
            room_connections.discard(websocket)
            if not room_connections:
                self._rooms.pop(metadata.room_id, None)
        return metadata

    async def send(self, websocket: WebSocket, event: dict[str, Any]) -> bool:
        """Send to a registered socket; detached/unknown sockets return false."""

        async with self._lock:
            metadata = self._metadata.get(websocket)
        if metadata is None:
            return False
        return await self._send_with_lock(websocket, event, metadata.operation_lock)

    async def close(
        self, websocket: WebSocket, *, code: int, reason: str, event: dict[str, Any] | None = None
    ) -> tuple[str, str] | None:
        """Detach/close once and return room/user metadata to the winning owner."""

        async with self._lock:
            metadata = self._detach_connection(websocket)
        if metadata is not None:
            await _finish_connection_cleanup(
                self._close_with_lock(websocket, metadata.operation_lock, code=code, reason=reason)
                if event is None
                else self._notify_and_close(websocket, metadata.operation_lock, event, code=code, reason=reason)
            )
            return metadata.room_id, metadata.username
        return None

    async def broadcast(
        self,
        room_id: str,
        event: dict[str, Any],
        *,
        exclude: WebSocket | None = None,
        exclude_connection_id: str | None = None,
    ) -> None:
        """Broadcast to a bounded snapshot of one room's connections."""

        async with self._lock:
            recipients = tuple(
                (connection, self._metadata[connection].operation_lock)
                for connection in self._rooms.get(room_id, ())
                if connection is not exclude
                and self._metadata[connection].broadcast_ready
                and (exclude_connection_id is None or self._metadata[connection].connection_id != exclude_connection_id)
            )
        if recipients:
            await asyncio.gather(
                *(self._send_with_lock(connection, event, operation_lock) for connection, operation_lock in recipients)
            )

    async def close_all(self) -> None:
        """Close and forget all connections during graceful shutdown."""

        async with self._lock:
            connections = tuple(
                (connection, metadata.operation_lock) for connection, metadata in self._metadata.items()
            )
            self._metadata.clear()
            self._rooms.clear()
        await asyncio.gather(
            *(self._close_with_lock(connection, operation_lock) for connection, operation_lock in connections)
        )

    async def close_room(
        self,
        room_id: str,
        event: dict[str, Any],
        *,
        code: int = 4409,
        reason: str = "Room archived",
    ) -> None:
        """Notify, remove, and deterministically close every socket in one room."""

        connections = await self._detach_room_connections(room_id)
        if connections:
            await asyncio.gather(
                *(
                    self._notify_and_close(connection, operation_lock, event, code=code, reason=reason)
                    for connection, operation_lock in connections
                )
            )

    async def close_member(
        self,
        room_id: str,
        subject: str,
        event: dict[str, Any],
        *,
        code: int = 4403,
        reason: str = "Room access revoked",
    ) -> int:
        """Notify and close every socket for one authenticated room subject."""

        connections = await self._detach_room_connections(room_id, subject=subject)
        if connections:
            await asyncio.gather(
                *(
                    self._notify_and_close(connection, operation_lock, event, code=code, reason=reason)
                    for connection, operation_lock in connections
                )
            )
        return len(connections)

    async def _detach_room_connections(
        self,
        room_id: str,
        *,
        subject: str | None = None,
    ) -> tuple[tuple[WebSocket, asyncio.Lock], ...]:
        """Atomically detach all room sockets or only those for one subject."""

        async with self._lock:
            room_connections = self._rooms.get(room_id)
            if room_connections is None:
                return ()
            connections = tuple(
                (connection, self._metadata[connection].operation_lock)
                for connection in room_connections
                if subject is None or self._metadata[connection].subject == subject
            )
            for connection, _operation_lock in connections:
                self._metadata.pop(connection, None)
                room_connections.discard(connection)
            if not room_connections:
                self._rooms.pop(room_id, None)
            return connections

    async def _send_with_lock(
        self,
        websocket: WebSocket,
        event: dict[str, Any],
        operation_lock: asyncio.Lock,
    ) -> bool:
        try:
            async with operation_lock:
                async with self._lock:
                    metadata = self._metadata.get(websocket)
                    if metadata is None or metadata.operation_lock is not operation_lock:
                        return False
                await asyncio.wait_for(websocket.send_json(event), timeout=self.send_timeout)
            return True
        except Exception as exc:
            logger.info("Dropping unavailable WebSocket connection: %s", type(exc).__name__)
            await self.close(websocket, code=1013, reason="Client unavailable")
            return False

    async def _close_with_lock(
        self,
        websocket: WebSocket,
        operation_lock: asyncio.Lock,
        *,
        code: int = 1012,
        reason: str = "Service restarting",
    ) -> None:
        async with operation_lock:
            try:
                await asyncio.wait_for(
                    websocket.close(code=code, reason=reason),
                    timeout=self.send_timeout,
                )
            except Exception:
                logger.debug("WebSocket was already closed during shutdown")

    async def _notify_and_close(
        self,
        websocket: WebSocket,
        operation_lock: asyncio.Lock,
        event: dict[str, Any],
        *,
        code: int,
        reason: str,
    ) -> None:
        async with operation_lock:
            try:
                await asyncio.wait_for(websocket.send_json(event), timeout=self.send_timeout)
            except Exception:
                logger.debug("WebSocket notification failed during room lifecycle change")
            try:
                await asyncio.wait_for(websocket.close(code=code, reason=reason), timeout=self.send_timeout)
            except Exception:
                logger.debug("WebSocket was already closed during room lifecycle change")

    @property
    def active_connections(self) -> int:
        return len(self._metadata)

    def room_connections(self, room_id: str) -> int:
        return len(self._rooms.get(room_id, ()))
