# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""In-process, room-scoped WebSocket connection management."""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from typing import Any

from fastapi import WebSocket

logger = logging.getLogger(__name__)


class ConnectionManager:
    """Track bounded connections and broadcast without blocking on slow peers."""

    def __init__(self, *, max_connections: int, max_per_room: int, send_timeout: float) -> None:
        self.max_connections = max_connections
        self.max_per_room = max_per_room
        self.send_timeout = send_timeout
        self._rooms: dict[str, set[WebSocket]] = defaultdict(set)
        self._metadata: dict[WebSocket, tuple[str, str, str | None, str | None]] = {}
        self._lock = asyncio.Lock()

    async def register(
        self,
        websocket: WebSocket,
        room_id: str,
        username: str,
        subject: str | None = None,
        *,
        connection_id: str | None = None,
    ) -> bool:
        """Register an already-accepted socket, returning false at capacity."""

        async with self._lock:
            if len(self._metadata) >= self.max_connections or len(self._rooms[room_id]) >= self.max_per_room:
                if not self._rooms[room_id]:
                    self._rooms.pop(room_id, None)
                return False
            self._rooms[room_id].add(websocket)
            self._metadata[websocket] = (room_id, username, subject, connection_id)
            return True

    async def unregister(self, websocket: WebSocket) -> tuple[str, str] | None:
        """Remove a socket and return its room/user metadata once."""

        async with self._lock:
            metadata = self._metadata.pop(websocket, None)
            if metadata is None:
                return None
            room_id, _, _, _ = metadata
            room_connections = self._rooms.get(room_id)
            if room_connections is not None:
                room_connections.discard(websocket)
                if not room_connections:
                    self._rooms.pop(room_id, None)
            return metadata[0], metadata[1]

    async def send(self, websocket: WebSocket, event: dict[str, Any]) -> bool:
        """Send one event with a timeout and evict a failed connection."""

        try:
            await asyncio.wait_for(websocket.send_json(event), timeout=self.send_timeout)
            return True
        except Exception as exc:
            logger.info("Dropping unavailable WebSocket connection: %s", type(exc).__name__)
            await self.unregister(websocket)
            return False

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
                connection
                for connection in self._rooms.get(room_id, ())
                if connection is not exclude
                and (exclude_connection_id is None or self._metadata[connection][3] != exclude_connection_id)
            )
        if recipients:
            await asyncio.gather(*(self.send(connection, event) for connection in recipients))

    async def close_all(self) -> None:
        """Close and forget all connections during graceful shutdown."""

        async with self._lock:
            connections = tuple(self._metadata)
            self._metadata.clear()
            self._rooms.clear()
        await asyncio.gather(*(self._close(connection) for connection in connections))

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
                *(self._notify_and_close(connection, event, code=code, reason=reason) for connection in connections)
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
                *(self._notify_and_close(connection, event, code=code, reason=reason) for connection in connections)
            )
        return len(connections)

    async def _detach_room_connections(
        self,
        room_id: str,
        *,
        subject: str | None = None,
    ) -> tuple[WebSocket, ...]:
        """Atomically detach all room sockets or only those for one subject."""

        async with self._lock:
            room_connections = self._rooms.get(room_id)
            if room_connections is None:
                return ()
            connections = tuple(
                connection
                for connection in room_connections
                if subject is None or self._metadata[connection][2] == subject
            )
            for connection in connections:
                self._metadata.pop(connection, None)
                room_connections.discard(connection)
            if not room_connections:
                self._rooms.pop(room_id, None)
            return connections

    async def _close(self, websocket: WebSocket) -> None:
        try:
            await asyncio.wait_for(
                websocket.close(code=1012, reason="Service restarting"),
                timeout=self.send_timeout,
            )
        except Exception:
            logger.debug("WebSocket was already closed during shutdown")

    async def _notify_and_close(
        self,
        websocket: WebSocket,
        event: dict[str, Any],
        *,
        code: int,
        reason: str,
    ) -> None:
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
