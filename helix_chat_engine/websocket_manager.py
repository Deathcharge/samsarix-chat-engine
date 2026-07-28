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
        self._metadata: dict[WebSocket, tuple[str, str]] = {}
        self._lock = asyncio.Lock()

    async def register(self, websocket: WebSocket, room_id: str, username: str) -> bool:
        """Register an already-accepted socket, returning false at capacity."""

        async with self._lock:
            if len(self._metadata) >= self.max_connections or len(self._rooms[room_id]) >= self.max_per_room:
                if not self._rooms[room_id]:
                    self._rooms.pop(room_id, None)
                return False
            self._rooms[room_id].add(websocket)
            self._metadata[websocket] = (room_id, username)
            return True

    async def unregister(self, websocket: WebSocket) -> tuple[str, str] | None:
        """Remove a socket and return its room/user metadata once."""

        async with self._lock:
            metadata = self._metadata.pop(websocket, None)
            if metadata is None:
                return None
            room_id, _ = metadata
            room_connections = self._rooms.get(room_id)
            if room_connections is not None:
                room_connections.discard(websocket)
                if not room_connections:
                    self._rooms.pop(room_id, None)
            return metadata

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
    ) -> None:
        """Broadcast to a bounded snapshot of one room's connections."""

        async with self._lock:
            recipients = tuple(connection for connection in self._rooms.get(room_id, ()) if connection is not exclude)
        if recipients:
            await asyncio.gather(*(self.send(connection, event) for connection in recipients))

    async def close_all(self) -> None:
        """Close and forget all connections during graceful shutdown."""

        async with self._lock:
            connections = tuple(self._metadata)
            self._metadata.clear()
            self._rooms.clear()
        await asyncio.gather(*(self._close(connection) for connection in connections))

    async def _close(self, websocket: WebSocket) -> None:
        try:
            await asyncio.wait_for(
                websocket.close(code=1012, reason="Service restarting"),
                timeout=self.send_timeout,
            )
        except Exception:
            logger.debug("WebSocket was already closed during shutdown")

    @property
    def active_connections(self) -> int:
        return len(self._metadata)

    def room_connections(self, room_id: str) -> int:
        return len(self._rooms.get(room_id, ()))
