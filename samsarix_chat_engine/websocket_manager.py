# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""In-process, room-scoped WebSocket connection management."""

from __future__ import annotations

import asyncio
import json
import logging
from collections import defaultdict, deque
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
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


class _PendingEvent(NamedTuple):
    payload: str
    size: int


@dataclass(slots=True)
class ConnectionMetadata:
    room_id: str
    username: str
    subject: str | None
    connection_id: str | None
    operation_lock: asyncio.Lock
    broadcast_ready: bool
    after_sequence: int | None = None
    pending: deque[_PendingEvent] = field(default_factory=deque)
    pending_bytes: int = 0
    inflight: _PendingEvent | None = None


class ConnectionManager:
    """Track bounded connections and broadcast without blocking on slow peers."""

    def __init__(
        self,
        *,
        max_connections: int,
        max_per_room: int,
        send_timeout: float,
        max_pending_events: int = 64,
        max_pending_bytes: int = 262_144,
        max_total_pending_bytes: int = 8_388_608,
    ) -> None:
        if any(
            type(limit) is not int or limit < 1
            for limit in (max_pending_events, max_pending_bytes, max_total_pending_bytes)
        ):
            raise ValueError("pending broadcast limits must be positive integers")
        self.max_connections = max_connections
        self.max_per_room = max_per_room
        self.send_timeout = send_timeout
        self.max_pending_events = max_pending_events
        self.max_pending_bytes = max_pending_bytes
        self.max_total_pending_bytes = max_total_pending_bytes
        self._pending_bytes = 0
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
        after_sequence: int | None = None,
        admission_check: Callable[[], bool] | None = None,
    ) -> bool:
        """Register an accepted socket; reject duplicates, failed guards, or capacity."""

        _validate_sequence(after_sequence)
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
                after_sequence,
            )
            return True

    async def activate(self, websocket: WebSocket) -> bool:
        """Flush queued broadcasts after initial frames, within one send deadline."""

        async with self._lock:
            metadata = self._metadata.get(websocket)
            if metadata is None:
                return False
            if metadata.broadcast_ready:
                return True
        try:
            return await asyncio.wait_for(self._activate(websocket, metadata), timeout=self.send_timeout)
        except asyncio.CancelledError:
            await self.close(websocket, code=1012, reason="Initialization cancelled")
            raise
        except Exception as exc:
            logger.info("Closing incomplete WebSocket initialization: %s", type(exc).__name__)
            await self.close(websocket, code=1013, reason="History synchronization unavailable")
            return False

    async def _activate(self, websocket: WebSocket, metadata: ConnectionMetadata) -> bool:
        async with metadata.operation_lock:
            while True:
                async with self._lock:
                    if self._metadata.get(websocket) is not metadata:
                        return False
                    if not metadata.pending:
                        metadata.broadcast_ready = True
                        return True
                    event = metadata.pending.popleft()
                    metadata.inflight = event
                try:
                    await websocket.send_json(json.loads(event.payload))
                finally:
                    # No suspension here: repeated cancellation cannot leak budget.
                    # Retain the charge during I/O, including after detachment.
                    metadata.inflight = None
                    metadata.pending_bytes -= event.size
                    self._pending_bytes -= event.size
                    del event

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
        self._discard_pending(metadata)
        room_connections = self._rooms.get(metadata.room_id)
        if room_connections is not None:
            room_connections.discard(websocket)
            if not room_connections:
                self._rooms.pop(metadata.room_id, None)
        return metadata

    def _discard_pending(self, metadata: ConnectionMetadata) -> None:
        """Release queued budget while preserving any activation-owned send."""

        retained = metadata.inflight.size if metadata.inflight is not None else 0
        self._pending_bytes -= metadata.pending_bytes - retained
        metadata.pending_bytes = retained
        metadata.pending.clear()

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
        event_sequence: int | None = None,
    ) -> None:
        """Send to active peers; queue bounded snapshots for initializing peers."""

        _validate_sequence(event_sequence)
        recipients: list[tuple[WebSocket, asyncio.Lock]] = []
        overflow: list[tuple[WebSocket, asyncio.Lock]] = []
        buffered: _PendingEvent | None = None
        invalid_payload = False
        async with self._lock:
            for connection in tuple(self._rooms.get(room_id, ())):
                metadata = self._metadata[connection]
                if not _accepts_sequence(metadata, event_sequence):
                    continue
                if connection is exclude or (
                    exclude_connection_id is not None and metadata.connection_id == exclude_connection_id
                ):
                    continue
                if metadata.broadcast_ready:
                    recipients.append((connection, metadata.operation_lock))
                    continue
                if buffered is None and not invalid_payload:
                    try:
                        payload = json.dumps(event, ensure_ascii=False, separators=(",", ":"))
                        buffered = _PendingEvent(payload, len(payload.encode("utf-8")))
                    except (TypeError, ValueError, UnicodeError, RecursionError):
                        invalid_payload = True
                if (
                    buffered is None
                    or len(metadata.pending) + (metadata.inflight is not None) >= self.max_pending_events
                    or metadata.pending_bytes + buffered.size > self.max_pending_bytes
                    or self._pending_bytes + buffered.size > self.max_total_pending_bytes
                ):
                    self._detach_connection(connection)
                    overflow.append((connection, metadata.operation_lock))
                    continue
                metadata.pending.append(buffered)
                metadata.pending_bytes += buffered.size
                self._pending_bytes += buffered.size

        async def deliver() -> None:
            await asyncio.gather(
                *(self._send_with_lock(connection, event, operation_lock) for connection, operation_lock in recipients),
                *(
                    self._close_with_lock(
                        connection, operation_lock, code=1013, reason="History synchronization overflow"
                    )
                    for connection, operation_lock in overflow
                ),
            )

        if overflow:
            # Once detached, this caller owns bounded physical closure even if cancelled.
            await _finish_connection_cleanup(deliver())
        elif recipients:
            await deliver()

    async def close_all(self) -> None:
        """Close and forget all connections during graceful shutdown."""

        async with self._lock:
            connections = tuple(
                (connection, metadata.operation_lock) for connection, metadata in self._metadata.items()
            )
            for metadata in self._metadata.values():
                self._discard_pending(metadata)
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
        event_sequence: int | None = None,
    ) -> None:
        """Notify, remove, and deterministically close every socket in one room."""

        _validate_sequence(event_sequence)
        connections = await self._detach_room_connections(room_id, event_sequence=event_sequence)
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
        event_sequence: int | None = None,
    ) -> int:
        """Notify and close every socket for one authenticated room subject."""

        _validate_sequence(event_sequence)
        connections = await self._detach_room_connections(room_id, subject=subject, event_sequence=event_sequence)
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
        event_sequence: int | None = None,
    ) -> tuple[tuple[WebSocket, asyncio.Lock], ...]:
        """Atomically detach all room sockets or only those for one subject."""

        async with self._lock:
            room_connections = self._rooms.get(room_id)
            if room_connections is None:
                return ()
            connections = tuple(
                (connection, self._metadata[connection].operation_lock)
                for connection in room_connections
                if (subject is None or self._metadata[connection].subject == subject)
                and _accepts_sequence(self._metadata[connection], event_sequence)
            )
            for connection, _operation_lock in connections:
                metadata = self._metadata.pop(connection)
                self._discard_pending(metadata)
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


def _validate_sequence(sequence: int | None) -> None:
    if sequence is not None and (type(sequence) is not int or sequence < 0):
        raise ValueError("event sequence must be a nonnegative integer or None")


def _accepts_sequence(metadata: ConnectionMetadata, event_sequence: int | None) -> bool:
    return event_sequence is None or metadata.after_sequence is None or event_sequence > metadata.after_sequence
