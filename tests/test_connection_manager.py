"""Focused tests for bounded connection cleanup and failure handling."""

from typing import cast

import pytest
from fastapi import WebSocket

from samsarix_chat_engine import ConnectionManager


class FakeWebSocket:
    def __init__(self, *, fail_send: bool = False) -> None:
        self.fail_send = fail_send
        self.sent: list[dict[str, object]] = []
        self.closed: list[tuple[int, str]] = []

    async def send_json(self, event: dict[str, object]) -> None:
        if self.fail_send:
            raise RuntimeError("closed")
        self.sent.append(event)

    async def close(self, *, code: int, reason: str) -> None:
        self.closed.append((code, reason))


def as_websocket(fake: FakeWebSocket) -> WebSocket:
    return cast(WebSocket, fake)


@pytest.mark.asyncio
async def test_registration_limits_broadcast_failure_and_shutdown() -> None:
    manager = ConnectionManager(max_connections=2, max_per_room=1, send_timeout=0.1)
    first = FakeWebSocket()
    rejected = FakeWebSocket()

    assert await manager.register(as_websocket(first), "room", "A") is True
    assert await manager.register(as_websocket(rejected), "room", "B") is False
    assert manager.active_connections == 1
    assert manager.room_connections("room") == 1

    await manager.broadcast("room", {"type": "test"})
    assert first.sent == [{"type": "test"}]

    await manager.close_all()
    assert first.closed == [(1012, "Service restarting")]
    assert manager.active_connections == 0


@pytest.mark.asyncio
async def test_failed_send_evicts_connection_and_unregister_is_idempotent() -> None:
    manager = ConnectionManager(max_connections=2, max_per_room=2, send_timeout=0.1)
    broken = FakeWebSocket(fail_send=True)
    websocket = as_websocket(broken)
    await manager.register(websocket, "room", "A")

    assert await manager.send(websocket, {"type": "test"}) is False
    assert manager.active_connections == 0
    assert await manager.unregister(websocket) is None


@pytest.mark.asyncio
async def test_close_room_notifies_and_removes_only_target_room() -> None:
    manager = ConnectionManager(max_connections=3, max_per_room=3, send_timeout=0.1)
    target = FakeWebSocket()
    other = FakeWebSocket()
    await manager.register(as_websocket(target), "target", "A")
    await manager.register(as_websocket(other), "other", "B")

    await manager.close_room("target", {"type": "room.archived"})

    assert target.sent == [{"type": "room.archived"}]
    assert target.closed == [(4409, "Room archived")]
    assert other.sent == []
    assert manager.active_connections == 1
    assert manager.room_connections("target") == 0
    assert manager.room_connections("other") == 1
