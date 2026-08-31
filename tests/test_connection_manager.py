"""Focused tests for bounded connection cleanup and failure handling."""

import asyncio
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


class ContendedFakeWebSocket(FakeWebSocket):
    def __init__(self) -> None:
        super().__init__()
        self.active_operations = 0
        self.max_active_operations = 0

    async def _enter_operation(self) -> None:
        self.active_operations += 1
        self.max_active_operations = max(self.max_active_operations, self.active_operations)
        await asyncio.sleep(0.01)

    async def send_json(self, event: dict[str, object]) -> None:
        await self._enter_operation()
        try:
            self.sent.append(event)
        finally:
            self.active_operations -= 1

    async def close(self, *, code: int, reason: str) -> None:
        await self._enter_operation()
        try:
            self.closed.append((code, reason))
        finally:
            self.active_operations -= 1


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
    assert broken.closed == [(1013, "Client unavailable")]
    assert await manager.unregister(websocket) is None


@pytest.mark.asyncio
async def test_broadcast_can_exclude_an_origin_connection_id() -> None:
    manager = ConnectionManager(max_connections=2, max_per_room=2, send_timeout=0.1)
    origin = FakeWebSocket()
    peer = FakeWebSocket()
    await manager.register(as_websocket(origin), "room", "A", connection_id="socket-a")
    await manager.register(as_websocket(peer), "room", "B", connection_id="socket-b")

    await manager.broadcast("room", {"type": "typing.started"}, exclude_connection_id="socket-a")

    assert origin.sent == []
    assert peer.sent == [{"type": "typing.started"}]


@pytest.mark.asyncio
async def test_pending_connection_receives_initial_frames_before_broadcasts() -> None:
    manager = ConnectionManager(max_connections=2, max_per_room=2, send_timeout=0.1)
    pending = FakeWebSocket()
    websocket = as_websocket(pending)
    await manager.register(websocket, "room", "Pending", broadcast_ready=False)

    await manager.broadcast("room", {"type": "presence.joined"})
    assert pending.sent == []
    assert await manager.send(websocket, {"type": "ready"}) is True
    assert await manager.send(websocket, {"type": "history"}) is True
    assert await manager.activate(websocket) is True
    await manager.broadcast("room", {"type": "message.created"})

    assert pending.sent == [
        {"type": "ready"},
        {"type": "history"},
        {"type": "message.created"},
    ]


@pytest.mark.asyncio
async def test_send_and_close_are_serialized_per_connection() -> None:
    manager = ConnectionManager(max_connections=1, max_per_room=1, send_timeout=0.1)
    target = ContendedFakeWebSocket()
    websocket = as_websocket(target)
    await manager.register(websocket, "room", "A")

    send_task = asyncio.create_task(manager.send(websocket, {"type": "lease.error"}))
    await asyncio.sleep(0)
    await manager.close(websocket, code=1012, reason="Storage unavailable")
    assert await send_task is True

    assert target.max_active_operations == 1
    assert target.sent == [{"type": "lease.error"}]
    assert target.closed == [(1012, "Storage unavailable")]
    assert manager.active_connections == 0


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


@pytest.mark.asyncio
async def test_close_room_attempts_close_when_notification_fails() -> None:
    manager = ConnectionManager(max_connections=1, max_per_room=1, send_timeout=0.1)
    target = FakeWebSocket(fail_send=True)
    await manager.register(as_websocket(target), "target", "A")

    await manager.close_room("target", {"type": "room.archived"})

    assert target.closed == [(4409, "Room archived")]
    assert manager.active_connections == 0


@pytest.mark.asyncio
async def test_close_member_targets_subject_without_disrupting_room() -> None:
    manager = ConnectionManager(max_connections=3, max_per_room=3, send_timeout=0.1)
    target = FakeWebSocket()
    second_target = FakeWebSocket()
    peer = FakeWebSocket()
    await manager.register(as_websocket(target), "room", "Target", "subject-1")
    await manager.register(as_websocket(second_target), "room", "Target 2", "subject-1")
    await manager.register(as_websocket(peer), "room", "Peer", "subject-2")

    closed = await manager.close_member("room", "subject-1", {"type": "member.banned"})

    assert closed == 2
    assert target.sent == [{"type": "member.banned"}]
    assert target.closed == [(4403, "Room access revoked")]
    assert second_target.sent == [{"type": "member.banned"}]
    assert second_target.closed == [(4403, "Room access revoked")]
    assert peer.sent == []
    assert peer.closed == []
    assert manager.active_connections == 1
    assert manager.room_connections("room") == 1

    assert await manager.close_member("room", "subject-2", {"type": "member.banned"}) == 1
    assert manager.active_connections == 0
    assert manager.room_connections("room") == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["room", "member", "all", "single"])
async def test_detached_socket_rejects_sends_and_duplicate_close(operation: str) -> None:
    manager = ConnectionManager(max_connections=1, max_per_room=1, send_timeout=1)
    close_started = asyncio.Event()
    finish_close = asyncio.Event()

    class ClosingSocket(FakeWebSocket):
        async def close(self, *, code: int, reason: str) -> None:
            self.closed.append((code, reason))
            close_started.set()
            await finish_close.wait()

    target = ClosingSocket()
    websocket = as_websocket(target)
    assert await manager.register(websocket, "room", "A", "subject")
    if operation == "room":
        closing = asyncio.create_task(manager.close_room("room", {"type": "room.archived"}))
    elif operation == "member":
        closing = asyncio.create_task(manager.close_member("room", "subject", {"type": "member.banned"}))
    elif operation == "all":
        closing = asyncio.create_task(manager.close_all())
    else:
        closing = asyncio.create_task(manager.close(websocket, code=1012, reason="Storage unavailable"))
    try:
        await asyncio.wait_for(close_started.wait(), 1)
        assert manager.active_connections == 0
        assert not await manager.send(websocket, {"type": "must.not.send"})
        await asyncio.wait_for(manager.close(websocket, code=1000, reason="duplicate"), 0.2)
        assert len(target.closed) == 1
        assert not await manager.activate(websocket)
        assert all(event["type"] != "must.not.send" for event in target.sent)
    finally:
        finish_close.set()
        await asyncio.wait_for(closing, 1)


@pytest.mark.asyncio
async def test_queued_broadcast_snapshot_is_discarded_after_room_detaches() -> None:
    manager = ConnectionManager(max_connections=1, max_per_room=1, send_timeout=1)
    target = FakeWebSocket()
    websocket = as_websocket(target)
    await manager.register(websocket, "room", "A")
    operation_lock = manager._metadata[websocket].operation_lock
    await operation_lock.acquire()
    broadcast = asyncio.create_task(manager.broadcast("room", {"type": "message.created"}))
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    close = asyncio.create_task(manager.close_room("room", {"type": "room.archived"}))
    try:
        await asyncio.sleep(0)
        assert manager.active_connections == 0
        operation_lock.release()
        await asyncio.wait_for(asyncio.gather(broadcast, close), 1)
        assert target.sent == [{"type": "room.archived"}]
        assert target.closed == [(4409, "Room archived")]
    finally:
        if operation_lock.locked():
            operation_lock.release()
        for task in (broadcast, close):
            task.cancel()
        await asyncio.gather(broadcast, close, return_exceptions=True)


@pytest.mark.asyncio
async def test_unknown_sockets_are_inert_and_duplicate_registration_preserves_lock() -> None:
    manager = ConnectionManager(max_connections=2, max_per_room=2, send_timeout=0.1)
    target = FakeWebSocket()
    websocket = as_websocket(target)
    assert not await manager.send(websocket, {"type": "not.registered"})
    await manager.close(websocket, code=1000, reason="not registered")
    assert target.sent == target.closed == []
    assert await manager.register(websocket, "room", "A")
    metadata = manager._metadata[websocket]
    assert not await manager.register(websocket, "other", "B")
    assert manager._metadata[websocket] is metadata
    assert manager.room_connections("room") == 1
    assert manager.room_connections("other") == 0


@pytest.mark.asyncio
async def test_cancelled_closer_keeps_ownership_until_physical_close_finishes() -> None:
    manager = ConnectionManager(max_connections=1, max_per_room=1, send_timeout=1)
    started = asyncio.Event()
    finish = asyncio.Event()

    class ClosingSocket(FakeWebSocket):
        async def close(self, *, code: int, reason: str) -> None:
            started.set()
            await finish.wait()
            self.closed.append((code, reason))

    target = ClosingSocket()
    websocket = as_websocket(target)
    await manager.register(websocket, "room", "A")
    task = asyncio.create_task(manager.close(websocket, code=1012, reason="Storage unavailable"))
    try:
        await asyncio.wait_for(started.wait(), 1)
        assert manager.active_connections == 0
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        task.cancel()
        finish.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 1)
        assert target.closed == [(1012, "Storage unavailable")]
        assert await manager.close(websocket, code=1000, reason="duplicate") is None
    finally:
        finish.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
