# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Sequence-filtered delivery and teardown without a database."""

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest

from samsarix_chat_engine import ConnectionManager


def _manager():
    return ConnectionManager(max_connections=5, max_per_room=5, send_timeout=1, max_pending_events=1)


def _socket():
    return Mock(send_json=AsyncMock(), close=AsyncMock())


@pytest.mark.asyncio
@pytest.mark.parametrize("ready", [True, False])
@pytest.mark.parametrize("sequence", [9, 10, 11, 20, 21, None])
async def test_event_floor_is_exclusive_for_active_and_initializing_sockets(ready, sequence):
    manager = _manager()
    older, newer, legacy = _socket(), _socket(), _socket()
    for websocket, floor in ((older, 10), (newer, 20), (legacy, None)):
        await manager.register(websocket, "room", "A", after_sequence=floor, broadcast_ready=ready)
    event = {"type": "test"}
    await manager.broadcast("room", event, event_sequence=sequence)
    for websocket, floor in ((older, 10), (newer, 20), (legacy, None)):
        if not ready:
            websocket.send_json.assert_not_awaited()
            assert await manager.activate(websocket)
        if sequence is None or floor is None or sequence > floor:
            websocket.send_json.assert_awaited_once_with(event)
        else:
            websocket.send_json.assert_not_awaited()
    assert manager._pending_bytes == 0


@pytest.mark.asyncio
async def test_pre_admission_backlog_does_not_consume_buffer_or_close_new_socket():
    manager = _manager()
    websocket = _socket()
    await manager.register(websocket, "room", "A", after_sequence=100, broadcast_ready=False)
    for sequence in range(101):
        await manager.broadcast("room", {"type": "old"}, event_sequence=sequence)
    assert manager._pending_bytes == 0
    websocket.close.assert_not_awaited()
    await manager.broadcast("room", {"type": "new"}, event_sequence=101)
    assert await manager.activate(websocket)
    websocket.send_json.assert_awaited_once_with({"type": "new"})


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["room", "member"])
@pytest.mark.parametrize("sequence", [20, None])
async def test_lifecycle_closes_only_matching_pre_event_admissions(kind, sequence):
    manager = _manager()
    older, newer, peer, unrelated = [_socket() for _ in range(4)]
    for websocket, room, subject, floor in (
        (older, "room", "alice", 10),
        (newer, "room", "alice", 20),
        (peer, "room", "bob", 10),
        (unrelated, "other", "alice", 10),
    ):
        await manager.register(websocket, room, subject, subject, after_sequence=floor, broadcast_ready=False)
    event = {"type": "room.archived" if kind == "room" else "member.banned"}
    if kind == "room":
        await manager.close_room("room", event, event_sequence=sequence)
    else:
        assert await manager.close_member("room", "alice", event, event_sequence=sequence) == (
            2 if sequence is None else 1
        )
    closed = {older}
    if sequence is None:
        closed.add(newer)
    if kind == "room":
        closed.add(peer)
    for websocket in (older, newer, peer, unrelated):
        if websocket in closed:
            websocket.send_json.assert_awaited_once_with(event)
            websocket.close.assert_awaited_once()
            assert not await manager.activate(websocket)
        else:
            websocket.send_json.assert_not_awaited()
            websocket.close.assert_not_awaited()
            assert await manager.activate(websocket)


@pytest.mark.asyncio
async def test_lifecycle_filter_is_applied_under_the_registration_lock():
    manager = _manager()
    older, newer = _socket(), _socket()
    await manager.register(older, "room", "A", after_sequence=10)
    await manager._lock.acquire()
    registration = asyncio.create_task(manager.register(newer, "room", "B", after_sequence=30))
    await asyncio.sleep(0)
    closing = asyncio.create_task(manager.close_room("room", {"type": "room.archived"}, event_sequence=20))
    manager._lock.release()
    assert await asyncio.wait_for(registration, 1)
    await asyncio.wait_for(closing, 1)
    older.close.assert_awaited_once()
    newer.close.assert_not_awaited()
    assert manager.active_connections == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["register", "broadcast", "room", "member"])
@pytest.mark.parametrize("value", [-1, True, 1.5, "10"])
async def test_invalid_sequence_fails_before_mutating_connections(operation, value):
    manager = _manager()
    websocket = _socket()
    await manager.register(websocket, "room", "A", "alice", after_sequence=10)
    with pytest.raises(ValueError, match="nonnegative integer"):
        if operation == "register":
            await manager.register(_socket(), "room", "B", after_sequence=value)
        elif operation == "broadcast":
            await manager.broadcast("room", {"type": "test"}, event_sequence=value)
        elif operation == "room":
            await manager.close_room("room", {"type": "test"}, event_sequence=value)
        else:
            await manager.close_member("room", "alice", {"type": "test"}, event_sequence=value)
    assert manager.active_connections == 1
    websocket.send_json.assert_not_awaited()
    websocket.close.assert_not_awaited()
