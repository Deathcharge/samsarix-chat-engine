# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Fault injection through the real socket handler, without a live database."""

from __future__ import annotations

import asyncio
import json
import sqlite3
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from anyio import CancelScope
from fastapi import WebSocket

from samsarix_chat_engine import Settings, create_app
from samsarix_chat_engine.models import RoomCreate


@pytest.fixture(params=["sqlite", "postgres"])
async def socket_app(request, settings: Settings, monkeypatch):
    settings = replace(settings, token_signing_secret="handshake-test-signing-secret-32-bytes")
    application = create_app(settings)
    runtime = None
    if request.param == "postgres":
        pytest.importorskip("psycopg")
        store = application.state.store
        reservations: set[str] = set()

        async def admit(manager, websocket, **kwargs):
            reservations.add(kwargs["connection_id"])
            return await manager.register(websocket, broadcast_ready=False, **kwargs)

        async def release(connection_id):
            reservations.discard(connection_id)
            return True

        runtime = SimpleNamespace(
            store=store,
            message_limiter=application.state.message_limiter,
            search_limiter=application.state.search_limiter,
            read_state_limiter=application.state.read_state_limiter,
            typing_limiter=application.state.typing_limiter,
            admit_connection=AsyncMock(side_effect=admit),
            release_connection=AsyncMock(side_effect=release),
            renew_connection=AsyncMock(),
            connection_counts=AsyncMock(return_value=SimpleNamespace(room=1)),
            reservations=reservations,
        )
        monkeypatch.setattr("samsarix_chat_engine.postgres_runtime.PostgresApplicationRuntime", lambda *a, **k: runtime)
        application = create_app(
            Settings(
                storage_backend="postgres",
                postgres_url="postgresql://test:test@127.0.0.1/samsarix_test",
                postgres_instance_id="handshake-test",
                postgres_lease_seconds=3,
                token_signing_secret=settings.token_signing_secret,
            )
        )
    await application.state.store.initialize()
    await application.state.store.create_room(RoomCreate(id="room", name="Room"))
    try:
        yield application, runtime
    finally:
        await application.state.connections.close_all()
        await application.state.store.close()


def _socket(application):
    token = application.state.token_service.issue("Alice", rooms=["room"], permissions=["room:read", "room:write"])
    incoming = asyncio.Queue()
    incoming.put_nowait({"type": "websocket.connect"})
    sent = []

    async def send(message):
        sent.append(message)

    websocket = WebSocket(
        {"type": "websocket", "headers": [(b"authorization", f"Bearer {token}".encode())]},
        incoming.get,
        send,
    )
    endpoint = next(
        route.endpoint for route in application.routes if getattr(route, "path", None) == "/v1/rooms/{room_id}/ws"
    )
    return websocket, sent, incoming, endpoint


def _inject_phase(application, runtime, monkeypatch, phase, injected):
    store = application.state.store
    if phase in {"initial_room", "room_recheck", "initial_moderation", "moderation_recheck"}:
        method = "get_room" if "room" in phase else "get_member_moderation"
        original = getattr(store, method)
        target_call = 2 if "recheck" in phase else 1
        calls = 0

        async def query(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == target_call:
                await injected()
            return await original(*args, **kwargs)

        monkeypatch.setattr(store, method, query)
    elif phase == "history":
        monkeypatch.setattr(store, "list_messages", injected)
    elif phase == "counts":
        if runtime is None:
            pytest.skip("database-owned counts only apply to PostgreSQL")
        monkeypatch.setattr(runtime, "connection_counts", injected)
    else:
        original = application.state.connections.send

        async def send(websocket, event):
            if event["type"] == phase:
                await injected()
            return await original(websocket, event)

        monkeypatch.setattr(application.state.connections, "send", send)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "phase", ["initial_room", "initial_moderation", "room_recheck", "moderation_recheck", "history", "counts"]
)
async def test_storage_failure_closes_handshake_and_releases_capacity(socket_app, monkeypatch, caplog, phase):
    application, runtime = socket_app

    async def fail(*args, **kwargs):
        if runtime is not None:
            from samsarix_chat_engine.postgres import PostgresUnavailableError

            raise PostgresUnavailableError("private database credentials")
        raise sqlite3.OperationalError("private database path")

    _inject_phase(application, runtime, monkeypatch, phase, fail)
    websocket, sent, _, endpoint = _socket(application)
    try:
        await endpoint(websocket, "room", username=None)
    finally:
        assert application.state.connections.active_connections == 0
        if runtime is not None:
            assert runtime.reservations == set()
    frames = [json.loads(item["text"]) for item in sent if item["type"] == "websocket.send"]
    assert frames == [
        {"type": "error", "code": "storage_unavailable", "message": "Chat storage is temporarily unavailable"}
    ]
    assert sent[-1] == {"type": "websocket.close", "code": 1012, "reason": "Storage unavailable"}
    assert "private database" not in repr(sent) + caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "phase",
    [
        "initial_room",
        "initial_moderation",
        "room_recheck",
        "moderation_recheck",
        "history",
        "counts",
        "ready",
        "history_frame",
    ],
)
async def test_cancelled_handshake_releases_local_and_database_ownership(socket_app, monkeypatch, phase):
    application, runtime = socket_app
    entered = asyncio.Event()

    async def pause(*args, **kwargs):
        entered.set()
        await asyncio.Event().wait()

    if phase == "history_frame":
        original = application.state.connections.send

        async def send(websocket, event):
            if event["type"] == "history":
                await pause()
            return await original(websocket, event)

        monkeypatch.setattr(application.state.connections, "send", send)
    else:
        _inject_phase(application, runtime, monkeypatch, phase, pause)
    websocket, sent, _, endpoint = _socket(application)
    task = asyncio.create_task(endpoint(websocket, "room", username=None))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        admitted = phase not in {"initial_room", "initial_moderation"}
        assert application.state.connections.active_connections == int(admitted)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert application.state.connections.active_connections == 0
        assert sent[-1]["type"] == "websocket.close"
        assert sent[-1]["code"] == 1012
        if runtime is not None:
            assert runtime.reservations == set()
            assert runtime.release_connection.await_count == int(admitted)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("cancellation", ["task", "scope"])
async def test_cleanup_survives_repeated_cancellation_and_emits_no_phantom_presence(
    socket_app, monkeypatch, cancellation
):
    application, runtime = socket_app
    entered = asyncio.Event()
    cleanup_started = asyncio.Event()
    finish_cleanup = asyncio.Event()
    scope_ready = asyncio.Future()

    async def pause(*args, **kwargs):
        entered.set()
        await asyncio.Event().wait()

    _inject_phase(application, runtime, monkeypatch, "history", pause)
    manager = application.state.connections
    close = manager.close
    broadcasts = AsyncMock(wraps=manager.broadcast)
    monkeypatch.setattr(manager, "broadcast", broadcasts)

    async def slow_close(*args, **kwargs):
        cleanup_started.set()
        await finish_cleanup.wait()
        return await close(*args, **kwargs)

    monkeypatch.setattr(manager, "close", slow_close)
    websocket, sent, _, endpoint = _socket(application)

    async def run():
        with CancelScope() as scope:
            scope_ready.set_result(scope)
            await endpoint(websocket, "room", username=None)

    task = asyncio.create_task(run())
    try:
        scope = await scope_ready
        await asyncio.wait_for(entered.wait(), 1)
        if cancellation == "task":
            task.cancel()
        else:
            scope.cancel()
        await asyncio.wait_for(cleanup_started.wait(), 1)
        if cancellation == "task":
            task.cancel()
        else:
            scope.cancel()
        await asyncio.sleep(0)
        assert not task.done(), "request must retain ownership until cleanup completes"
        finish_cleanup.set()
        if cancellation == "task":
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, 1)
        else:
            await asyncio.wait_for(task, 1)
        assert manager.active_connections == 0
        assert sent[-1]["type"] == "websocket.close"
        broadcasts.assert_not_awaited()
        if runtime is not None:
            assert runtime.reservations == set()
            runtime.release_connection.assert_awaited_once()
    finally:
        finish_cleanup.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_receive_loop_storage_failure_closes_and_stops_heartbeat(socket_app, monkeypatch):
    application, runtime = socket_app
    broadcasts = AsyncMock(wraps=application.state.connections.broadcast)
    monkeypatch.setattr(application.state.connections, "broadcast", broadcasts)
    websocket, sent, incoming, endpoint = _socket(application)
    original_receive = websocket.receive

    async def receive():
        packet = await original_receive()
        if packet["type"] == "websocket.receive":
            raise sqlite3.OperationalError("private database path")
        return packet

    monkeypatch.setattr(websocket, "receive", receive)
    incoming.put_nowait({"type": "websocket.receive", "text": '{"type":"ping"}'})
    await endpoint(websocket, "room", username=None)
    frames = [json.loads(item["text"]) for item in sent if item["type"] == "websocket.send"]
    assert [frame["type"] for frame in frames] == ["ready", "history", "error"]
    assert frames[-1]["code"] == "storage_unavailable"
    assert sent[-1]["code"] == 1012
    assert application.state.connections.active_connections == 0
    assert not any(task.get_name().startswith("samsarix-connection-") for task in asyncio.all_tasks())
    if runtime is not None:
        assert runtime.reservations == set()
        runtime.release_connection.assert_awaited_once()
    else:
        assert [call.args[1]["type"] for call in broadcasts.await_args_list] == ["presence.joined", "presence.left"]


@pytest.mark.asyncio
async def test_unexpected_initialization_error_releases_ownership_and_closes_1011(socket_app, monkeypatch):
    application, runtime = socket_app
    monkeypatch.setattr(application.state.store, "list_messages", AsyncMock(side_effect=RuntimeError("unexpected")))
    websocket, sent, _, endpoint = _socket(application)
    with pytest.raises(RuntimeError, match="unexpected"):
        await endpoint(websocket, "room", username=None)
    assert application.state.connections.active_connections == 0
    assert sent[-1] == {"type": "websocket.close", "code": 1011, "reason": "Unexpected server error"}
    if runtime is not None:
        assert runtime.reservations == set()


@pytest.mark.asyncio
@pytest.mark.parametrize("socket_app", ["postgres"], indirect=True)
@pytest.mark.parametrize("state", ["archived", "missing"])
async def test_lifecycle_change_during_admission_uses_domain_close(socket_app, state):
    from samsarix_chat_engine.postgres_connections import ConnectionRoomUnavailableError

    application, runtime = socket_app
    room = await application.state.store.get_room("room")
    snapshot = room.model_copy(update={"archived_at": datetime.now(timezone.utc)}) if state == "archived" else None
    runtime.admit_connection.side_effect = ConnectionRoomUnavailableError(snapshot)
    websocket, sent, _, endpoint = _socket(application)
    await endpoint(websocket, "room", username=None)
    frames = [json.loads(item["text"]) for item in sent if item["type"] == "websocket.send"]
    assert len(frames) == 1
    assert frames[0]["type"] == "error"
    assert frames[0]["code"] == ("room_archived" if state == "archived" else "room_not_found")
    assert sent[-1]["code"] == (4409 if state == "archived" else 4404)
    assert application.state.connections.active_connections == 0
    runtime.release_connection.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("socket_app", ["postgres"], indirect=True)
@pytest.mark.parametrize("state", ["archived", "missing", "unavailable", "expired"])
async def test_heartbeat_distinguishes_room_lifecycle_from_storage_and_lease_failure(socket_app, monkeypatch, state):
    from samsarix_chat_engine.postgres import PostgresUnavailableError
    from samsarix_chat_engine.postgres_connections import ConnectionLeaseError, ConnectionRoomUnavailableError

    application, runtime = socket_app
    room = await application.state.store.get_room("room")
    snapshot = room.model_copy(update={"archived_at": datetime.now(timezone.utc)})
    runtime.renew_connection.side_effect = {
        "archived": ConnectionRoomUnavailableError(snapshot),
        "missing": ConnectionRoomUnavailableError(None),
        "unavailable": PostgresUnavailableError("private database credentials"),
        "expired": ConnectionLeaseError("expired"),
    }[state]
    websocket, sent, incoming, endpoint = _socket(application)
    original_close = websocket.close

    async def close(*args, **kwargs):
        await original_close(*args, **kwargs)
        incoming.put_nowait({"type": "websocket.disconnect", "code": kwargs["code"]})

    monkeypatch.setattr(websocket, "close", close)
    await asyncio.wait_for(endpoint(websocket, "room", username=None), 5)
    frames = [json.loads(item["text"]) for item in sent if item["type"] == "websocket.send"]
    assert [frame["type"] for frame in frames[:2]] == ["ready", "history"]
    if state == "archived":
        assert frames[2:] == [{"type": "room.archived", "room": snapshot.model_dump(mode="json")}]
        assert sent[-1]["code"] == 4409
    elif state == "missing":
        assert frames[-1] == {"type": "error", "code": "room_not_found", "message": "Room not found"}
        assert sent[-1]["code"] == 4404
    else:
        assert frames[-1]["code"] == "storage_unavailable"
        assert sent[-1]["code"] == 1012
    assert "private database" not in repr(sent)
    assert application.state.connections.active_connections == 0
    assert runtime.reservations == set()
    runtime.release_connection.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["history", "ready", "activation"])
async def test_handshake_does_not_drop_events_after_history_snapshot(socket_app, monkeypatch, phase):
    application, runtime = socket_app
    manager = application.state.connections
    store = application.state.store
    websocket, sent, incoming, endpoint = _socket(application)
    events = [
        {"type": "message.created", "message": {"id": "during-handshake", "content": "original"}},
        {"type": "message.updated", "message": {"id": "during-handshake", "content": "edited"}},
        {"type": "message.deleted", "message": {"id": "during-handshake", "content": ""}},
    ]

    async def broadcast():
        for event in events:
            await manager.broadcast("room", event)

    if phase == "history":
        original = store.list_messages

        async def history(*args, **kwargs):
            snapshot = await original(*args, **kwargs)
            await broadcast()
            return snapshot

        monkeypatch.setattr(store, "list_messages", history)
    elif phase == "ready":
        original_send = manager.send

        async def send(connection, event):
            if event["type"] == "ready":
                await broadcast()
            return await original_send(connection, event)

        monkeypatch.setattr(manager, "send", send)
    else:
        original_activate = manager.activate

        async def activate(connection):
            await broadcast()
            return await original_activate(connection)

        monkeypatch.setattr(manager, "activate", activate)

    incoming.put_nowait({"type": "websocket.disconnect", "code": 1000})
    await asyncio.wait_for(endpoint(websocket, "room", username=None), 5)
    frames = [json.loads(item["text"]) for item in sent if item["type"] == "websocket.send"]
    assert [event["type"] for event in frames[:2]] == ["ready", "history"]
    assert frames[1]["items"] == []
    assert frames[2:] == events
    assert manager.active_connections == 0
    if runtime is not None:
        assert runtime.reservations == set()
