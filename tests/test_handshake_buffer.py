# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Deterministic ordering, memory accounting and cancellation for activation."""

import asyncio
import json
from typing import cast

import pytest
from fastapi import WebSocket

from samsarix_chat_engine import ConnectionManager


class Socket:
    def __init__(self):
        self.sent = []
        self.closed = []
        self.block_send = False
        self.started = asyncio.Event()
        self.finish = asyncio.Event()

    async def send_json(self, event):
        if self.block_send:
            self.started.set()
            await self.finish.wait()
        self.sent.append(event)

    async def close(self, *, code, reason):
        self.closed.append((code, reason))


def socket(target):
    return cast(WebSocket, target)


def manager(**kwargs):
    return ConnectionManager(max_connections=3, max_per_room=3, send_timeout=0.3, **kwargs)


@pytest.mark.asyncio
async def test_pending_payload_is_a_snapshot_and_origin_exclusion_still_applies():
    connections = manager()
    target, excluded, unrelated = Socket(), Socket(), Socket()
    for peer, room, connection_id in ((target, "room", "a"), (excluded, "room", "b"), (unrelated, "other", "c")):
        await connections.register(socket(peer), room, "A", connection_id=connection_id, broadcast_ready=False)
    event = {"type": "message.created", "message": {"content": "before"}}
    await connections.broadcast("room", event, exclude_connection_id="b")
    event["message"]["content"] = "after"
    await connections.broadcast("room", {"type": "typing.stopped"}, exclude=socket(target))
    for peer in (target, excluded, unrelated):
        assert await connections.activate(socket(peer))
    assert target.sent == [{"type": "message.created", "message": {"content": "before"}}]
    assert excluded.sent == [{"type": "typing.stopped"}]
    assert unrelated.sent == []
    assert connections._pending_bytes == 0


@pytest.mark.asyncio
async def test_new_broadcasts_queue_while_activation_drains_and_cannot_overtake():
    connections = manager()
    target, peer = Socket(), Socket()
    await connections.register(socket(target), "room", "A", broadcast_ready=False)
    await connections.register(socket(peer), "room", "B")
    await connections.broadcast("room", {"type": "first"})
    target.block_send = True
    activation = asyncio.create_task(connections.activate(socket(target)))
    try:
        await asyncio.wait_for(target.started.wait(), 1)
        await asyncio.wait_for(connections.broadcast("room", {"type": "second"}), 0.1)
        assert peer.sent == [{"type": "first"}, {"type": "second"}]
        assert target.sent == []
        target.finish.set()
        assert await activation
        await connections.broadcast("room", {"type": "third"})
        assert target.sent == [{"type": "first"}, {"type": "second"}, {"type": "third"}]
        assert connections._pending_bytes == 0
    finally:
        target.finish.set()
        activation.cancel()
        await asyncio.gather(activation, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", ["events", "bytes", "global"])
async def test_overflow_closes_incomplete_socket_without_disrupting_active_peers(limit):
    event = {"type": "unicode", "text": "\U0001f680"}
    size = len(json.dumps(event, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
    options = {
        "events": {"max_pending_events": 1},
        "bytes": {"max_pending_bytes": size},
        "global": {"max_total_pending_bytes": size},
    }
    connections = manager(**options[limit])
    target, peer = Socket(), Socket()
    await connections.register(socket(target), "room", "A", broadcast_ready=False)
    await connections.register(socket(peer), "room", "B")
    await connections.broadcast("room", event)
    assert connections._pending_bytes == size
    await connections.broadcast("room", event)
    assert target.closed == [(1013, "History synchronization overflow")]
    assert target.sent == []
    assert peer.sent == [event, event]
    assert connections._pending_bytes == 0
    assert not await connections.activate(socket(target))
    assert not await connections.send(socket(target), {"type": "history"})
    assert connections.active_connections == 1


@pytest.mark.asyncio
async def test_global_budget_spans_rooms_and_recovers_after_detachment():
    event = {"type": "test"}
    size = len(json.dumps(event, separators=(",", ":")))
    connections = manager(max_total_pending_bytes=size)
    first, second, replacement = Socket(), Socket(), Socket()
    await connections.register(socket(first), "first", "A", broadcast_ready=False)
    await connections.register(socket(second), "second", "B", broadcast_ready=False)
    await connections.broadcast("first", event)
    await connections.broadcast("second", event)
    assert second.closed[0][0] == 1013
    assert connections._pending_bytes == size
    await connections.unregister(socket(first))
    assert connections._pending_bytes == 0
    await connections.register(socket(replacement), "second", "B", broadcast_ready=False)
    await connections.broadcast("second", event)
    assert await connections.activate(socket(replacement))
    assert replacement.sent == [event]
    assert connections._pending_bytes == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["unregister", "single", "room", "member", "all"])
async def test_every_detachment_discards_queued_events_and_budget(operation):
    connections = manager()
    target = Socket()
    await connections.register(socket(target), "room", "A", "subject", broadcast_ready=False)
    await connections.broadcast("room", {"type": "queued"})
    if operation == "unregister":
        await connections.unregister(socket(target))
    elif operation == "single":
        await connections.close(socket(target), code=1000, reason="closed")
    elif operation == "room":
        await connections.close_room("room", {"type": "room.archived"})
    elif operation == "member":
        await connections.close_member("room", "subject", {"type": "member.banned"})
    else:
        await connections.close_all()
    assert connections._pending_bytes == 0
    assert not await connections.activate(socket(target))
    assert {"type": "queued"} not in target.sent


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["timeout", "cancel", "error"])
async def test_failed_activation_closes_and_releases_all_pending_budget(failure):
    connections = manager()
    target = Socket()
    await connections.register(socket(target), "room", "A", broadcast_ready=False)
    await connections.broadcast("room", {"type": "first"})
    await connections.broadcast("room", {"type": "second"})
    if failure == "error":

        async def fail(event):
            raise RuntimeError("private error details")

        target.send_json = fail
        assert not await connections.activate(socket(target))
    else:
        target.block_send = True
        activation = asyncio.create_task(connections.activate(socket(target)))
        await asyncio.wait_for(target.started.wait(), 1)
        if failure == "cancel":
            activation.cancel()
            with pytest.raises(asyncio.CancelledError):
                await activation
        else:
            assert not await asyncio.wait_for(activation, 1)
    assert target.closed[0][0] == (1012 if failure == "cancel" else 1013)
    assert connections.active_connections == 0
    assert connections._pending_bytes == 0


@pytest.mark.asyncio
async def test_inflight_payload_remains_charged_after_detachment_until_send_finishes():
    event = {"type": "one"}
    size = len(json.dumps(event, separators=(",", ":")))
    connections = manager(max_total_pending_bytes=size)
    first, second = Socket(), Socket()
    await connections.register(socket(first), "room", "A", broadcast_ready=False)
    await connections.broadcast("room", event)
    first.block_send = True
    activation = asyncio.create_task(connections.activate(socket(first)))
    try:
        await asyncio.wait_for(first.started.wait(), 1)
        await connections.unregister(socket(first))
        assert connections._pending_bytes == size
        await connections.register(socket(second), "other", "B", broadcast_ready=False)
        await connections.broadcast("other", event)
        assert second.closed[0][0] == 1013
        first.finish.set()
        assert not await activation
        assert connections._pending_bytes == 0
    finally:
        first.finish.set()
        activation.cancel()
        await asyncio.gather(activation, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [object(), "\ud800"])
async def test_unserializable_pending_broadcast_fails_closed(value):
    connections = manager()
    target = Socket()
    await connections.register(socket(target), "room", "A", broadcast_ready=False)
    await connections.broadcast("room", {"type": "bad", "value": value})
    assert target.closed[0][0] == 1013
    assert connections._pending_bytes == 0


@pytest.mark.parametrize("name", ["max_pending_events", "max_pending_bytes", "max_total_pending_bytes"])
@pytest.mark.parametrize("value", [0, -1, True, 0.5])
def test_buffer_limits_reject_unbounded_or_ambiguous_values(name, value):
    with pytest.raises(ValueError, match="positive integers"):
        manager(**{name: value})


@pytest.mark.asyncio
async def test_continuous_arrivals_cannot_reset_the_whole_activation_deadline():
    connections = manager()
    target = Socket()
    await connections.register(socket(target), "room", "A", broadcast_ready=False)
    await connections.broadcast("room", {"type": "first"})

    async def continually_refill(event):
        target.sent.append(event)
        await connections.broadcast("room", {"type": "next"})
        await asyncio.sleep(0.01)

    target.send_json = continually_refill
    assert not await asyncio.wait_for(connections.activate(socket(target)), 2)
    assert target.sent
    assert target.closed[0][0] == 1013
    assert connections._pending_bytes == 0


@pytest.mark.asyncio
async def test_cancelled_overflow_producer_still_finishes_physical_close():
    connections = manager(max_pending_events=1)
    target = Socket()
    closing = asyncio.Event()
    finish_close = asyncio.Event()

    async def delayed_close(*, code, reason):
        closing.set()
        await finish_close.wait()
        target.closed.append((code, reason))

    target.close = delayed_close
    await connections.register(socket(target), "room", "A", broadcast_ready=False)
    await connections.broadcast("room", {"type": "first"})
    overflow = asyncio.create_task(connections.broadcast("room", {"type": "second"}))
    try:
        await asyncio.wait_for(closing.wait(), 1)
        overflow.cancel()
        await asyncio.sleep(0)
        assert not overflow.done()
        overflow.cancel()
        finish_close.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(overflow, 1)
        assert target.closed == [(1013, "History synchronization overflow")]
        assert connections._pending_bytes == 0
    finally:
        finish_close.set()
        overflow.cancel()
        await asyncio.gather(overflow, return_exceptions=True)
