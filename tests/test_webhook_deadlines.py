# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Total webhook deadlines, physical cleanup, and bounded outstanding work."""

from __future__ import annotations

import asyncio
import socket
import threading
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from samsarix_chat_engine.models import RoomCreate
from samsarix_chat_engine.webhooks import WebhookAttemptResult, WebhookDispatcher
from tests.test_webhooks import SECRET_BYTES, _store


async def _event(event: threading.Event) -> None:
    for _ in range(200):
        if event.is_set():
            return
        await asyncio.sleep(0.01)
    pytest.fail("transport barrier was not reached")


@asynccontextmanager
async def _dispatcher(
    tmp_path: Path, *, url: str = "https://hooks.example.com/events", timeout: float = 0.15
) -> AsyncIterator[Any]:
    store = _store(tmp_path / "deadlines.db")
    await store.initialize()
    await store.create_room(RoomCreate(id="room", name="Room"))
    for content in ("first", "second"):
        await store.create_message(
            room_id="room", sender="Alice", content=content, client_message_id=None, allow_frozen=False
        )
    dispatcher = WebhookDispatcher(
        store,
        url=url,
        secrets=(SECRET_BYTES,),
        timeout=timeout,
        max_attempts=3,
        allow_private_targets=False,
        poll_interval=0.02,
    )
    try:
        yield dispatcher, store
    finally:
        dispatcher.stop()
        await store.close()


@pytest.mark.timeout(10)
async def test_slow_headers_hit_total_deadline_and_close_peer(tmp_path: Path) -> None:
    arrived = asyncio.Event()
    closed = asyncio.Event()
    tasks: set[asyncio.Task[None]] = set()
    writers: list[asyncio.StreamWriter] = []

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writers.append(writer)
        try:
            headers = await reader.readuntil(b"\r\n\r\n")
            length = next(
                int(line.split(b":", 1)[1])
                for line in headers.split(b"\r\n")
                if line.lower().startswith(b"content-length:")
            )
            await reader.readexactly(length)
            arrived.set()
            writer.write(b"HTTP/1.1 204 No Content\r\n")
            await writer.drain()
            while True:
                try:
                    data = await asyncio.wait_for(reader.read(1), 0.03)
                except asyncio.TimeoutError:
                    writer.write(b"X-Slow: still-here\r\n")
                    await writer.drain()
                    continue
                assert data == b""
                closed.set()
                return
        except (ConnectionError, asyncio.IncompleteReadError):
            closed.set()
        finally:
            writer.close()
            with suppress(OSError):
                await writer.wait_closed()

    def accept(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.create_task(handle(reader, writer))
        tasks.add(task)
        task.add_done_callback(tasks.discard)

    server = await asyncio.start_server(accept, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        async with _dispatcher(tmp_path, url=f"http://127.0.0.1:{port}/events") as (dispatcher, store):
            task = asyncio.create_task(dispatcher.process_due_once())
            try:
                await asyncio.wait_for(arrived.wait(), 2)
                assert await asyncio.wait_for(task, 0.8)
                await asyncio.wait_for(closed.wait(), 1)
                pending, _ = await store.list_webhook_deliveries(status="pending")
                attempted = [item for item in pending if item.attempt_count]
                assert len(attempted) == 1 and attempted[0].last_error == "timeout"
                assert attempted[0].delivered_at is None
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
    finally:
        server.close()
        await server.wait_closed()
        for writer in writers:
            writer.close()
        remaining = tuple(tasks)
        for task in remaining:
            task.cancel()
        await asyncio.gather(*remaining, return_exceptions=True)


@pytest.mark.timeout(10)
@pytest.mark.parametrize("finish", ["deadline", "cancel", "stop"])
async def test_blocked_dns_is_bounded_and_cannot_send_late(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, finish: str
) -> None:
    entered = threading.Event()
    release = threading.Event()
    exited = threading.Event()
    calls = []
    connections = []
    real_resolve = socket.getaddrinfo

    def resolve(host: str, port: int, **kwargs: Any) -> Any:
        if host != "hooks.example.com":
            return real_resolve(host, port, **kwargs)
        calls.append(host)
        entered.set()
        try:
            assert release.wait(5), "test failed to release its resolver"
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", port))]
        finally:
            exited.set()

    def connect(*args: Any, **kwargs: Any) -> Any:
        connections.append(args)
        raise AssertionError("expired/cancelled DNS result must never create a connection")

    monkeypatch.setattr("samsarix_chat_engine.webhooks.socket.getaddrinfo", resolve)
    monkeypatch.setattr("samsarix_chat_engine.webhooks._PinnedHTTPSConnection", connect)
    async with _dispatcher(tmp_path, timeout=0.15 if finish == "deadline" else 10) as (dispatcher, store):
        task = asyncio.create_task(dispatcher.run() if finish == "stop" else dispatcher.process_due_once())
        try:
            await _event(entered)
            if finish == "cancel":
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await asyncio.wait_for(task, 0.8)
            elif finish == "stop":
                dispatcher.stop()
                await asyncio.wait_for(task, 0.8)
            else:
                assert await asyncio.wait_for(task, 0.8)
            # Do not take more claims or start more DNS work while the old native
            # resolver still owns the sole transport slot.
            for _ in range(5):
                assert not await dispatcher.process_due_once()
            assert calls == ["hooks.example.com"]
            pending, _ = await store.list_webhook_deliveries(status="pending")
            assert sorted(item.attempt_count for item in pending) == ([0, 1] if finish == "deadline" else [0, 0])
            release.set()
            await _event(exited)
            await asyncio.sleep(0.05)
            assert connections == []
        finally:
            release.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await _event(exited)


@pytest.mark.timeout(10)
async def test_late_success_cannot_overwrite_timeout_or_spawn_more_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    entered = threading.Event()
    release = threading.Event()
    returned = threading.Event()
    calls = []

    def send(**kwargs: Any) -> WebhookAttemptResult:
        calls.append(kwargs["delivery"].delivery.id)
        entered.set()
        assert release.wait(5)
        returned.set()
        return WebhookAttemptResult(status_code=204, error=None)

    monkeypatch.setattr("samsarix_chat_engine.webhooks._send_request", send)
    async with _dispatcher(tmp_path) as (dispatcher, store):
        task = asyncio.create_task(dispatcher.process_due_once())
        try:
            await _event(entered)
            assert await asyncio.wait_for(task, 0.8)
            assert not await dispatcher.process_due_once()
            assert len(calls) == 1
            release.set()
            await _event(returned)
            for _ in range(100):
                if await dispatcher.process_due_once():
                    break
                await asyncio.sleep(0.01)
            else:
                pytest.fail("transport capacity did not recover")
            pending, _ = await store.list_webhook_deliveries(status="pending")
            delivered, _ = await store.list_webhook_deliveries(status="delivered")
            assert [item.id for item in pending] == [calls[0]]
            assert pending[0].last_error == "timeout" and pending[0].attempt_count == 1
            assert [item.id for item in delivered] == [calls[1]]
            assert delivered[0].attempt_count == 1 and calls[0] != calls[1]
        finally:
            release.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await _event(returned)


async def test_stopped_dispatcher_does_not_claim_more_rows(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    async with _dispatcher(tmp_path) as (dispatcher, store):

        async def forbidden(now: datetime) -> None:
            raise AssertionError("stopped dispatcher claimed a row")

        monkeypatch.setattr(store, "next_webhook_delivery", forbidden)
        dispatcher.stop()
        assert not await dispatcher.process_due_once(now=datetime.now(timezone.utc))
