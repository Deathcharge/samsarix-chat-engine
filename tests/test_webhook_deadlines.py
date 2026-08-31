# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Total webhook deadlines, physical cleanup, and bounded outstanding work."""

from __future__ import annotations

import asyncio
import os
import socket
import ssl
import sys
import threading
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx2 as httpx
import pytest

from samsarix_chat_engine.models import RoomCreate
from samsarix_chat_engine.webhooks import WebhookAttemptResult, WebhookDispatcher
from tests.test_webhooks import API_KEY, SECRET_BYTES, _store


async def _event(event: threading.Event) -> None:
    for _ in range(200):
        if event.is_set():
            return
        await asyncio.sleep(0.01)
    pytest.fail("transport barrier was not reached")


async def _completed(task: asyncio.Task[Any], timeout: float = 0.8) -> Any:
    # wait_for cancels the task at its own timeout; a cancellation-suppressing
    # implementation could then return and incorrectly make the test pass.
    done, _ = await asyncio.wait({task}, timeout=timeout)
    assert task in done, "attempt did not finish without test-driven cancellation"
    return await task


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
@pytest.mark.parametrize("finish", ["deadline", "cancel", "stop"])
async def test_slow_headers_hit_total_deadline_and_close_peer(tmp_path: Path, finish: str) -> None:
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
        async with _dispatcher(
            tmp_path, url=f"http://127.0.0.1:{port}/events", timeout=0.15 if finish == "deadline" else 10
        ) as (dispatcher, store):
            task = asyncio.create_task(dispatcher.process_due_once())
            try:
                await asyncio.wait_for(arrived.wait(), 2)
                if finish == "cancel":
                    task.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await _completed(task)
                elif finish == "stop":
                    dispatcher.stop()
                    assert not await _completed(task)
                else:
                    assert await _completed(task)
                await asyncio.wait_for(closed.wait(), 1)
                pending, _ = await store.list_webhook_deliveries(status="pending")
                attempted = [item for item in pending if item.attempt_count]
                if finish == "deadline":
                    assert len(attempted) == 1 and attempted[0].last_error == "timeout"
                    assert attempted[0].delivered_at is None
                else:
                    assert attempted == []
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
@pytest.mark.parametrize("finish,record_delay", [("deadline", 0), ("deadline", 1), ("cancel", 0), ("stop", 0)])
async def test_blocked_dns_is_bounded_and_cannot_send_late(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, finish: str, record_delay: float
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
        original_record = store.record_webhook_attempt

        async def record(*args: Any, **kwargs: Any) -> None:
            # Storage is outside the network-attempt budget. Deliberately make
            # it slower than the old 0.8-second whole-iteration assertion.
            await asyncio.sleep(record_delay)
            await original_record(*args, **kwargs)

        monkeypatch.setattr(store, "record_webhook_attempt", record)
        task = asyncio.create_task(dispatcher.run() if finish == "stop" else dispatcher.process_due_once())
        try:
            await _event(entered)
            if finish == "cancel":
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await _completed(task)
            elif finish == "stop":
                dispatcher.stop()
                await _completed(task)
            else:
                assert await _completed(task)
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
            assert await _completed(task)
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


@pytest.mark.timeout(20)
async def test_real_uvicorn_process_exits_with_a_stuck_resolver(tmp_path: Path) -> None:
    from tests.process_helpers import LoggedServer, _stop_server, _unused_port, _wait_ready

    port = _unused_port()
    base = f"http://127.0.0.1:{port}"
    environment = {
        key: value for key, value in os.environ.items() if not key.startswith(("SAMSARIX_CHAT_", "HELIX_CHAT_"))
    }
    process = LoggedServer(
        [
            sys.executable,
            "-m",
            "tests.webhook_shutdown_server",
            "--port",
            str(port),
            "--database",
            str(tmp_path / "shutdown.db"),
        ],
        env=environment,
    )
    try:
        async with httpx.AsyncClient(timeout=2, trust_env=False) as client:
            await _wait_ready(client, base, process)
            headers = {"X-API-Key": API_KEY}
            assert (
                await client.post(f"{base}/v1/rooms", headers=headers, json={"id": "room", "name": "Room"})
            ).status_code == 201
            assert (
                await client.post(
                    f"{base}/v1/rooms/room/messages",
                    headers=headers,
                    json={"sender": "operator", "content": "blocked DNS"},
                )
            ).status_code == 201
            for _ in range(200):
                if "resolver-entered" in process.output_tail():
                    break
                await asyncio.sleep(0.01)
            else:
                pytest.fail(f"resolver did not start: {process.output_tail()}")
            assert (await client.post(f"{base}/_test/stop")).status_code == 200
            for _ in range(300):
                if process.poll() is not None:
                    break
                await asyncio.sleep(0.01)
            assert process.poll() == 0, f"process failed to exit without test kill: {process.output_tail()}"
            await asyncio.to_thread(process.reader.join, 1)
            assert "server-exited" in process.output_tail()
    finally:
        if process.poll() is None:
            process.kill()
        await asyncio.to_thread(_stop_server, process)


def _tls_contexts(tmp_path: Path, *, wrong_name: bool, trusted: bool) -> tuple[ssl.SSLContext, ssl.SSLContext]:
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    key = rsa.generate_private_key(public_exponent=65_537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "webhook test")])
    now = datetime.now(timezone.utc)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(
            x509.SubjectAlternativeName([x509.DNSName("wrong.invalid" if wrong_name else "localhost")]), critical=False
        )
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(x509.KeyUsage(True, False, True, False, False, True, True, None, None), critical=True)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), critical=False)
        .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(key.public_key()), critical=False)
        .sign(key, hashes.SHA256())
    )
    pem = certificate.public_bytes(serialization.Encoding.PEM)
    cert_path, key_path = tmp_path / "tls-cert.pem", tmp_path / "tls-key.pem"
    cert_path.write_bytes(pem)
    key_path.write_bytes(
        key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    )
    server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_context.load_cert_chain(cert_path, key_path)
    client_context = ssl.create_default_context()
    if trusted:
        client_context.load_verify_locations(cadata=pem.decode())
    return server_context, client_context


@pytest.mark.timeout(15)
@pytest.mark.parametrize(
    "mode", ["valid", "wrong-name", "untrusted", "slow-headers", "stalled-handshake", "cancel-headers", "stop-headers"]
)
async def test_real_tls_preserves_verification_pinning_and_deadline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    server_context, client_context = _tls_contexts(
        tmp_path, wrong_name=mode == "wrong-name", trusted=mode != "untrusted"
    )
    sni = []
    server_context.set_servername_callback(lambda connection, hostname, context: sni.append(hostname))
    arrived = asyncio.Event()
    closed = asyncio.Event()
    requests = []
    tasks: set[asyncio.Task[None]] = set()
    writers = []

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writers.append(writer)
        try:
            if mode == "stalled-handshake":
                assert await reader.read(4096)
                arrived.set()
                await reader.read()
                closed.set()
                return
            headers = await reader.readuntil(b"\r\n\r\n")
            requests.append(headers)
            length = next(
                int(line.split(b":", 1)[1])
                for line in headers.split(b"\r\n")
                if line.lower().startswith(b"content-length:")
            )
            body = await reader.readexactly(length)
            assert b'"content":"first"' in body
            arrived.set()
            writer.write(b"HTTP/1.1 204 No Content\r\n")
            await writer.drain()
            if mode.endswith("headers"):
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
            writer.write(b"\r\n")
            await writer.drain()
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

    server = await asyncio.start_server(
        accept, "127.0.0.1", 0, ssl=None if mode == "stalled-handshake" else server_context
    )
    port = server.sockets[0].getsockname()[1]
    resolved = []
    real_resolve = socket.getaddrinfo

    def resolve(host: str, port: int, **kwargs: Any) -> Any:
        if host == "localhost":
            resolved.append(host)
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", port))]
        return real_resolve(host, port, **kwargs)

    monkeypatch.setattr("samsarix_chat_engine.webhooks.socket.getaddrinfo", resolve)
    monkeypatch.setattr("samsarix_chat_engine.webhook_transport.ssl.create_default_context", lambda: client_context)
    timeout = 0.3 if mode in {"slow-headers", "stalled-handshake"} else 2
    try:
        async with _dispatcher(tmp_path, url=f"https://localhost:{port}/events", timeout=timeout) as (
            dispatcher,
            store,
        ):
            task = asyncio.create_task(dispatcher.process_due_once())
            if mode in {"cancel-headers", "stop-headers"}:
                await asyncio.wait_for(arrived.wait(), 2)
                if mode == "cancel-headers":
                    task.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await _completed(task)
                else:
                    dispatcher.stop()
                    assert not await _completed(task)
            else:
                assert await _completed(task, timeout=3)
            assert resolved == ["localhost"]
            if mode == "valid":
                delivered, _ = await store.list_webhook_deliveries(status="delivered")
                assert len(delivered) == 1 and delivered[0].last_status_code == 204
                assert sni == ["localhost"] and f"Host: localhost:{port}".encode() in requests[0]
            else:
                pending, _ = await store.list_webhook_deliveries(status="pending")
                attempted = [item for item in pending if item.attempt_count]
                if mode in {"cancel-headers", "stop-headers"}:
                    assert attempted == []
                    await asyncio.wait_for(closed.wait(), 1)
                    return
                assert len(attempted) == 1
                assert attempted[0].last_error == (
                    "timeout" if mode in {"slow-headers", "stalled-handshake"} else "tls_error"
                )
                if mode in {"slow-headers", "stalled-handshake"}:
                    assert arrived.is_set()
                    await asyncio.wait_for(closed.wait(), 1)
                else:
                    assert not arrived.is_set() and requests == []
    finally:
        server.close()
        await server.wait_closed()
        for writer in writers:
            writer.close()
        remaining = tuple(tasks)
        for task in remaining:
            task.cancel()
        await asyncio.gather(*remaining, return_exceptions=True)
