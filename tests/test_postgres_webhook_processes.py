# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Real worker death, natural claim expiry, and signed webhook recovery."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import httpx2 as httpx
import pytest

psycopg = pytest.importorskip("psycopg")

from tests.test_postgres_processes import (  # noqa: E402
    _OPERATOR_KEY,
    _SIGNING_SECRET,
    LoggedServer,
    _member_socket,
    _member_token,
    _receive_type,
    _start_server,
    _stop_server,
    _unused_port,
    _wait_ready,
)

pytestmark = pytest.mark.postgres
_WEBHOOK_KEY = b"process-webhook-test-signing-key-32-bytes"
_OPERATOR = {"X-API-Key": _OPERATOR_KEY}


@dataclass
class Receipt:
    body: bytes
    headers: dict[str, str]
    reply: asyncio.Event = field(default_factory=asyncio.Event)
    disconnected: asyncio.Event = field(default_factory=asyncio.Event)
    responded: bool = False

    @property
    def delivery_id(self) -> str:
        return self.headers["webhook-id"]


class WebhookSink:
    """Loopback-only receiver with explicit per-attempt acknowledgement barriers."""

    def __init__(self) -> None:
        self.queue: asyncio.Queue[Receipt | Exception] = asyncio.Queue(maxsize=16)
        self.receipts: list[Receipt] = []
        self.tasks: set[asyncio.Task[None]] = set()
        self.errors: list[Exception] = []

    def accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.create_task(self.handle(reader, writer))
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        pending: list[asyncio.Task[Any]] = []
        try:
            raw_headers = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 5)
            lines = raw_headers.decode("ascii").split("\r\n")
            assert lines[0] == "POST /events HTTP/1.1"
            headers = {key.lower(): value.strip() for key, value in (line.split(":", 1) for line in lines[1:] if line)}
            length = int(headers["content-length"])
            assert 0 < length <= 65_536
            body = await asyncio.wait_for(reader.readexactly(length), 5)
            assert headers["content-type"] == "application/json"
            timestamp = headers["webhook-timestamp"]
            assert abs(time.time() - int(timestamp)) <= 300
            signed = headers["webhook-id"].encode() + b"." + timestamp.encode() + b"." + body
            expected = base64.b64encode(hmac.new(_WEBHOOK_KEY, signed, hashlib.sha256).digest()).decode()
            assert any(
                hmac.compare_digest(part.removeprefix("v1,"), expected)
                for part in headers["webhook-signature"].split()
                if part.startswith("v1,")
            )
            payload = json.loads(body)
            assert payload["id"] == headers["webhook-id"]
            assert payload["type"] == "message.created"
            assert len(self.receipts) < 16
            receipt = Receipt(body, headers)
            self.receipts.append(receipt)
            self.queue.put_nowait(receipt)
            eof = asyncio.create_task(reader.read(1))
            reply = asyncio.create_task(receipt.reply.wait())
            pending.extend((eof, reply))
            completed, _ = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            if eof in completed:
                assert await eof == b""
                receipt.disconnected.set()
            else:
                writer.write(b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")
                await writer.drain()
                receipt.responded = True
        except Exception as exc:
            self.errors.append(exc)
            if not self.queue.full():
                self.queue.put_nowait(exc)
        finally:
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            writer.close()
            with suppress(OSError):
                await writer.wait_closed()

    async def next(self, *, timeout: float = 10) -> Receipt:
        received = await asyncio.wait_for(self.queue.get(), timeout)
        assert isinstance(received, Receipt), f"receiver failed: {received!r}"
        return received


@asynccontextmanager
async def _sink() -> AsyncIterator[tuple[WebhookSink, str]]:
    sink = WebhookSink()
    server = await asyncio.start_server(sink.accept, "127.0.0.1", 0, limit=16_384)
    port = server.sockets[0].getsockname()[1]
    try:
        yield sink, f"http://127.0.0.1:{port}/events"
    finally:
        server.close()
        await server.wait_closed()
        tasks = tuple(sink.tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    assert not sink.errors


async def _claim(conninfo: str, delivery_id: str) -> tuple[Any, ...] | None:
    async with await psycopg.AsyncConnection.connect(conninfo, autocommit=True) as connection:
        cursor = await connection.execute(
            """
            SELECT lease_owner, lease_expires_at, clock_timestamp(), attempt_count, delivered_at, payload
            FROM public.samsarix_webhook_deliveries WHERE id = %s
            """,
            (delivery_id,),
        )
        return await cursor.fetchone()


async def _until_expired(conninfo: str, expires_at: datetime) -> None:
    # Read the real database clock. Never shorten or rewrite the production lease.
    async with await psycopg.AsyncConnection.connect(conninfo, autocommit=True) as connection:
        deadline = time.monotonic() + 75
        while True:
            cursor = await connection.execute("SELECT clock_timestamp() > %s", (expires_at,))
            row = await cursor.fetchone()
            if row[0]:
                return
            assert time.monotonic() < deadline, "database lease did not expire within bounded wait"
            await asyncio.sleep(0.2)


async def _delivered(client: httpx.AsyncClient, base: str, delivery_id: str) -> dict[str, Any]:
    deadline = time.monotonic() + 10
    while True:
        response = await client.get(
            f"{base}/v1/admin/webhook-deliveries", headers=_OPERATOR, params={"status": "delivered"}
        )
        assert response.status_code == 200
        for item in response.json()["items"]:
            assert "payload" not in item
            if item["id"] == delivery_id:
                assert item["delivered_at"] is not None and item["last_status_code"] == 204
                return item
        assert time.monotonic() < deadline, "delivery acknowledgement did not commit"
        await asyncio.sleep(0.05)


@pytest.mark.timeout(150)
@pytest.mark.parametrize("delete_before_reclaim", [False, True], ids=["recover", "deleted-payload"])
async def test_killed_webhook_worker_natural_lease_recovery(
    clean_postgres_database: str, delete_before_reclaim: bool
) -> None:
    # The production outbox lease is 60 seconds. Allow that real expiry plus
    # bounded process startup/teardown, rather than weakening the clock contract.
    processes: list[LoggedServer] = []
    member = {"Authorization": f"Bearer {_member_token('Alice', 'recovery')}"}
    async with _sink() as (sink, destination), httpx.AsyncClient(timeout=5, trust_env=False) as client:
        settings = {
            "POSTGRES_LEASE_SECONDS": "30",
            "WEBHOOK_URL": destination,
            "WEBHOOK_SIGNING_SECRET": "whsec_" + base64.b64encode(_WEBHOOK_KEY).decode(),
            "WEBHOOK_EVENTS": "message.created",
            "WEBHOOK_TIMEOUT": "30",
        }
        try:
            first_port = _unused_port()
            first_base = f"http://127.0.0.1:{first_port}"
            first = _start_server(
                clean_postgres_database,
                "webhook-first",
                first_port,
                signing_secret=_SIGNING_SECRET,
                settings=settings,
            )
            processes.append(first)
            await _wait_ready(client, first_base, first)
            room = await client.post(
                f"{first_base}/v1/rooms", headers=_OPERATOR, json={"id": "recovery", "name": "Recovery"}
            )
            assert room.status_code == 201
            saved = await client.post(
                f"{first_base}/v1/rooms/recovery/messages",
                headers=member,
                json={"content": "customer request before worker crash"},
            )
            assert saved.status_code == 201
            original = await sink.next()
            assert json.loads(original.body)["data"]["message"] == saved.json()
            initial_claim = await _claim(clean_postgres_database, original.delivery_id)
            assert initial_claim is not None
            first_owner, expires_at, database_now, attempts, delivered_at, payload = initial_claim
            assert first_owner and expires_at > database_now and attempts == 0 and delivered_at is None
            assert bytes(payload) == original.body

            # Start the survivor after the first process owns an in-flight request.
            # Both workers use exactly the same destination, event and signing settings.
            second_port = _unused_port()
            second_base = f"http://127.0.0.1:{second_port}"
            second = _start_server(
                clean_postgres_database,
                "webhook-second",
                second_port,
                signing_secret=_SIGNING_SECRET,
                settings=settings,
            )
            processes.append(second)
            assert first.pid != second.pid
            await _wait_ready(client, second_base, second)
            async with _member_socket(second_port, "Alice", "recovery") as (socket, history):
                assert history["items"] == [saved.json()]
                await socket.send(json.dumps({"type": "ping"}))
                await _receive_type(socket, "pong")
                control = await client.post(
                    f"{second_base}/v1/rooms/recovery/messages",
                    headers=member,
                    json={"content": "survivor can claim other work"},
                )
                assert control.status_code == 201
                live = await _receive_type(socket, "message.created")
                assert live["message"] == control.json()
                control_receipt = await sink.next()
                assert control_receipt.delivery_id != original.delivery_id
                assert json.loads(control_receipt.body)["data"]["message"] == control.json()
                control_claim = await _claim(clean_postgres_database, control_receipt.delivery_id)
                assert control_claim is not None and control_claim[0] and control_claim[0] != first_owner
                assert control_claim[1] > control_claim[2] and control_claim[3] == 0 and control_claim[4] is None
                second_owner = control_claim[0]
                still_owned = await _claim(clean_postgres_database, original.delivery_id)
                assert still_owned is not None and still_owned[0] == first_owner
                assert still_owned[1] == expires_at and still_owned[1] > still_owned[2]
                assert not original.responded and not original.disconnected.is_set()
                assert sink.queue.empty()
                # Kill only the Popen child created above. Do not gracefully release its claim.
                assert first.poll() is None
                first.kill()
                await asyncio.to_thread(first.wait, 5)
                await asyncio.wait_for(original.disconnected.wait(), 5)
                assert first.poll() is not None and not original.responded
                stranded = await _claim(clean_postgres_database, original.delivery_id)
                assert stranded is not None and stranded[0] == first_owner
                assert stranded[1] == expires_at and stranded[1] > stranded[2]
                control_receipt.reply.set()
                assert (await _delivered(client, second_base, control_receipt.delivery_id))["attempt_count"] == 1

                if delete_before_reclaim:
                    deleted = await client.delete(
                        f"{second_base}/v1/rooms/recovery/messages/{saved.json()['id']}", headers=member
                    )
                    assert deleted.status_code == 204
                    assert (await _receive_type(socket, "message.deleted"))["message"]["content"] == ""
                    assert await _claim(clean_postgres_database, original.delivery_id) is None
                    denied = await client.post(
                        f"{second_base}/v1/admin/webhook-deliveries/{original.delivery_id}/retry", headers=_OPERATOR
                    )
                    assert denied.status_code == 404
                    assert denied.json()["error"]["code"] == "webhook_delivery_not_found"
                    await _until_expired(clean_postgres_database, expires_at)
                else:
                    recovered = await sink.next(timeout=75)
                    assert recovered.delivery_id == original.delivery_id
                    assert recovered.body == original.body
                    assert int(recovered.headers["webhook-timestamp"]) > int(original.headers["webhook-timestamp"])
                    renewed = await _claim(clean_postgres_database, recovered.delivery_id)
                    assert renewed is not None and renewed[0] == second_owner
                    assert renewed[2] >= expires_at and renewed[1] > renewed[2]
                    assert renewed[3] == 0 and renewed[4] is None
                    recovered.reply.set()
                    recorded = await _delivered(client, second_base, recovered.delivery_id)
                    # A killed, unrecorded HTTP attempt is not in the persisted counter.
                    assert recorded["attempt_count"] == 1

                # A later acknowledged event proves the surviving dispatcher made
                # progress after natural expiry, not merely that no callback arrived.
                marker = await client.post(
                    f"{second_base}/v1/rooms/recovery/messages",
                    headers=member,
                    json={"content": "after natural lease expiry"},
                )
                assert marker.status_code == 201
                assert (await _receive_type(socket, "message.created"))["message"] == marker.json()
                marker_receipt = await sink.next()
                assert json.loads(marker_receipt.body)["data"]["message"] == marker.json()
                marker_receipt.reply.set()
                assert (await _delivered(client, second_base, marker_receipt.delivery_id))["attempt_count"] == 1
                assert second.poll() is None
                assert [receipt.delivery_id for receipt in sink.receipts].count(original.delivery_id) == (
                    1 if delete_before_reclaim else 2
                )
                assert sink.queue.empty() and not sink.errors
                denied_metadata = await client.get(f"{second_base}/v1/admin/webhook-deliveries", headers=member)
                assert denied_metadata.status_code == 403
        finally:
            for process in reversed(processes):
                await asyncio.to_thread(_stop_server, process)
