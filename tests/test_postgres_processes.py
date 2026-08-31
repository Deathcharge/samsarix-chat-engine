# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Real-process PostgreSQL failover and reconnect acceptance tests."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from typing import Any
from urllib.parse import parse_qs, urlsplit

import httpx2 as httpx
import pytest

from tests.process_helpers import LoggedServer, _stop_server, _unused_port, _wait_ready

pytest.importorskip("psycopg")
websockets = pytest.importorskip("websockets.asyncio.client")

from samsarix_chat_engine import AccessTokenService  # noqa: E402

pytestmark = pytest.mark.postgres

_OPERATOR_KEY = "postgres-process-operator-key-1234"
_SIGNING_SECRET = "postgres-process-test-signing-secret-at-least-32-bytes"


def _start_server(
    conninfo: str,
    instance_id: str,
    port: int,
    *,
    pool_timeout: float = 10,
    operation_timeout: float = 10,
    signing_secret: str | None = None,
    settings: dict[str, str] | None = None,
) -> LoggedServer:
    environment = {
        key: value for key, value in os.environ.items() if not key.startswith(("SAMSARIX_CHAT_", "HELIX_CHAT_"))
    }
    environment.update(
        {
            "SAMSARIX_CHAT_STORAGE": "postgres",
            "SAMSARIX_CHAT_POSTGRES_URL": conninfo,
            "SAMSARIX_CHAT_POSTGRES_INSTANCE_ID": instance_id,
            "SAMSARIX_CHAT_POSTGRES_MAX_POOL_SIZE": "4",
            "SAMSARIX_CHAT_POSTGRES_POOL_TIMEOUT": str(pool_timeout),
            "SAMSARIX_CHAT_POSTGRES_OPERATION_TIMEOUT": str(operation_timeout),
            "SAMSARIX_CHAT_POSTGRES_LEASE_SECONDS": "3",
            "SAMSARIX_CHAT_POSTGRES_RELAY_POLL": "0.05",
            "SAMSARIX_CHAT_POSTGRES_MAINTENANCE_INTERVAL": "0.1",
            "SAMSARIX_CHAT_API_KEY": _OPERATOR_KEY,
            "SAMSARIX_CHAT_MAX_CONNECTIONS": "8",
            "SAMSARIX_CHAT_MAX_CONNECTIONS_PER_ROOM": "8",
        }
    )
    if signing_secret is not None:
        environment["SAMSARIX_CHAT_TOKEN_SIGNING_SECRET"] = signing_secret
    for key, value in (settings or {}).items():
        environment[f"SAMSARIX_CHAT_{key}"] = value
    return LoggedServer(
        [
            sys.executable,
            "-m",
            "samsarix_chat_engine",
            "serve",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--log-level",
            "warning",
        ],
        env=environment,
    )


class DatabaseProxy:
    """Test-only TCP cut for one replica, never a database administration action."""

    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self.enabled = True
        self.forwarding = asyncio.Event()
        self.forwarding.set()
        self.writers: set[asyncio.StreamWriter] = set()
        self.tasks: set[asyncio.Task[None]] = set()

    def accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.create_task(self.forward(reader, writer))
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    def cut(self) -> None:
        self.enabled = False
        for writer in tuple(self.writers):
            writer.transport.abort()

    async def forward(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writers = [writer]
        copies: list[asyncio.Task[None]] = []
        self.writers.add(writer)
        try:
            if not self.enabled:
                return
            upstream_reader, upstream_writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port), timeout=2
            )
            writers.append(upstream_writer)
            self.writers.add(upstream_writer)
            if not self.enabled:
                return

            async def copy(source: asyncio.StreamReader, destination: asyncio.StreamWriter) -> None:
                while chunk := await source.read(65_536):
                    await self.forwarding.wait()
                    destination.write(chunk)
                    await destination.drain()

            copies = [
                asyncio.create_task(copy(reader, upstream_writer)),
                asyncio.create_task(copy(upstream_reader, writer)),
            ]
            await asyncio.wait(copies, return_when=asyncio.FIRST_COMPLETED)
        except (OSError, asyncio.TimeoutError):
            pass
        finally:
            for task in copies:
                task.cancel()
            await asyncio.gather(*copies, return_exceptions=True)
            for stream in writers:
                self.writers.discard(stream)
                stream.close()
                with suppress(OSError, asyncio.TimeoutError):
                    await asyncio.wait_for(stream.wait_closed(), timeout=2)


@asynccontextmanager
async def _database_proxy(conninfo: str) -> AsyncIterator[tuple[DatabaseProxy, str]]:
    parsed = urlsplit(conninfo)
    if (
        parsed.scheme not in {"postgres", "postgresql"}
        or parsed.hostname not in {"localhost", "127.0.0.1", "::1"}
        or {"host", "hostaddr", "port"}.intersection(parse_qs(parsed.query))
    ):
        pytest.skip("network interruption test requires a single loopback PostgreSQL URL without host overrides")
    proxy = DatabaseProxy(parsed.hostname, parsed.port or 5432)
    server = await asyncio.start_server(proxy.accept, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    userinfo = parsed.netloc.rpartition("@")[0]
    netloc = f"{userinfo}@127.0.0.1:{port}" if userinfo else f"127.0.0.1:{port}"
    try:
        yield proxy, parsed._replace(netloc=netloc).geturl()
    finally:
        server.close()
        await server.wait_closed()
        proxy.cut()
        tasks = tuple(proxy.tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def _process_output(process: LoggedServer) -> str:
    return process.output_tail()


async def _receive_type(websocket: Any, event_type: str, *, username: str | None = None) -> dict[str, Any]:
    deadline = time.monotonic() + 12
    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        event = json.loads(await asyncio.wait_for(websocket.recv(), timeout=remaining))
        if event.get("type") == event_type and (username is None or event.get("username") == username):
            return event
    pytest.fail(f"did not receive {event_type}")


@pytest.mark.asyncio
async def test_two_uvicorn_processes_reap_crashed_leases_and_restart(
    clean_postgres_database: str,
) -> None:
    first_port = _unused_port()
    second_port = _unused_port()
    first_url = f"http://127.0.0.1:{first_port}"
    second_url = f"http://127.0.0.1:{second_port}"
    processes: list[LoggedServer] = []
    first = _start_server(clean_postgres_database, "process-first", first_port)
    second = _start_server(clean_postgres_database, "process-second", second_port)
    processes.extend((first, second))
    headers = {"X-API-Key": _OPERATOR_KEY}

    try:
        async with httpx.AsyncClient(headers=headers, timeout=2.0, trust_env=False) as client:
            await asyncio.gather(
                _wait_ready(client, first_url, first),
                _wait_ready(client, second_url, second),
            )
            created = await client.post(
                f"{first_url}/v1/rooms",
                json={"id": "process-room", "name": "Process room"},
            )
            assert created.status_code == 201
            assert (await client.get(f"{second_url}/v1/rooms/process-room")).status_code == 200

            websocket_headers = {"X-API-Key": _OPERATOR_KEY}
            async with websockets.connect(
                f"ws://127.0.0.1:{first_port}/v1/rooms/process-room/ws?username=Alice",
                additional_headers=websocket_headers,
                open_timeout=5,
                close_timeout=1,
            ) as alice:
                assert json.loads(await asyncio.wait_for(alice.recv(), 5))["type"] == "ready"
                assert json.loads(await asyncio.wait_for(alice.recv(), 5))["type"] == "history"
                # HTTP upgrade (and even receiving history) can precede activation.
                # This test checks crash/reap, not best-effort presence during startup.
                await alice.send(json.dumps({"type": "ping"}))
                await _receive_type(alice, "pong")
                async with websockets.connect(
                    f"ws://127.0.0.1:{second_port}/v1/rooms/process-room/ws?username=Bob",
                    additional_headers=websocket_headers,
                    open_timeout=5,
                    close_timeout=1,
                ) as bob:
                    assert json.loads(await asyncio.wait_for(bob.recv(), 5))["type"] == "ready"
                    assert json.loads(await asyncio.wait_for(bob.recv(), 5))["type"] == "history"
                    await _receive_type(alice, "presence.joined", username="Bob")

                    await bob.send(json.dumps({"type": "message", "content": "survives process loss"}))
                    bob_message = await _receive_type(bob, "message.created")
                    alice_message = await _receive_type(alice, "message.created")
                    assert alice_message["message"]["id"] == bob_message["message"]["id"]

                    first.kill()
                    first.wait(timeout=5)
                    left = await _receive_type(bob, "presence.left", username="Alice")
                    assert left["active_connections"] == 1
                    assert (await client.get(f"{second_url}/v1/stats")).json() == {"active_connections": 1}

            restarted = _start_server(clean_postgres_database, "process-first", first_port)
            processes.append(restarted)
            await _wait_ready(client, first_url, restarted)
            history = await client.get(f"{first_url}/v1/rooms/process-room/messages")
            assert history.status_code == 200
            assert [item["content"] for item in history.json()["items"]] == ["survives process loss"]
    finally:
        for process in reversed(processes):
            _stop_server(process)


@pytest.mark.asyncio
async def test_duplicate_live_instance_id_fails_startup_without_disrupting_owner(
    clean_postgres_database: str,
) -> None:
    """A manifest identity collision must fail closed and permit a later clean replacement."""

    first_port = _unused_port()
    duplicate_port = _unused_port()
    first_url = f"http://127.0.0.1:{first_port}"
    processes: list[LoggedServer] = []
    first = _start_server(clean_postgres_database, "stable-replica-0", first_port)
    processes.append(first)
    headers = {"X-API-Key": _OPERATOR_KEY}

    try:
        async with httpx.AsyncClient(headers=headers, timeout=3, trust_env=False) as client:
            await _wait_ready(client, first_url, first)
            duplicate = _start_server(clean_postgres_database, "stable-replica-0", duplicate_port)
            processes.append(duplicate)
            await asyncio.wait_for(asyncio.to_thread(duplicate.wait), timeout=15)
            duplicate.reader.join(timeout=2)

            output = _process_output(duplicate)
            assert duplicate.returncode not in {None, 0}
            assert "instance ID is already active" in output
            assert _OPERATOR_KEY not in output
            password = urlsplit(clean_postgres_database).password
            assert password is None or password not in output

            assert (await client.get(f"{first_url}/readyz")).status_code == 200
            created = await client.post(
                f"{first_url}/v1/rooms",
                json={"id": "identity-room", "name": "Stable identity"},
            )
            assert created.status_code == 201

            await asyncio.to_thread(_stop_server, first)
            replacement = _start_server(clean_postgres_database, "stable-replica-0", first_port)
            processes.append(replacement)
            await _wait_ready(client, first_url, replacement)
            recovered = await client.get(f"{first_url}/v1/rooms/identity-room")
            assert recovered.status_code == 200
            assert recovered.json()["name"] == "Stable identity"
    finally:
        for process in reversed(processes):
            _stop_server(process)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["reset", "stall"])
async def test_database_network_cut_fences_clients_and_recovers_without_process_restart(
    clean_postgres_database: str,
    fault: str,
) -> None:
    """Cut only replica A's database sockets; replica B must continue serving."""

    processes: list[LoggedServer] = []
    headers = {"X-API-Key": _OPERATOR_KEY}
    async with _database_proxy(clean_postgres_database) as (proxy, proxied_url):
        try:
            first_port = _unused_port()
            first = _start_server(proxied_url, "network-first", first_port, pool_timeout=1, operation_timeout=2)
            processes.append(first)
            second_port = _unused_port()
            second = _start_server(clean_postgres_database, "network-second", second_port)
            processes.append(second)
            first_url = f"http://127.0.0.1:{first_port}"
            second_url = f"http://127.0.0.1:{second_port}"
            first_ws = f"ws://127.0.0.1:{first_port}/v1/rooms/network-room/ws?username=Alice"
            second_ws = f"ws://127.0.0.1:{second_port}/v1/rooms/network-room/ws?username=Bob"
            async with httpx.AsyncClient(headers=headers, timeout=5, trust_env=False) as client:
                await asyncio.gather(_wait_ready(client, first_url, first), _wait_ready(client, second_url, second))
                assert (
                    await client.post(f"{second_url}/v1/rooms", json={"id": "network-room", "name": "Network"})
                ).status_code == 201
                async with websockets.connect(
                    second_ws, additional_headers=headers, open_timeout=5, close_timeout=1
                ) as bob:
                    assert json.loads(await asyncio.wait_for(bob.recv(), 5))["type"] == "ready"
                    assert json.loads(await asyncio.wait_for(bob.recv(), 5))["type"] == "history"
                    async with websockets.connect(
                        first_ws, additional_headers=headers, open_timeout=5, close_timeout=1
                    ) as alice:
                        assert json.loads(await asyncio.wait_for(alice.recv(), 5))["type"] == "ready"
                        assert json.loads(await asyncio.wait_for(alice.recv(), 5))["type"] == "history"
                        await _receive_type(bob, "presence.joined", username="Alice")
                        await bob.send(json.dumps({"type": "message", "content": "before interruption"}))
                        before = await _receive_type(alice, "message.created")
                        assert (await _receive_type(bob, "message.created"))["message"]["id"] == before["message"]["id"]

                        if fault == "reset":
                            proxy.cut()
                        else:
                            # Keep TCP open but stop forwarding either direction, including new handshakes.
                            proxy.forwarding.clear()
                        await asyncio.wait_for(alice.wait_closed(), timeout=10)
                        assert alice.close_code == 1012
                        assert first.poll() is None, "database interruption must not kill the application process"
                        assert (await client.get(f"{first_url}/healthz")).status_code == 200
                        unavailable = await client.get(f"{first_url}/readyz")
                        assert unavailable.status_code == 503
                        assert unavailable.json() == {"status": "not_ready"}
                        assert (await client.get(f"{second_url}/readyz")).status_code == 200
                        left = await _receive_type(bob, "presence.left", username="Alice")
                        assert left["active_connections"] == 1

                        rejected = await client.post(
                            f"{first_url}/v1/rooms/network-room/messages",
                            json={"sender": "Alice", "content": "must not commit"},
                        )
                        assert rejected.status_code == 503
                        assert rejected.json() == {
                            "error": {
                                "code": "storage_unavailable",
                                "message": "Chat storage is temporarily unavailable",
                            }
                        }
                        await bob.send(json.dumps({"type": "message", "content": "during interruption"}))
                        during = await _receive_type(bob, "message.created")
                        proxy.enabled = True
                        proxy.forwarding.set()
                        await _wait_ready(client, first_url, first)

                    async with websockets.connect(
                        first_ws, additional_headers=headers, open_timeout=5, close_timeout=1
                    ) as recovered:
                        assert json.loads(await asyncio.wait_for(recovered.recv(), 5))["type"] == "ready"
                        history = json.loads(await asyncio.wait_for(recovered.recv(), 5))
                        assert history["type"] == "history"
                        assert {item["id"] for item in history["items"]} == {
                            before["message"]["id"],
                            during["message"]["id"],
                        }
                        await _receive_type(bob, "presence.joined", username="Alice")
                        await recovered.send(json.dumps({"type": "message", "content": "after recovery"}))
                        after = await _receive_type(recovered, "message.created")
                        assert (await _receive_type(bob, "message.created"))["message"]["id"] == after["message"]["id"]
                        assert (await client.get(f"{first_url}/v1/stats")).json() == {"active_connections": 2}
        finally:
            # The proxy must keep forwarding while graceful process shutdown releases database leases.
            proxy.enabled = True
            proxy.forwarding.set()
            for process in reversed(processes):
                await asyncio.to_thread(_stop_server, process)


def test_child_output_is_drained_beyond_pipe_capacity_and_remains_bounded() -> None:
    process = LoggedServer(
        [sys.executable, "-c", "import sys; sys.stdout.write('x' * 1000000 + '\\noutput-complete\\n')"],
        env=dict(os.environ),
    )
    try:
        # A PIPE with no concurrent reader would block this child before it can exit.
        assert process.wait(timeout=10) == 0
    finally:
        _stop_server(process)
    assert not process.reader.is_alive()
    assert process.stdout is not None and process.stdout.closed
    output = _process_output(process)
    assert output.endswith("output-complete\n")
    assert len(output) <= 4_000


@pytest.fixture
async def moderation_replicas(clean_postgres_database: str) -> AsyncIterator[tuple[httpx.AsyncClient, int, int]]:
    processes: list[LoggedServer] = []
    try:
        async with httpx.AsyncClient(timeout=5, trust_env=False) as client:
            first_port = _unused_port()
            first = _start_server(clean_postgres_database, "moderation-a", first_port, signing_secret=_SIGNING_SECRET)
            processes.append(first)
            await _wait_ready(client, f"http://127.0.0.1:{first_port}", first)
            second_port = _unused_port()
            second = _start_server(clean_postgres_database, "moderation-b", second_port, signing_secret=_SIGNING_SECRET)
            processes.append(second)
            await _wait_ready(client, f"http://127.0.0.1:{second_port}", second)
            for room_id in ("moderated", "unrelated"):
                created = await client.post(
                    f"http://127.0.0.1:{first_port}/v1/rooms",
                    headers={"X-API-Key": _OPERATOR_KEY},
                    json={"id": room_id, "name": room_id},
                )
                assert created.status_code == 201
            yield client, first_port, second_port
    finally:
        for process in reversed(processes):
            await asyncio.to_thread(_stop_server, process)


def _member_token(subject: str, room_id: str = "moderated") -> str:
    return AccessTokenService(_SIGNING_SECRET).issue(
        subject, rooms=[room_id], permissions=["room:read", "room:write"], expires_in_seconds=300
    )


@asynccontextmanager
async def _member_socket(
    port: int, subject: str, room_id: str = "moderated"
) -> AsyncIterator[tuple[Any, dict[str, Any]]]:
    async with websockets.connect(
        f"ws://127.0.0.1:{port}/v1/rooms/{room_id}/ws",
        additional_headers={"Authorization": f"Bearer {_member_token(subject, room_id)}"},
        open_timeout=5,
        close_timeout=1,
    ) as connection:
        ready = json.loads(await asyncio.wait_for(connection.recv(), 5))
        assert ready["type"] == "ready"
        assert ready["username"] == subject
        history = json.loads(await asyncio.wait_for(connection.recv(), 5))
        assert history["type"] == "history"
        yield connection, history


async def _wait_connection_count(client: httpx.AsyncClient, port: int, expected: int) -> None:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        response = await client.get(f"http://127.0.0.1:{port}/v1/stats", headers={"X-API-Key": _OPERATOR_KEY})
        response.raise_for_status()
        if response.json() == {"active_connections": expected}:
            return
        await asyncio.sleep(0.05)
    pytest.fail(f"global connection count did not converge to {expected}")


async def _assert_member_reconnect_denied(port: int, *, code: str, close_code: int) -> None:
    async with websockets.connect(
        f"ws://127.0.0.1:{port}/v1/rooms/moderated/ws",
        additional_headers={"Authorization": f"Bearer {_member_token('Alice')}"},
        open_timeout=5,
        close_timeout=1,
    ) as connection:
        rejection = json.loads(await asyncio.wait_for(connection.recv(), 5))
        assert rejection["type"] == "error"
        assert rejection["code"] == code
        await asyncio.wait_for(connection.wait_closed(), 5)
        assert connection.close_code == close_code


@pytest.mark.asyncio
async def test_cross_process_freeze_mute_ban_and_unban_preserve_room_isolation(
    moderation_replicas: tuple[httpx.AsyncClient, int, int],
) -> None:
    client, first_port, second_port = moderation_replicas
    first = f"http://127.0.0.1:{first_port}/v1/rooms/moderated"
    second = f"http://127.0.0.1:{second_port}/v1/rooms/moderated"
    operator = {"X-API-Key": _OPERATOR_KEY}
    alice_headers = {"Authorization": f"Bearer {_member_token('Alice')}"}
    bob_headers = {"Authorization": f"Bearer {_member_token('Bob')}"}
    async with (
        _member_socket(first_port, "Alice") as (alice_a, _),
        _member_socket(second_port, "Alice") as (alice_b, _),
        _member_socket(second_port, "Bob") as (bob, _),
        _member_socket(first_port, "Alice", "unrelated") as (unrelated, _),
    ):
        assert (await client.patch(first, headers=bob_headers, json={"frozen": True})).status_code == 403
        assert (await client.patch(first, headers=operator, json={"frozen": True})).status_code == 200
        for connection in (alice_a, alice_b, bob):
            assert (await _receive_type(connection, "room.frozen"))["room"]["frozen_at"] is not None
        blocked = await client.post(f"{second}/messages", headers=bob_headers, json={"content": "blocked by freeze"})
        assert blocked.status_code == 409 and blocked.json()["error"]["code"] == "room_frozen"
        await bob.send(json.dumps({"type": "message", "content": "blocked socket write"}))
        assert (await _receive_type(bob, "error"))["code"] == "room_frozen"
        announcement = await client.post(
            f"{first}/messages", headers=operator, json={"sender": "operator", "content": "announcement"}
        )
        assert announcement.status_code == 201
        for connection in (alice_a, alice_b, bob):
            assert (await _receive_type(connection, "message.created"))["message"]["id"] == announcement.json()["id"]
        assert (await client.patch(second, headers=operator, json={"frozen": False})).status_code == 200
        for connection in (alice_a, alice_b, bob):
            await _receive_type(connection, "room.unfrozen")

        moderation = f"{first}/members/Alice/moderation"
        assert (
            await client.patch(moderation, headers=bob_headers, json={"banned_for_seconds": 300})
        ).status_code == 403
        assert (await client.patch(moderation, headers=operator, json={"muted_for_seconds": 300})).status_code == 200
        muted = await client.post(f"{second}/messages", headers=alice_headers, json={"content": "blocked by mute"})
        assert muted.status_code == 403 and muted.json()["error"]["code"] == "room_muted"
        assert (await client.get(f"{second}/messages", headers=alice_headers)).status_code == 200
        await alice_b.send(json.dumps({"type": "message", "content": "muted socket write"}))
        assert (await _receive_type(alice_b, "error"))["code"] == "room_muted"
        assert (
            await client.patch(f"{second}/members/Alice/moderation", headers=operator, json={"muted_for_seconds": 0})
        ).status_code == 200
        resumed = await client.post(f"{second}/messages", headers=alice_headers, json={"content": "after unmute"})
        assert resumed.status_code == 201
        for connection in (alice_a, alice_b, bob):
            assert (await _receive_type(connection, "message.created"))["message"]["id"] == resumed.json()["id"]

        assert (await client.patch(moderation, headers=operator, json={"banned_for_seconds": 300})).status_code == 200
        for connection in (alice_a, alice_b):
            assert (await _receive_type(connection, "member.banned"))["subject"] == "Alice"
            await asyncio.wait_for(connection.wait_closed(), 5)
            assert connection.close_code == 4403
        for port in (first_port, second_port):
            await _wait_connection_count(client, port, 2)
            await _assert_member_reconnect_denied(port, code="room_banned", close_code=4403)
        denied = await client.get(f"{second}/messages", headers=alice_headers)
        assert denied.status_code == 403 and denied.json()["error"]["code"] == "room_banned"
        departures = [await _receive_type(bob, "presence.left", username="Alice") for _ in range(2)]
        assert sorted(event["active_connections"] for event in departures) == [1, 2]
        # An identically named subject in another room must receive neither moderation nor room messages.
        await unrelated.send(json.dumps({"type": "ping"}))
        assert json.loads(await asyncio.wait_for(unrelated.recv(), 5)) == {"type": "pong"}
        await unrelated.send(json.dumps({"type": "message", "content": "other room remains writable"}))
        assert (await _receive_type(unrelated, "message.created"))["message"][
            "content"
        ] == "other room remains writable"
        await bob.send(json.dumps({"type": "ping"}))
        assert json.loads(await asyncio.wait_for(bob.recv(), 5)) == {"type": "pong"}
        assert (
            await client.patch(f"{second}/members/Alice/moderation", headers=operator, json={"banned_for_seconds": 0})
        ).status_code == 200
        async with _member_socket(first_port, "Alice") as (_, history):
            assert {item["id"] for item in history["items"]} == {announcement.json()["id"], resumed.json()["id"]}


@pytest.mark.asyncio
async def test_cross_process_archive_closes_both_replicas_and_reopen_restores_history(
    moderation_replicas: tuple[httpx.AsyncClient, int, int],
) -> None:
    client, first_port, second_port = moderation_replicas
    first = f"http://127.0.0.1:{first_port}/v1/rooms/moderated"
    second = f"http://127.0.0.1:{second_port}/v1/rooms/moderated"
    operator = {"X-API-Key": _OPERATOR_KEY}
    member = {"Authorization": f"Bearer {_member_token('Alice')}"}
    async with (
        _member_socket(first_port, "Alice") as (alice, _),
        _member_socket(second_port, "Bob") as (bob, _),
        _member_socket(second_port, "Alice", "unrelated") as (unrelated, _),
    ):
        saved = await client.post(f"{first}/messages", headers=member, json={"content": "retained across archive"})
        assert saved.status_code == 201
        for connection in (alice, bob):
            assert (await _receive_type(connection, "message.created"))["message"]["id"] == saved.json()["id"]
        assert (await client.patch(second, headers=operator, json={"archived": True})).status_code == 200
        for connection in (alice, bob):
            archived = await _receive_type(connection, "room.archived")
            assert archived["room"]["archived_at"] is not None
            await asyncio.wait_for(connection.wait_closed(), 5)
            assert connection.close_code == 4409
        for port in (first_port, second_port):
            await _wait_connection_count(client, port, 1)
            await _assert_member_reconnect_denied(port, code="room_archived", close_code=4409)
        blocked = await client.post(f"{first}/messages", headers=member, json={"content": "must not append"})
        assert blocked.status_code == 409 and blocked.json()["error"]["code"] == "room_archived"
        assert (await client.get(f"{second}/messages", headers=member)).json()["items"][0]["id"] == saved.json()["id"]
        await unrelated.send(json.dumps({"type": "ping"}))
        assert json.loads(await asyncio.wait_for(unrelated.recv(), 5)) == {"type": "pong"}
        assert (await client.patch(first, headers=operator, json={"archived": False})).status_code == 200
        async with (
            _member_socket(first_port, "Alice") as (reopened_a, history_a),
            _member_socket(second_port, "Bob") as (reopened_b, history_b),
        ):
            assert history_a["items"] == history_b["items"] == [saved.json()]
            await reopened_a.send(json.dumps({"type": "message", "content": "after reopen"}))
            message = await _receive_type(reopened_a, "message.created")
            assert (await _receive_type(reopened_b, "message.created"))["message"]["id"] == message["message"]["id"]
