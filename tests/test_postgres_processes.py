# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Real-process PostgreSQL failover and reconnect acceptance tests."""

from __future__ import annotations

import asyncio
import json
import os
import socket
import subprocess
import sys
import threading
import time
from collections import deque
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from typing import Any
from urllib.parse import parse_qs, urlsplit

import httpx2 as httpx
import pytest

pytest.importorskip("psycopg")
websockets = pytest.importorskip("websockets.asyncio.client")

pytestmark = pytest.mark.postgres

_OPERATOR_KEY = "postgres-process-operator-key-1234"


def _unused_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


class LoggedServer(subprocess.Popen[str]):
    """Drain child diagnostics continuously into a bounded in-memory tail."""

    def __init__(self, command: list[str], *, env: dict[str, str]) -> None:
        super().__init__(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        self._output: deque[str] = deque(maxlen=32)
        self._output_lock = threading.Lock()
        self.reader = threading.Thread(target=self._drain, name=f"test-server-output-{self.pid}", daemon=True)
        self.reader.start()

    def _drain(self) -> None:
        assert self.stdout is not None
        try:
            while chunk := self.stdout.readline(4096):
                with self._output_lock:
                    self._output.append(chunk)
        finally:
            self.stdout.close()

    def output_tail(self) -> str:
        with self._output_lock:
            return "".join(self._output)[-4_000:]


def _start_server(
    conninfo: str, instance_id: str, port: int, *, pool_timeout: float = 10, operation_timeout: float = 10
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


def _stop_server(process: LoggedServer) -> None:
    try:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=12)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
    finally:
        process.reader.join(timeout=2)


def _process_output(process: LoggedServer) -> str:
    return process.output_tail()


async def _wait_ready(client: httpx.AsyncClient, base_url: str, process: LoggedServer) -> None:
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if process.poll() is not None:
            pytest.fail(f"server exited before readiness:\n{_process_output(process)}")
        try:
            response = await client.get(f"{base_url}/readyz")
            if response.status_code == 200:
                return
        except httpx.HTTPError:
            pass
        await asyncio.sleep(0.05)
    pytest.fail(f"server did not become ready:\n{_process_output(process)}")


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
            async with (
                websockets.connect(
                    f"ws://127.0.0.1:{first_port}/v1/rooms/process-room/ws?username=Alice",
                    additional_headers=websocket_headers,
                    open_timeout=5,
                    close_timeout=1,
                ) as alice,
                websockets.connect(
                    f"ws://127.0.0.1:{second_port}/v1/rooms/process-room/ws?username=Bob",
                    additional_headers=websocket_headers,
                    open_timeout=5,
                    close_timeout=1,
                ) as bob,
            ):
                assert json.loads(await alice.recv())["type"] == "ready"
                assert json.loads(await alice.recv())["type"] == "history"
                assert json.loads(await bob.recv())["type"] == "ready"
                assert json.loads(await bob.recv())["type"] == "history"
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
