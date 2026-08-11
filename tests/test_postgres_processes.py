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
import time
from typing import Any

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


def _start_server(conninfo: str, instance_id: str, port: int) -> subprocess.Popen[str]:
    environment = {key: value for key, value in os.environ.items() if not key.startswith("SAMSARIX_CHAT_")}
    environment.update(
        {
            "SAMSARIX_CHAT_STORAGE": "postgres",
            "SAMSARIX_CHAT_POSTGRES_URL": conninfo,
            "SAMSARIX_CHAT_POSTGRES_INSTANCE_ID": instance_id,
            "SAMSARIX_CHAT_POSTGRES_MAX_POOL_SIZE": "4",
            "SAMSARIX_CHAT_POSTGRES_LEASE_SECONDS": "3",
            "SAMSARIX_CHAT_POSTGRES_RELAY_POLL": "0.05",
            "SAMSARIX_CHAT_POSTGRES_MAINTENANCE_INTERVAL": "0.1",
            "SAMSARIX_CHAT_API_KEY": _OPERATOR_KEY,
            "SAMSARIX_CHAT_MAX_CONNECTIONS": "8",
            "SAMSARIX_CHAT_MAX_CONNECTIONS_PER_ROOM": "8",
        }
    )
    return subprocess.Popen(  # noqa: S603 - fixed interpreter and argument vector
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
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def _stop_server(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=12)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def _process_output(process: subprocess.Popen[str]) -> str:
    if process.poll() is None or process.stdout is None:
        return ""
    return process.stdout.read()[-4_000:]


async def _wait_ready(client: httpx.AsyncClient, base_url: str, process: subprocess.Popen[str]) -> None:
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
    processes: list[subprocess.Popen[str]] = []
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
