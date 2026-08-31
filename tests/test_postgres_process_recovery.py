# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Kernel-paused replica recovery with live lag and naturally pruned event gaps."""

from __future__ import annotations

import asyncio
import json
import signal
import sys
import time
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import httpx2 as httpx
import pytest

psycopg = pytest.importorskip("psycopg")
websockets = pytest.importorskip("websockets.asyncio.client")
from websockets.exceptions import ConnectionClosed  # noqa: E402

from tests.process_helpers import LoggedServer, _stop_server, _unused_port, _wait_ready  # noqa: E402
from tests.test_postgres_processes import (  # noqa: E402
    _OPERATOR_KEY,
    _SIGNING_SECRET,
    _member_token,
    _receive_type,
    _start_server,
    _wait_connection_count,
)

pytestmark = [
    pytest.mark.postgres,
    pytest.mark.skipif(sys.platform != "linux", reason="kernel pause barrier requires Linux SIGSTOP and procfs"),
]
_NAMES = ("recovery-paused", "recovery-healthy")
_OPERATOR = {"X-API-Key": _OPERATOR_KEY}


def _member(subject: str, room: str = "shared") -> dict[str, str]:
    return {"Authorization": f"Bearer {_member_token(subject, room)}"}


@dataclass
class Snapshot:
    generation: Any
    cursor: int
    live: bool
    pruned: int
    backlog: int
    age: float


async def _snapshot(connection: Any, name: str) -> Snapshot:
    cursor = await connection.execute(
        """
        SELECT generation, last_sequence, lease_expires_at > clock_timestamp(),
          (SELECT pruned_through_sequence FROM public.samsarix_realtime_retention WHERE singleton),
          (SELECT count(*) FROM public.samsarix_realtime_events WHERE sequence > instance.last_sequence),
          COALESCE((SELECT EXTRACT(EPOCH FROM clock_timestamp() - min(created_at))
                    FROM public.samsarix_realtime_events WHERE sequence > instance.last_sequence), 0)
        FROM public.samsarix_instance_cursors AS instance WHERE instance_id = %s
        """,
        (name,),
    )
    row = await cursor.fetchone()
    assert row is not None
    return Snapshot(row[0], int(row[1]), bool(row[2]), int(row[3]), int(row[4]), float(row[5]))


async def _caught_up(connection: Any, name: str) -> None:
    cursor = await connection.execute("SELECT COALESCE(MAX(sequence), 0) FROM public.samsarix_realtime_events")
    head = (await cursor.fetchone())[0]
    deadline = time.monotonic() + 5
    while (await _snapshot(connection, name)).cursor < head:
        assert time.monotonic() < deadline, f"{name} did not acknowledge the committed head"
        await asyncio.sleep(0.02)


@asynccontextmanager
async def _paused(process: LoggedServer, observer: Any) -> AsyncIterator[None]:
    """Stop only our child, after verifying it holds no database transaction.

    Checking before SIGSTOP alone races a new transaction. Check after the
    kernel confirms the stop; resume/retry if any of its sessions is not idle.
    This avoids pinning unrelated event/connection locks for the whole fault.
    """
    paused = False
    try:
        deadline = time.monotonic() + 5
        while True:
            assert process.poll() is None, process.output_tail()
            process.send_signal(signal.SIGSTOP)
            paused = True
            while True:
                status = await asyncio.to_thread(Path(f"/proc/{process.pid}/status").read_text)
                state = next(line for line in status.splitlines() if line.startswith("State:")).split()[1]
                if state == "T":
                    break
                assert time.monotonic() < deadline, "child did not reach kernel-stopped state"
                await asyncio.sleep(0.01)
            cursor = await observer.execute(
                "SELECT state, xact_start FROM pg_stat_activity WHERE application_name = %s",
                (_NAMES[0],),
            )
            sessions = await cursor.fetchall()
            if sessions and all(state == "idle" and started is None for state, started in sessions):
                break
            process.send_signal(signal.SIGCONT)
            paused = False
            assert time.monotonic() < deadline, "could not pause the child outside a database transaction"
            await asyncio.sleep(0.02)
        yield
    finally:
        if paused and process.poll() is None:
            process.send_signal(signal.SIGCONT)


@asynccontextmanager
async def _socket(port: int, subject: str, room: str = "shared") -> AsyncIterator[tuple[Any, dict[str, Any]]]:
    async with websockets.connect(
        f"ws://127.0.0.1:{port}/v1/rooms/{room}/ws",
        additional_headers=_member(subject, room),
        open_timeout=5,
        close_timeout=1,
        # Do not let a client's keepalive timeout impersonate server-side fencing
        # while the server is deliberately paused through a real prune interval.
        ping_interval=None,
    ) as connection:
        ready = json.loads(await asyncio.wait_for(connection.recv(), 5))
        assert ready["type"] == "ready" and ready["username"] == subject
        history = json.loads(await asyncio.wait_for(connection.recv(), 5))
        assert history["type"] == "history"
        await connection.send(json.dumps({"type": "ping"}))
        await _receive_type(connection, "pong")
        yield connection, history


async def _assert_fenced_without_obsolete_frames(connection: Any) -> None:
    deadline = time.monotonic() + 10
    for _ in range(32):
        try:
            raw = await asyncio.wait_for(connection.recv(), max(0.01, deadline - time.monotonic()))
        except ConnectionClosed:
            assert connection.close_code == 1012
            return
        event = json.loads(raw)
        # A connection heartbeat may discover the expired lease before the relay.
        assert event["type"] == "error" and event["code"] == "storage_unavailable", event
    pytest.fail("stale client was not fenced within bounded frames")


async def _expect_message(connection: Any, message_id: str, *, allow_presence: bool = False) -> None:
    deadline = time.monotonic() + 5
    for _ in range(32):
        event = json.loads(await asyncio.wait_for(connection.recv(), max(0.01, deadline - time.monotonic())))
        if allow_presence and event["type"].startswith("presence."):
            continue
        assert event["type"] == "message.created" and event["message"]["id"] == message_id, event
        return
    pytest.fail("live sentinel not received within bounded frames")


@pytest.mark.timeout(120)
@pytest.mark.parametrize("fault", ["count", "age", "retained-gap"])
async def test_paused_replica_fences_obsolete_state_and_recovers_signed_members(
    clean_postgres_database: str, fault: str
) -> None:
    settings = {
        "POSTGRES_LEASE_SECONDS": "3" if fault == "retained-gap" else "120",
        "POSTGRES_RELAY_MAX_PENDING_EVENTS": "8" if fault == "count" else "100",
        "POSTGRES_RELAY_MAX_EVENT_AGE": "2" if fault == "age" else "3600",
        "POSTGRES_MAX_REALTIME_EVENTS": "3" if fault == "retained-gap" else "100000",
    }
    processes = []
    ports = []
    try:
        async with (
            httpx.AsyncClient(timeout=5, trust_env=False) as client,
            await psycopg.AsyncConnection.connect(clean_postgres_database, autocommit=True) as observer,
        ):
            await observer.execute("SET statement_timeout = '5s'")
            for name in _NAMES:
                parts = urlsplit(clean_postgres_database)
                query = dict(parse_qsl(parts.query))
                query["application_name"] = name
                port = _unused_port()
                process = _start_server(
                    urlunsplit(parts._replace(query=urlencode(query))),
                    name,
                    port,
                    signing_secret=_SIGNING_SECRET,
                    settings=settings,
                )
                processes.append(process)
                ports.append(port)
                await _wait_ready(client, f"http://127.0.0.1:{port}", process)
            assert len({process.pid for process in processes}) == 2
            base = f"http://127.0.0.1:{ports[1]}"

            async def request(method: str, path: str, *, status: int = 200, **kwargs: Any) -> Any:
                response = await client.request(method, f"{base}/v1/rooms/{path}", **kwargs)
                assert response.status_code == status, response.text
                # Keep the healthy replica below the same limits. Only the
                # stopped process should accumulate the deliberately old backlog.
                await _caught_up(observer, _NAMES[1])
                return response.json() if response.content else None

            for room in ("shared", "unrelated"):
                response = await client.post(f"{base}/v1/rooms", headers=_OPERATOR, json={"id": room, "name": room})
                assert response.status_code == 201
            async with (
                _socket(ports[0], "Alice") as (stale, _),
                _socket(ports[1], "Observer", "unrelated") as (healthy, _),
            ):
                before = await request(
                    "POST", "shared/messages", status=201, headers=_member("Bob"), json={"content": "before pause"}
                )
                assert (await _receive_type(stale, "message.created"))["message"]["id"] == before["id"]
                await _caught_up(observer, _NAMES[0])
                async with _paused(processes[0], observer):
                    original = await _snapshot(observer, _NAMES[0])
                    assert original.live and original.backlog == 0
                    created = []
                    for content in ("edit me", "erase me", "keep during pause"):
                        created.append(
                            await request(
                                "POST", "shared/messages", status=201, headers=_member("Bob"), json={"content": content}
                            )
                        )
                    await request(
                        "PATCH",
                        f"shared/messages/{created[0]['id']}",
                        headers=_member("Bob"),
                        json={"content": "edited during pause"},
                    )
                    await request("DELETE", f"shared/messages/{created[1]['id']}", status=204, headers=_member("Bob"))
                    for update in ({"frozen": True}, {"frozen": False}, {"archived": True}, {"archived": False}):
                        await request("PATCH", "shared", headers=_OPERATOR, json=update)
                    alive = await request(
                        "POST",
                        "unrelated/messages",
                        status=201,
                        headers=_member("Observer", "unrelated"),
                        json={"content": "healthy while peer is stopped"},
                    )
                    await _expect_message(healthy, alive["id"])
                    assert (await client.get(f"{base}/readyz")).status_code == 200
                    expected = (await request("GET", "shared/messages", headers=_member("Bob")))["items"]
                    by_id = {item["id"]: item for item in expected}
                    assert len(by_id) == 4 and by_id[created[0]["id"]]["content"] == "edited during pause"
                    assert by_id[created[1]["id"]]["content"] == "" and by_id[created[1]["id"]]["deleted_at"]
                    deadline = time.monotonic() + (75 if fault == "retained-gap" else 5)
                    while True:
                        paused = await _snapshot(observer, _NAMES[0])
                        assert paused.generation == original.generation and paused.cursor == original.cursor
                        if fault == "retained-gap":
                            reached = not paused.live and paused.pruned > original.cursor
                        else:
                            assert paused.live and paused.pruned <= original.cursor
                            reached = paused.backlog > 8 if fault == "count" else paused.age > 2
                        if reached:
                            break
                        assert time.monotonic() < deadline, f"fault barrier not reached: {paused}"
                        await asyncio.sleep(0.05)
                    # No clock, cursor, lease, event row, prune timer, or application
                    # method is rewritten. Gap pruning is the healthy process's
                    # real periodic maintenance after natural lease expiry.
                await _assert_fenced_without_obsolete_frames(stale)
                assert processes[0].poll() is None
                reason = (
                    f"lag exceeded its {fault} limit"
                    if fault != "retained-gap"
                    else "retained PostgreSQL event-log gap"
                )
                deadline = time.monotonic() + 10
                while True:
                    recovered = await _snapshot(observer, _NAMES[0])
                    if (
                        reason in processes[0].output_tail()
                        and recovered.live
                        and recovered.generation != original.generation
                        and recovered.cursor > original.cursor
                    ):
                        break
                    assert time.monotonic() < deadline, processes[0].output_tail()
                    await asyncio.sleep(0.02)
                await _wait_ready(client, f"http://127.0.0.1:{ports[0]}", processes[0])
                await _wait_connection_count(client, ports[1], 1)
                deadline = time.monotonic() + 5
                while True:
                    cursor = await observer.execute(
                        "SELECT count(*) FROM public.samsarix_connection_leases WHERE instance_id = %s", (_NAMES[0],)
                    )
                    if (await cursor.fetchone())[0] == 0:
                        break
                    assert time.monotonic() < deadline, "old physical connection leases were not cleaned up"
                    await asyncio.sleep(0.02)
                async with AsyncExitStack() as members:
                    connections = [(ports[0], subject) for subject in ("Alice", "Agent", "Reader")]
                    connections.append((ports[1], "Bob"))
                    joining = [
                        asyncio.create_task(members.enter_async_context(_socket(port, subject)))
                        for port, subject in connections
                    ]
                    try:
                        joined = await asyncio.gather(*joining)
                    finally:
                        # Finish/cancel every enter before the stack exits, even
                        # if one handshake fails while the others are pending.
                        for task in joining:
                            task.cancel()
                        await asyncio.gather(*joining, return_exceptions=True)
                    assert all(history["items"] == expected for _, history in joined)
                    current = await request("GET", "shared", headers=_member("Bob"))
                    assert current["archived_at"] is None and current["frozen_at"] is None
                    after = await request(
                        "POST",
                        "shared/messages",
                        status=201,
                        headers=_member("Bob"),
                        json={"content": "after recovery"},
                    )
                    for connection, _ in joined:
                        await _expect_message(connection, after["id"], allow_presence=True)
                    await _wait_connection_count(client, ports[0], 5)
                    denied = await client.get(f"{base}/v1/rooms/unrelated/messages", headers=_member("Alice"))
                    assert denied.status_code == 403
                    sentinel = await request(
                        "POST",
                        "unrelated/messages",
                        status=201,
                        headers=_member("Observer", "unrelated"),
                        json={"content": "healthy through peer recovery"},
                    )
                    await _expect_message(healthy, sentinel["id"])
            await _wait_connection_count(client, ports[1], 0)
    finally:
        for process in reversed(processes):
            await asyncio.to_thread(_stop_server, process)
