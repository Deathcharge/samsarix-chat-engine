# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Bounded contention acceptance against two independent CLI server processes."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator, Coroutine
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import httpx2 as httpx
import pytest

psycopg = pytest.importorskip("psycopg")
websockets = pytest.importorskip("websockets.asyncio.client")

from samsarix_chat_engine.postgres_connections import POSTGRES_CONNECTION_CAP_LOCK_ID  # noqa: E402
from samsarix_chat_engine.postgres_store import POSTGRES_ROOM_CAP_LOCK_ID  # noqa: E402
from tests.test_postgres_processes import (  # noqa: E402
    _OPERATOR_KEY,
    _SIGNING_SECRET,
    _member_socket,
    _member_token,
    _receive_type,
    _start_server,
    _stop_server,
    _unused_port,
    _wait_connection_count,
    _wait_ready,
)

pytestmark = pytest.mark.postgres
_NAMES = ["contention-a", "contention-b"]
_OPERATOR = {"X-API-Key": _OPERATOR_KEY}


@dataclass
class Replicas:
    client: httpx.AsyncClient
    ports: list[int]
    conninfo: str

    def room(self, index: int, room_id: str = "shared") -> str:
        return f"http://127.0.0.1:{self.ports[index]}/v1/rooms/{room_id}"


@asynccontextmanager
async def _replicas(conninfo: str, **settings: str) -> AsyncIterator[Replicas]:
    processes = []
    ports = []
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            for name in _NAMES:
                port = _unused_port()
                parts = urlsplit(conninfo)
                query = dict(parse_qsl(parts.query))
                query["application_name"] = name
                named_conninfo = urlunsplit(parts._replace(query=urlencode(query)))
                process = _start_server(
                    named_conninfo,
                    name,
                    port,
                    signing_secret=_SIGNING_SECRET,
                    settings={"POSTGRES_LEASE_SECONDS": "30", **settings},
                )
                processes.append(process)
                ports.append(port)
                await _wait_ready(client, f"http://127.0.0.1:{port}", process)
            assert len({process.pid for process in processes}) == 2
            yield Replicas(client, ports, conninfo)
    finally:
        for process in reversed(processes):
            await asyncio.to_thread(_stop_server, process)


def _member(subject: str, room_id: str = "shared") -> dict[str, str]:
    return {"Authorization": f"Bearer {_member_token(subject, room_id)}"}


async def _create_room(replicas: Replicas, room_id: str = "shared") -> None:
    response = await replicas.client.post(
        f"http://127.0.0.1:{replicas.ports[0]}/v1/rooms",
        headers=_OPERATOR,
        json={"id": room_id, "name": room_id},
    )
    assert response.status_code == 201, response.text


async def _contend(
    replicas: Replicas,
    operations: list[Coroutine[Any, Any, Any]],
    *,
    lock_id: int | None = None,
    rate_scope: str | None = None,
) -> list[Any]:
    """Release a real DB barrier only after both named replicas are lock-waiting.

    The observer is autocommit: pg_stat_activity snapshots must not be reused
    across polls. No application method, clock, or response is mocked.
    """
    tasks: list[asyncio.Task[Any]] = []
    try:
        async with (
            await psycopg.AsyncConnection.connect(replicas.conninfo) as holder,
            await psycopg.AsyncConnection.connect(replicas.conninfo, autocommit=True) as observer,
        ):
            if lock_id is not None:
                await holder.execute("SELECT pg_advisory_xact_lock(%s)", (lock_id,))
                query_pattern = "%pg_advisory_xact_lock%"
            elif rate_scope is not None:
                cursor = await holder.execute(
                    "SELECT key_digest FROM public.samsarix_rate_buckets WHERE scope = %s FOR UPDATE",
                    (rate_scope,),
                )
                assert len(await cursor.fetchall()) == 1
                query_pattern = "%UPDATE public.samsarix_rate_buckets%"
            else:
                await holder.execute("SELECT id FROM public.samsarix_rooms WHERE id = 'shared' FOR UPDATE")
                query_pattern = "%FROM public.samsarix_rooms WHERE id =%FOR UPDATE%"
            tasks = [asyncio.create_task(operation) for operation in operations]
            deadline = time.monotonic() + 5
            while True:
                cursor = await observer.execute(
                    """
                    SELECT DISTINCT application_name FROM pg_stat_activity
                    WHERE application_name = ANY(%s) AND state = 'active'
                      AND wait_event_type = 'Lock' AND query LIKE %s
                    """,
                    (_NAMES, query_pattern),
                )
                waiting = {row[0] for row in await cursor.fetchall()}
                if waiting == set(_NAMES):
                    break
                assert time.monotonic() < deadline, f"both replicas did not contend: {waiting}"
                assert not any(task.done() for task in tasks), "request completed before barrier release"
                await asyncio.sleep(0.02)
            await holder.commit()
            return list(await asyncio.wait_for(asyncio.gather(*tasks), timeout=12))
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if not tasks:
            for operation in operations:
                operation.close()


async def _activate(connection: Any) -> None:
    await connection.send(json.dumps({"type": "ping"}))
    await _receive_type(connection, "pong")


async def _next_message(connection: Any) -> dict[str, Any]:
    deadline = time.monotonic() + 10
    for _ in range(64):
        event = json.loads(await asyncio.wait_for(connection.recv(), max(0.01, deadline - time.monotonic())))
        assert event["type"] != "error", event
        if event["type"].startswith("message."):
            return event
        assert event["type"].startswith("presence."), event
    pytest.fail("message not reached within bounded frame count")


async def _message_prefix(connection: Any, marker_id: str) -> list[dict[str, Any]]:
    """Keep every message frame up to a later committed sentinel, including repeats."""
    events = []
    for _ in range(16):
        event = await _next_message(connection)
        events.append(event)
        if event["message"]["id"] == marker_id:
            return events
    pytest.fail("message sentinel not reached within bounded frame count")


async def test_process_idempotency_mutations_fanout_and_reconnect(clean_postgres_database: str) -> None:
    async with _replicas(clean_postgres_database) as replicas:
        await _create_room(replicas)
        alice = _member("Alice")
        bob = _member("Bob")
        async with (
            _member_socket(replicas.ports[0], "Alice", "shared") as (first, _),
            _member_socket(replicas.ports[1], "Bob", "shared") as (second, _),
        ):
            await asyncio.gather(_activate(first), _activate(second))
            created = await _contend(
                replicas,
                [
                    replicas.client.post(
                        f"{replicas.room(index)}/messages",
                        headers=alice,
                        json={"content": "original", "client_message_id": "same-request"},
                    )
                    for index in range(2)
                ],
            )
            assert sorted(response.status_code for response in created) == [200, 201]
            assert created[0].json() == created[1].json()
            message_id = created[0].json()["id"]
            for index in range(2):
                url = f"{replicas.room(index)}/messages/{message_id}"
                for response in (
                    await replicas.client.patch(url, headers=bob, json={"content": "not the author"}),
                    await replicas.client.delete(url, headers=bob),
                ):
                    assert response.status_code == 403
                    assert response.json()["error"]["code"] == "message_not_owned"
            updated = await _contend(
                replicas,
                [
                    replicas.client.patch(
                        f"{replicas.room(index)}/messages/{message_id}",
                        headers=alice,
                        json={"content": f"edit-{index}"},
                    )
                    for index in range(2)
                ],
            )
            assert [response.status_code for response in updated] == [200, 200]
            edited_history = await replicas.client.get(f"{replicas.room(0)}/messages", headers=alice)
            assert edited_history.status_code == 200
            # Read the pre-delete frames before deletion intentionally scrubs retained
            # event payloads. No message frame is discarded, including duplicates.
            first_before = [await _next_message(first) for _ in range(3)]
            second_before = [await _next_message(second) for _ in range(3)]
            deleted = await _contend(
                replicas,
                [
                    replicas.client.delete(f"{replicas.room(index)}/messages/{message_id}", headers=alice)
                    for index in range(2)
                ],
            )
            assert [response.status_code for response in deleted] == [204, 204]
            rejected = await replicas.client.patch(
                f"{replicas.room(1)}/messages/{message_id}", headers=alice, json={"content": "resurrection"}
            )
            assert rejected.status_code == 409 and rejected.json()["error"]["code"] == "message_deleted"
            marker = await replicas.client.post(
                f"{replicas.room(1)}/messages", headers=alice, json={"content": "after all mutations"}
            )
            assert marker.status_code == 201
            first_events, second_events = await asyncio.gather(
                _message_prefix(first, marker.json()["id"]), _message_prefix(second, marker.json()["id"])
            )
            first_events = first_before + first_events
            second_events = second_before + second_events
            assert first_events == second_events
            assert [event["type"] for event in first_events] == [
                "message.created",
                "message.updated",
                "message.updated",
                "message.deleted",
                "message.created",
            ]
            assert first_events[0]["message"] == created[0].json()
            assert {event["message"]["content"] for event in first_events[1:3]} == {"edit-0", "edit-1"}
            assert edited_history.json()["items"] == [first_events[2]["message"]]
            tombstone = first_events[3]["message"]
            assert tombstone["id"] == message_id and tombstone["deleted_at"] is not None
            assert tombstone["content"] == ""
            expected = [tombstone, marker.json()]
            for index in range(2):
                history = await replicas.client.get(f"{replicas.room(index)}/messages", headers=alice)
                assert history.status_code == 200 and history.json()["items"] == expected
        for port in replicas.ports:
            async with _member_socket(port, "Alice", "shared") as (connection, history):
                assert history["items"] == expected
                await _activate(connection)


@pytest.mark.parametrize("per_room", [False, True], ids=["global", "per-room"])
async def test_process_connection_caps_and_reusable_capacity(clean_postgres_database: str, per_room: bool) -> None:
    expected = 2 if per_room else 3
    async with _replicas(
        clean_postgres_database, MAX_CONNECTIONS="8" if per_room else "3", MAX_CONNECTIONS_PER_ROOM="2"
    ) as replicas:
        for room_id in ("shared", "other"):
            await _create_room(replicas, room_id)
        admitted: list[tuple[Any, int, str]] = []

        async def attempt(index: int, room_id: str) -> bool:
            connection = await websockets.connect(
                f"ws://127.0.0.1:{replicas.ports[index]}/v1/rooms/{room_id}/ws",
                additional_headers=_member("Alice", room_id),
                open_timeout=10,
                close_timeout=1,
            )
            try:
                event = json.loads(await asyncio.wait_for(connection.recv(), 10))
                if event["type"] == "error":
                    assert event["code"] == "connection_capacity_reached"
                    await asyncio.wait_for(connection.wait_closed(), 5)
                    assert connection.close_code == 1013
                    return False
                assert event["type"] == "ready"
                assert json.loads(await asyncio.wait_for(connection.recv(), 5))["type"] == "history"
                await _activate(connection)
                admitted.append((connection, index, room_id))
                connection = None
                return True
            finally:
                if connection is not None:
                    await connection.close()

        try:
            outcomes = await _contend(
                replicas,
                [attempt(index % 2, "shared" if per_room or index % 4 < 2 else "other") for index in range(8)],
                lock_id=POSTGRES_CONNECTION_CAP_LOCK_ID,
            )
            assert outcomes.count(True) == expected and outcomes.count(False) == 8 - expected
            assert all(sum(room == room_id for _, _, room in admitted) <= 2 for room_id in ("shared", "other"))
            for port in replicas.ports:
                await _wait_connection_count(replicas.client, port, expected)
            released, index, room_id = admitted.pop()
            await released.close()
            for port in replicas.ports:
                await _wait_connection_count(replicas.client, port, expected - 1)
            assert await attempt(1 - index, room_id)
            for port in replicas.ports:
                await _wait_connection_count(replicas.client, port, expected)
        finally:
            await asyncio.gather(*(connection.close() for connection, _, _ in admitted))
        for port in replicas.ports:
            await _wait_connection_count(replicas.client, port, 0)


async def test_process_room_capacity(clean_postgres_database: str) -> None:
    async with _replicas(clean_postgres_database, MAX_ROOMS="3") as replicas:
        responses = await _contend(
            replicas,
            [
                replicas.client.post(
                    f"http://127.0.0.1:{replicas.ports[index % 2]}/v1/rooms",
                    headers=_OPERATOR,
                    json={"id": f"room-{index}", "name": f"Room {index}"},
                )
                for index in range(8)
            ],
            lock_id=POSTGRES_ROOM_CAP_LOCK_ID,
        )
        assert sorted(response.status_code for response in responses) == [201] * 3 + [507] * 5
        for response in responses:
            if response.status_code == 507:
                assert response.json()["error"]["code"] == "room_capacity_reached"
        expected = {response.json()["id"] for response in responses if response.status_code == 201}
        for port in replicas.ports:
            response = await replicas.client.get(f"http://127.0.0.1:{port}/v1/rooms", headers=_OPERATOR)
            assert response.status_code == 200
            assert {room["id"] for room in response.json()} == expected


async def _rate_window(conninfo: str) -> int:
    async with await psycopg.AsyncConnection.connect(conninfo, autocommit=True) as connection:
        cursor = await connection.execute("SELECT EXTRACT(EPOCH FROM clock_timestamp())")
        row = await cursor.fetchone()
        return int(row[0] // 60)


async def _start_rate_window(conninfo: str) -> int:
    # Leave at least 30 seconds for the bounded burst; use DB time, not host time.
    async with await psycopg.AsyncConnection.connect(conninfo, autocommit=True) as connection:
        cursor = await connection.execute("SELECT EXTRACT(EPOCH FROM clock_timestamp())")
        row = await cursor.fetchone()
        remaining = 60 - float(row[0] % 60)
    if remaining < 30:
        await asyncio.sleep(remaining + 0.05)
    return await _rate_window(conninfo)


@pytest.mark.parametrize("search", [False, True], ids=["http-message", "search"])
async def test_process_shared_subject_http_rate_limit(clean_postgres_database: str, search: bool) -> None:
    async with _replicas(clean_postgres_database, MESSAGES_PER_MINUTE="3", SEARCHES_PER_MINUTE="3") as replicas:
        for room_id in ("shared", "other"):
            await _create_room(replicas, room_id)

        async def request(index: int, room_id: str = "shared", subject: str = "Alice") -> Any:
            url = f"{replicas.room(index, room_id)}/messages"
            if search:
                return await replicas.client.get(
                    f"{url}/search", headers=_member(subject, room_id), params={"q": "hello"}
                )
            return await replicas.client.post(url, headers=_member(subject, room_id), json={"content": "hello"})

        window = await _start_rate_window(clean_postgres_database)
        success = 200 if search else 201
        assert (await request(0)).status_code == success
        responses = await _contend(
            replicas,
            [request(index % 2, "shared" if index % 4 < 2 else "other") for index in range(8)],
            rate_scope="search" if search else "message",
        )
        assert await _rate_window(clean_postgres_database) == window, "burst crossed a database minute boundary"
        assert sorted(response.status_code for response in responses) == [success] * 2 + [429] * 6
        for response in responses:
            if response.status_code == 429:
                assert response.json()["error"]["code"] == (
                    "search_rate_limit_exceeded" if search else "rate_limit_exceeded"
                )
                assert response.headers["Retry-After"] == "60"
        assert (await request(1, subject="Bob")).status_code == success
        # Scope/transport budgets intentionally differ; exhausting HTTP/search must not consume WS writes.
        async with _member_socket(replicas.ports[1], "Alice", "shared") as (connection, _):
            await _activate(connection)
            await connection.send(json.dumps({"type": "message", "content": "independent websocket budget"}))
            event = await _receive_type(connection, "message.created")
            assert event["message"]["content"] == "independent websocket budget"


async def _send_burst(connection: Any, commands: list[dict[str, Any]], error_code: str) -> int:
    """Ping is a receive-loop barrier, not an acknowledgement of relay delivery."""
    for command in commands:
        await connection.send(json.dumps(command))
    await connection.send(json.dumps({"type": "ping"}))
    errors = 0
    deadline = time.monotonic() + 10
    for _ in range(64):
        event = json.loads(await asyncio.wait_for(connection.recv(), max(0.01, deadline - time.monotonic())))
        if event["type"] == "pong":
            return errors
        if event["type"] == "error":
            assert event["code"] == error_code, event
            errors += 1
        else:
            assert event["type"].startswith(("presence.", "typing.", "message.")), event
    pytest.fail("burst completion pong not reached within bounded frame count")


@pytest.mark.parametrize("typing", [False, True], ids=["websocket-message", "typing"])
async def test_process_shared_subject_websocket_rate_limit(clean_postgres_database: str, typing: bool) -> None:
    async with _replicas(clean_postgres_database, MESSAGES_PER_MINUTE="3", TYPING_EVENTS_PER_MINUTE="3") as replicas:
        for room_id in ("shared", "other"):
            await _create_room(replicas, room_id)
        async with (
            _member_socket(replicas.ports[0], "Alice", "shared") as (first, _),
            _member_socket(replicas.ports[1], "Alice", "other") as (second, _),
        ):
            await asyncio.gather(_activate(first), _activate(second))

            def command(label: str) -> dict[str, Any]:
                return {"type": "typing", "active": True} if typing else {"type": "message", "content": label}

            error_code = "typing_rate_limit_exceeded" if typing else "rate_limit_exceeded"
            window = await _start_rate_window(clean_postgres_database)
            assert await _send_burst(first, [command("seed")], error_code) == 0
            rejected = await _contend(
                replicas,
                [
                    _send_burst(connection, [command(f"burst-{index}-{n}") for n in range(4)], error_code)
                    for index, connection in enumerate((first, second))
                ],
                rate_scope="typing" if typing else "message",
            )
            assert await _rate_window(clean_postgres_database) == window, "burst crossed a database minute boundary"
            assert sum(rejected) == 6
            async with await psycopg.AsyncConnection.connect(clean_postgres_database, autocommit=True) as connection:
                cursor = await connection.execute(
                    "SELECT event_count FROM public.samsarix_rate_buckets WHERE scope = %s",
                    ("typing" if typing else "message",),
                )
                assert await cursor.fetchall() == [(3,)]
            if not typing:
                messages = []
                for index, room_id in enumerate(("shared", "other")):
                    response = await replicas.client.get(
                        f"{replicas.room(index, room_id)}/messages", headers=_member("Alice", room_id)
                    )
                    assert response.status_code == 200
                    messages.extend(response.json()["items"])
                assert len(messages) == 3
                assert sum(message["content"] == "seed" for message in messages) == 1
                assert len({message["content"] for message in messages}) == 3
            async with _member_socket(replicas.ports[1], "Bob", "shared") as (bob, _):
                await _activate(bob)
                assert await _send_burst(bob, [command("independent subject")], error_code) == 0
            # HTTP writes have a distinct transport key even for the exhausted subject.
            response = await replicas.client.post(
                f"{replicas.room(1)}/messages", headers=_member("Alice"), json={"content": "independent HTTP budget"}
            )
            assert response.status_code == 201
