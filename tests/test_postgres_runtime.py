# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Deterministic runtime failure tests, without a database or child processes."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

pytest.importorskip("psycopg")

from samsarix_chat_engine.postgres import PostgresUnavailableError  # noqa: E402
from samsarix_chat_engine.postgres_realtime import PostgresRealtimeRelay  # noqa: E402
from samsarix_chat_engine.postgres_runtime import PostgresApplicationRuntime  # noqa: E402
from samsarix_chat_engine.websocket_manager import ConnectionManager  # noqa: E402


@pytest.mark.asyncio
async def test_relay_readiness_requires_unexpired_claim_without_recovery() -> None:
    foundation = SimpleNamespace(
        claim_instance=AsyncMock(return_value=SimpleNamespace(generation=uuid4(), last_sequence=0)),
        heartbeat_claimed_instance=AsyncMock(),
    )
    relay = PostgresRealtimeRelay(foundation, Mock(), instance_id="readiness-test", lease_seconds=3)
    assert not relay.ready
    await relay.initialize()
    assert relay.ready
    relay._lease_deadline = asyncio.get_running_loop().time() - 1
    assert not relay.ready
    await relay.heartbeat()
    assert relay.ready
    for flag in ("_fenced", "_fence_required", "_gap_recovery_required"):
        setattr(relay, flag, True)
        assert not relay.ready
        setattr(relay, flag, False)
    relay.stop()
    assert not relay.ready


@pytest.mark.asyncio
async def test_runtime_readiness_and_admission_fail_closed_during_relay_recovery() -> None:
    runtime = object.__new__(PostgresApplicationRuntime)
    runtime.instance_id = "readiness-test"
    runtime._relay_task = Mock(done=Mock(return_value=False))
    runtime._maintenance_task = Mock(done=Mock(return_value=False))
    runtime.relay = SimpleNamespace(ready=False, admission_token=None)
    runtime.store = SimpleNamespace(check_ready=AsyncMock(return_value=True))
    runtime.connections = SimpleNamespace(try_acquire=AsyncMock())

    assert not await runtime.check_ready()
    runtime.store.check_ready.assert_not_awaited()
    with pytest.raises(PostgresUnavailableError):
        await runtime.admit_connection(
            Mock(), Mock(), connection_id="socket", room_id="room", username="Alice", subject=None
        )
    runtime.connections.try_acquire.assert_not_awaited()
    runtime.relay.ready = True
    assert await runtime.check_ready()

    async def lose_lease_during_storage_check() -> bool:
        runtime.relay.ready = False
        return True

    runtime.store.check_ready.side_effect = lose_lease_during_storage_check
    assert not await runtime.check_ready()


@pytest.mark.asyncio
async def test_failed_release_does_not_skip_pool_close_or_raise_from_socket_cleanup(
    caplog: pytest.LogCaptureFixture,
) -> None:
    runtime = object.__new__(PostgresApplicationRuntime)
    runtime.instance_id = "release-test"
    runtime._stop = asyncio.Event()
    runtime._relay_task = None
    runtime._maintenance_task = None
    runtime.relay = SimpleNamespace(
        stop=Mock(), release=AsyncMock(side_effect=PostgresUnavailableError("private database details"))
    )
    runtime.store = SimpleNamespace(close=AsyncMock())
    runtime.connections = SimpleNamespace(
        release=AsyncMock(side_effect=PostgresUnavailableError("private database details"))
    )

    assert not await runtime.release_connection("socket")
    await runtime.close()
    runtime.store.close.assert_awaited_once()
    assert runtime._stop.is_set()
    assert "private database details" not in caplog.text
    assert "deferred to lease expiry" in caplog.text


async def _admission_runtime() -> tuple[PostgresApplicationRuntime, ConnectionManager]:
    manager = ConnectionManager(max_connections=2, max_per_room=2, send_timeout=0.1)
    generation = uuid4()
    foundation = SimpleNamespace(
        claim_instance=AsyncMock(return_value=SimpleNamespace(generation=generation, last_sequence=0))
    )
    runtime = object.__new__(PostgresApplicationRuntime)
    runtime.instance_id = "admission-test"
    runtime.relay = PostgresRealtimeRelay(foundation, manager, instance_id=runtime.instance_id)
    runtime.connections = SimpleNamespace(
        try_acquire=AsyncMock(return_value=SimpleNamespace(instance_generation=generation)),
        release=AsyncMock(return_value=True),
    )
    await runtime.relay.initialize()
    return runtime, manager


@pytest.mark.asyncio
@pytest.mark.parametrize("recover_before_reservation_finishes", [False, True])
async def test_admission_rejects_reservation_that_crossed_a_fence(
    recover_before_reservation_finishes: bool,
) -> None:
    runtime, manager = await _admission_runtime()
    token = runtime.relay.admission_token
    reserved = asyncio.Event()
    finish_reservation = asyncio.Event()
    lease = runtime.connections.try_acquire.return_value

    async def reserve(**_kwargs):
        reserved.set()
        await finish_reservation.wait()
        return lease

    runtime.connections.try_acquire.side_effect = reserve
    websocket = Mock(send_json=AsyncMock(), close=AsyncMock())
    admission = asyncio.create_task(
        runtime.admit_connection(manager, websocket, connection_id="socket", room_id="room", username="A", subject=None)
    )
    try:
        await asyncio.wait_for(reserved.wait(), 1)
        assert await runtime.relay._fence()
        if recover_before_reservation_finishes:
            await runtime.relay.initialize()
            assert runtime.relay.ready
            assert runtime.relay.admission_token != token
        finish_reservation.set()
        with pytest.raises(PostgresUnavailableError):
            await asyncio.wait_for(admission, 1)
        assert manager.active_connections == 0
        runtime.connections.release.assert_awaited_once_with(connection_id="socket", instance_id="admission-test")
        websocket.send_json.assert_not_awaited()
    finally:
        admission.cancel()
        await asyncio.gather(admission, return_exceptions=True)


@pytest.mark.asyncio
async def test_admission_rechecks_fence_inside_registration_lock() -> None:
    runtime, manager = await _admission_runtime()
    websocket = Mock(send_json=AsyncMock(), close=AsyncMock())
    # Make registration wait, then start fencing while it is waiting for the same lock.
    await manager._lock.acquire()
    admission = asyncio.create_task(
        runtime.admit_connection(manager, websocket, connection_id="socket", room_id="room", username="A", subject=None)
    )
    fencing = None
    try:
        await asyncio.sleep(0)
        runtime.connections.try_acquire.assert_awaited_once()
        fencing = asyncio.create_task(runtime.relay._fence())
        await asyncio.sleep(0)
        assert runtime.relay.admission_token is None
        manager._lock.release()
        with pytest.raises(PostgresUnavailableError):
            await asyncio.wait_for(admission, 1)
        assert await asyncio.wait_for(fencing, 1)
        assert manager.active_connections == 0
        runtime.connections.release.assert_awaited_once()
    finally:
        if manager._lock.locked():
            manager._lock.release()
        tasks = [admission] + ([fencing] if fencing is not None else [])
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_admitted_socket_is_included_in_subsequent_fence() -> None:
    runtime, manager = await _admission_runtime()
    websocket = Mock(send_json=AsyncMock(), close=AsyncMock())
    assert await runtime.admit_connection(
        manager, websocket, connection_id="socket", room_id="room", username="A", subject=None
    )
    assert manager.active_connections == 1
    runtime.connections.release.assert_not_awaited()
    assert await runtime.relay._fence()
    assert manager.active_connections == 0
    websocket.close.assert_awaited_once_with(code=1012, reason="Service restarting")


@pytest.mark.asyncio
@pytest.mark.parametrize("database_capacity", [False, True])
async def test_admission_capacity_rejection_releases_only_acquired_reservations(database_capacity: bool) -> None:
    runtime, manager = await _admission_runtime()
    if database_capacity:
        runtime.connections.try_acquire.return_value = None
    else:
        manager.max_connections = 0
    assert not await runtime.admit_connection(
        manager, Mock(), connection_id="socket", room_id="room", username="A", subject=None
    )
    assert manager.active_connections == 0
    assert runtime.connections.release.await_count == (0 if database_capacity else 1)


@pytest.mark.asyncio
async def test_admission_rejects_a_reservation_owned_by_another_database_generation() -> None:
    runtime, manager = await _admission_runtime()
    runtime.connections.try_acquire.return_value = SimpleNamespace(instance_generation=uuid4())
    with pytest.raises(PostgresUnavailableError):
        await runtime.admit_connection(
            manager, Mock(), connection_id="socket", room_id="room", username="A", subject=None
        )
    assert manager.active_connections == 0
    runtime.connections.release.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["unavailable", "cancelled"])
async def test_admission_releases_reservation_when_commit_reply_never_returns(failure: str) -> None:
    runtime, manager = await _admission_runtime()
    reserved = asyncio.Event()
    database_has_reservation = False

    async def reserve(**_kwargs):
        nonlocal database_has_reservation
        database_has_reservation = True
        reserved.set()
        if failure == "unavailable":
            raise PostgresUnavailableError("connection reply lost")
        await asyncio.Event().wait()

    async def release(**_kwargs):
        nonlocal database_has_reservation
        database_has_reservation = False
        return True

    runtime.connections.try_acquire.side_effect = reserve
    runtime.connections.release.side_effect = release
    task = asyncio.create_task(
        runtime.admit_connection(manager, Mock(), connection_id="socket", room_id="room", username="A", subject=None)
    )
    try:
        await asyncio.wait_for(reserved.wait(), 1)
        if failure == "cancelled":
            task.cancel()
        with pytest.raises(asyncio.CancelledError if failure == "cancelled" else PostgresUnavailableError):
            await asyncio.wait_for(task, 1)
        assert not database_has_reservation
        assert manager.active_connections == 0
        runtime.connections.release.assert_awaited_once_with(connection_id="socket", instance_id="admission-test")
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_repeated_cancellation_does_not_abandon_reservation_release() -> None:
    runtime, manager = await _admission_runtime()
    release_started = asyncio.Event()
    finish_release = asyncio.Event()

    async def release(**_kwargs):
        release_started.set()
        await finish_release.wait()
        return True

    runtime.connections.release.side_effect = release
    await manager._lock.acquire()
    task = asyncio.create_task(
        runtime.admit_connection(manager, Mock(), connection_id="socket", room_id="room", username="A", subject=None)
    )
    try:
        await asyncio.sleep(0)
        runtime.connections.try_acquire.assert_awaited_once()
        task.cancel()
        await asyncio.wait_for(release_started.wait(), 1)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        finish_release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 1)
        runtime.connections.release.assert_awaited_once()
        assert manager.active_connections == 0
    finally:
        manager._lock.release()
        finish_release.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_known_duplicate_reservation_error_does_not_release_existing_owner() -> None:
    from samsarix_chat_engine.postgres_connections import ConnectionLeaseError

    runtime, manager = await _admission_runtime()
    runtime.connections.try_acquire.side_effect = ConnectionLeaseError("connection ID is already leased")
    with pytest.raises(ConnectionLeaseError):
        await runtime.admit_connection(
            manager, Mock(), connection_id="existing-socket", room_id="room", username="A", subject=None
        )
    runtime.connections.release.assert_not_awaited()
