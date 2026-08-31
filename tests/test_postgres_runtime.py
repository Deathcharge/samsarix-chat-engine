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
    runtime.relay = SimpleNamespace(ready=False)
    runtime.store = SimpleNamespace(check_ready=AsyncMock(return_value=True))
    runtime.connections = SimpleNamespace(try_acquire=AsyncMock())

    assert not await runtime.check_ready()
    runtime.store.check_ready.assert_not_awaited()
    with pytest.raises(PostgresUnavailableError):
        await runtime.acquire_connection(connection_id="socket", room_id="room", username="Alice", subject=None)
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
