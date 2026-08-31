# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Deterministic PostgreSQL deadline tests; no database is required."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

pytest.importorskip("psycopg")

from samsarix_chat_engine.postgres import PostgresFoundation, PostgresUnavailableError  # noqa: E402


def _foundation() -> tuple[PostgresFoundation, SimpleNamespace]:
    service = PostgresFoundation("postgresql://localhost/samsarix_test", operation_timeout_seconds=0.1)
    connection = SimpleNamespace(pgconn=SimpleNamespace(finish=Mock()), execute=AsyncMock())

    @asynccontextmanager
    async def pool_connection():
        yield connection

    @asynccontextmanager
    async def transaction():
        yield

    service._pool = SimpleNamespace(connection=pool_connection, close=AsyncMock())
    service._opened = True
    connection.transaction = transaction
    return service, connection


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["setup", "query", "commit"])
async def test_deadline_closes_before_cancelling_and_covers_transaction_exit(phase: str) -> None:
    service, connection = _foundation()

    async def stalled(*_args):
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            # Models Psycopg's cancellation hook: a live connection would try more network I/O.
            connection.pgconn.finish.assert_called_once()
            raise

    if phase == "setup":
        connection.execute.side_effect = stalled
    elif phase == "commit":

        @asynccontextmanager
        async def stalled_commit():
            yield
            await stalled()

        connection.transaction = stalled_commit

    owner = asyncio.current_task()
    before = getattr(owner, "cancelling", lambda: 0)()
    started = asyncio.get_running_loop().time()
    with pytest.raises(PostgresUnavailableError, match="operation timed out"):
        async with service.transaction():
            if phase == "query":
                await stalled()
    assert asyncio.get_running_loop().time() - started < 1
    connection.pgconn.finish.assert_called_once()
    assert getattr(owner, "cancelling", lambda: 0)() == before


@pytest.mark.asyncio
async def test_success_removes_deadline_before_connection_can_be_reused() -> None:
    service, connection = _foundation()
    async with service.transaction() as acquired:
        assert acquired is connection
    await asyncio.sleep(0.15)
    connection.pgconn.finish.assert_not_called()


@pytest.mark.asyncio
async def test_external_cancellation_is_not_translated_into_storage_unavailability() -> None:
    service, connection = _foundation()
    entered = asyncio.Event()

    async def operation():
        async with service.transaction():
            entered.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(operation())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.sleep(0.15)
    connection.pgconn.finish.assert_not_called()


@pytest.mark.asyncio
async def test_deadline_still_breaks_stalled_external_cancellation_cleanup() -> None:
    if not hasattr(asyncio.Task, "uncancel"):
        pytest.skip("Python 3.10 cannot distinguish multiple pending cancellation requests")
    service, connection = _foundation()
    entered = asyncio.Event()

    async def operation():
        async with service.transaction():
            entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                # Model cancellation cleanup trying to use the same stalled network.
                await asyncio.Event().wait()

    task = asyncio.create_task(operation())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 1)
    connection.pgconn.finish.assert_called_once()


@pytest.mark.parametrize("timeout", [0, -1, 0.09, 301, float("inf"), float("nan")])
def test_operation_timeout_validation(timeout: float) -> None:
    with pytest.raises(ValueError, match="operation timeout"):
        PostgresFoundation("postgresql://localhost/samsarix_test", operation_timeout_seconds=timeout)


def test_server_deadlines_preserve_other_connection_options() -> None:
    service = PostgresFoundation(
        "dbname=samsarix_test options='-c timezone=UTC'", pool_timeout_seconds=0.5, operation_timeout_seconds=4.5
    )
    assert service._pool.kwargs == {
        "connect_timeout": 2,
        "options": "-c timezone=UTC -c statement_timeout=4500 -c idle_in_transaction_session_timeout=4500",
    }
    with pytest.raises(ValueError, match="invalid PostgreSQL connection information") as error:
        PostgresFoundation("not-a-connection-string private-value")
    assert "private-value" not in str(error.value)


@pytest.mark.asyncio
async def test_schema_initialization_timeout_closes_pool_and_does_not_mark_open() -> None:
    service, connection = _foundation()
    service._opened = False
    service._pool.open = AsyncMock()

    async def stalled(*_args):
        await asyncio.Event().wait()

    connection.execute.side_effect = stalled
    with pytest.raises(PostgresUnavailableError):
        await service.open()
    assert not service._opened
    service._pool.close.assert_awaited_once()
    connection.pgconn.finish.assert_called_once()
