# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Database-independent lock-order regressions for PostgreSQL rate controls."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from samsarix_chat_engine.postgres_rate_limits import PostgresRateLimiter, RateLimitDecision


@pytest.mark.asyncio
async def test_new_bucket_path_releases_row_locks_before_capacity_lock() -> None:
    first_connection = AsyncMock()
    second_connection = AsyncMock()
    connections = iter((first_connection, second_connection))
    transaction_order: list[Any] = []

    @asynccontextmanager
    async def transaction() -> Any:
        connection = next(connections)
        transaction_order.append(connection)
        yield connection

    foundation = SimpleNamespace(transaction=transaction)
    limiter = PostgresRateLimiter(foundation, scope="message", limit=5)  # type: ignore[arg-type]
    now = datetime(2026, 9, 1, tzinfo=UTC)
    window_started_at = now.replace(second=0)
    existing = RateLimitDecision(
        allowed=True,
        remaining=3,
        retry_after_seconds=0,
        reset_at=window_started_at + timedelta(minutes=1),
    )
    limiter._database_window = AsyncMock(  # type: ignore[method-assign]
        side_effect=((now, window_started_at), (now, window_started_at))
    )
    limiter._consume_existing = AsyncMock(side_effect=(None, existing))  # type: ignore[method-assign]

    assert await limiter.consume("new-subject") == existing
    assert transaction_order == [first_connection, second_connection]
    first_connection.execute.assert_not_awaited()
    statements = [call.args[0] for call in second_connection.execute.await_args_list]
    assert "pg_advisory_xact_lock" in statements[0]
    assert "DELETE FROM public.samsarix_rate_buckets" in statements[1]
