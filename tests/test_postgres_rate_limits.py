# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Live tests for deployment-wide PostgreSQL rate controls."""

from __future__ import annotations

import asyncio

import pytest

pytest.importorskip("psycopg")

from samsarix_chat_engine.postgres import PostgresFoundation  # noqa: E402
from samsarix_chat_engine.postgres_rate_limits import (  # noqa: E402
    PostgresRateLimiter,
    RateBucketCapacityError,
    _digest_key,
)

pytestmark = pytest.mark.postgres


@pytest.mark.asyncio
async def test_two_foundations_atomically_share_one_subject_limit(clean_postgres_database: str) -> None:
    first = PostgresFoundation(clean_postgres_database, max_pool_size=10)
    second = PostgresFoundation(clean_postgres_database, max_pool_size=10)
    await asyncio.gather(first.open(), second.open())
    try:
        first_limiter = PostgresRateLimiter(first, scope="message", limit=5)
        second_limiter = PostgresRateLimiter(second, scope="message", limit=5)
        decisions = await asyncio.gather(
            *((first_limiter if index % 2 == 0 else second_limiter).consume("signed-subject") for index in range(20))
        )

        assert sum(decision.allowed for decision in decisions) == 5
        assert sum(not decision.allowed for decision in decisions) == 15
        assert all(decision.remaining == 0 for decision in decisions if not decision.allowed)
        assert all(1 <= decision.retry_after_seconds <= 60 for decision in decisions if not decision.allowed)
        assert await first_limiter.active_bucket_count() == 1
    finally:
        await asyncio.gather(first.close(), second.close())


@pytest.mark.asyncio
async def test_scopes_and_identity_keys_have_independent_buckets(clean_postgres_database: str) -> None:
    foundation = PostgresFoundation(clean_postgres_database)
    await foundation.open()
    try:
        messages = PostgresRateLimiter(foundation, scope="message", limit=1)
        searches = PostgresRateLimiter(foundation, scope="search", limit=1)

        first = await messages.consume("alice")
        assert first.allowed and first.remaining == 0 and first.retry_after_seconds == 0
        assert not (await messages.consume("alice")).allowed
        assert await messages.allow("bob")
        assert await searches.allow("alice")
        assert await messages.active_bucket_count() == 3

        async with foundation.transaction() as connection:
            cursor = await connection.execute(
                "SELECT scope, key_digest, octet_length(key_digest) FROM public.samsarix_rate_buckets"
            )
            rows = await cursor.fetchall()
        assert {row[0] for row in rows} == {"message", "search"}
        assert all(row[2] == 32 for row in rows)
        assert all(bytes(row[1]) not in {b"alice", b"bob"} for row in rows)
    finally:
        await foundation.close()


@pytest.mark.asyncio
async def test_capacity_fails_closed_then_expired_bucket_is_reclaimed(clean_postgres_database: str) -> None:
    foundation = PostgresFoundation(clean_postgres_database)
    await foundation.open()
    try:
        limiter = PostgresRateLimiter(
            foundation,
            scope="typing",
            limit=5,
            max_buckets=1,
        )
        assert await limiter.allow("first")
        with pytest.raises(RateBucketCapacityError, match="capacity reached"):
            await limiter.allow("second")

        async with foundation.transaction() as connection:
            await connection.execute(
                """
                UPDATE public.samsarix_rate_buckets
                SET window_started_at = clock_timestamp() - interval '2 minutes',
                    expires_at = clock_timestamp() - interval '1 minute'
                """
            )
        assert await limiter.active_bucket_count() == 0
        assert await limiter.prune_expired() == 1
        assert await limiter.allow("second")
    finally:
        await foundation.close()


def test_rate_limiter_rejects_invalid_configuration_and_bounds_raw_keys() -> None:
    foundation = PostgresFoundation("postgresql://unused")
    with pytest.raises(ValueError, match="scope"):
        PostgresRateLimiter(foundation, scope="invalid", limit=1)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="between 1 and 100000"):
        PostgresRateLimiter(foundation, scope="message", limit=0)
    with pytest.raises(ValueError, match="window"):
        PostgresRateLimiter(foundation, scope="message", limit=1, window_seconds=0)
    with pytest.raises(ValueError, match="required"):
        _digest_key("message", "")
    with pytest.raises(ValueError, match="1024 bytes"):
        _digest_key("message", "🌀" * 257)
    assert len(_digest_key("message", "alice")) == 32
    assert _digest_key("message", "alice") != _digest_key("search", "alice")
