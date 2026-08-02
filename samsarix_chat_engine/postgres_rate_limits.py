# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Atomic PostgreSQL rate buckets for multi-instance request controls."""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Literal, cast

from psycopg import AsyncConnection

from .postgres import PostgresFoundation

POSTGRES_RATE_BUCKET_CAP_LOCK_ID = 7_495_346_927_831_819_048
RateLimitScope = Literal["message", "search", "typing"]
_RATE_LIMIT_SCOPES = frozenset({"message", "search", "typing"})
_MAX_RATE_KEY_BYTES = 1_024


class RateBucketCapacityError(RuntimeError):
    """Raised when active rate-bucket cardinality reaches its configured bound."""


@dataclass(frozen=True, slots=True)
class RateLimitDecision:
    """Result of one atomic rate consumption attempt."""

    allowed: bool
    remaining: int
    retry_after_seconds: int
    reset_at: datetime


class PostgresRateLimiter:
    """Enforce one fixed database-time window across all service replicas."""

    def __init__(
        self,
        foundation: PostgresFoundation,
        *,
        scope: RateLimitScope,
        limit: int,
        window_seconds: int = 60,
        max_buckets: int = 100_000,
    ) -> None:
        if scope not in _RATE_LIMIT_SCOPES:
            raise ValueError("invalid PostgreSQL rate-limit scope")
        if not 1 <= limit <= 100_000:
            raise ValueError("PostgreSQL rate limit must be between 1 and 100000")
        if not 1 <= window_seconds <= 3_600:
            raise ValueError("PostgreSQL rate window must be between 1 and 3600 seconds")
        if not 1 <= max_buckets <= 10_000_000:
            raise ValueError("PostgreSQL rate-bucket capacity must be between 1 and 10000000")
        self.foundation = foundation
        self.scope = scope
        self.limit = limit
        self.window_seconds = window_seconds
        self.max_buckets = max_buckets

    async def allow(self, key: str) -> bool:
        """Consume one allowance and return whether it was accepted."""

        return (await self.consume(key)).allowed

    async def consume(self, key: str) -> RateLimitDecision:
        """Atomically consume one allowance without persisting the raw identity key."""

        digest = _digest_key(self.scope, key)
        async with self.foundation.transaction() as connection:
            now, window_started_at = await self._database_window(connection)

            count = await self._increment_existing(connection, digest, window_started_at)
            if count is not None:
                return self._decision(True, count, now, window_started_at)
            existing = await self._read_existing(connection, digest, window_started_at)
            if existing is not None:
                count, observed_at = existing
                return self._decision(False, count, observed_at, window_started_at)

            # Only new-cardinality work takes the global capacity lock. Hot
            # identities contend on their own bucket row rather than a global
            # request-path mutex.
            await connection.execute("SELECT pg_advisory_xact_lock(%s)", (POSTGRES_RATE_BUCKET_CAP_LOCK_ID,))
            await connection.execute("DELETE FROM public.samsarix_rate_buckets WHERE expires_at <= clock_timestamp()")
            # Lock acquisition may span a boundary under a new-key storm.
            # Recompute from database time so an already-expired bucket is
            # never inserted for the prior window.
            now, window_started_at = await self._database_window(connection)

            count = await self._increment_existing(connection, digest, window_started_at)
            if count is not None:
                return self._decision(True, count, now, window_started_at)
            existing = await self._read_existing(connection, digest, window_started_at)
            if existing is not None:
                count, observed_at = existing
                return self._decision(False, count, observed_at, window_started_at)

            cursor = await connection.execute("SELECT COUNT(*) FROM public.samsarix_rate_buckets")
            row = await cursor.fetchone()
            if row is not None and int(row[0]) >= self.max_buckets:
                raise RateBucketCapacityError("PostgreSQL rate-bucket capacity reached")
            reset_at = window_started_at + timedelta(seconds=self.window_seconds)
            cursor = await connection.execute(
                """
                INSERT INTO public.samsarix_rate_buckets (
                    scope, key_digest, window_started_at, event_count, expires_at
                )
                VALUES (%s, %s, %s, 1, %s)
                RETURNING event_count
                """,
                (self.scope, digest, window_started_at, reset_at),
            )
            inserted = await cursor.fetchone()
            if inserted is None:  # pragma: no cover - PostgreSQL guarantees RETURNING
                raise RuntimeError("PostgreSQL did not return a rate bucket")
            return self._decision(True, int(inserted[0]), now, window_started_at)

    async def active_bucket_count(self) -> int:
        """Return active bucket cardinality for bounded operational visibility."""

        async with self.foundation.transaction() as connection:
            cursor = await connection.execute(
                "SELECT COUNT(*) FROM public.samsarix_rate_buckets WHERE expires_at > clock_timestamp()"
            )
            row = await cursor.fetchone()
        return int(row[0]) if row is not None else 0

    async def prune_expired(self) -> int:
        """Delete expired buckets under the same lock used for capacity decisions."""

        async with self.foundation.transaction() as connection:
            await connection.execute("SELECT pg_advisory_xact_lock(%s)", (POSTGRES_RATE_BUCKET_CAP_LOCK_ID,))
            cursor = await connection.execute(
                "DELETE FROM public.samsarix_rate_buckets WHERE expires_at <= clock_timestamp()"
            )
        return cursor.rowcount

    async def _increment_existing(
        self,
        connection: AsyncConnection[tuple[Any, ...]],
        digest: bytes,
        window_started_at: datetime,
    ) -> int | None:
        cursor = await connection.execute(
            """
            UPDATE public.samsarix_rate_buckets
            SET event_count = event_count + 1,
                updated_at = clock_timestamp()
            WHERE scope = %s
              AND key_digest = %s
              AND window_started_at = %s
              AND event_count < %s
              AND expires_at > clock_timestamp()
            RETURNING event_count
            """,
            (self.scope, digest, window_started_at, self.limit),
        )
        row = await cursor.fetchone()
        return int(row[0]) if row is not None else None

    async def _database_window(
        self,
        connection: AsyncConnection[tuple[Any, ...]],
    ) -> tuple[datetime, datetime]:
        cursor = await connection.execute(
            """
            SELECT
                statement_timestamp(),
                date_bin(
                    make_interval(secs => %s),
                    statement_timestamp(),
                    TIMESTAMPTZ '2000-01-01 00:00:00+00'
                )
            """,
            (self.window_seconds,),
        )
        timing = await cursor.fetchone()
        if timing is None:  # pragma: no cover - PostgreSQL always returns this scalar row
            raise RuntimeError("PostgreSQL did not return rate-bucket time")
        return cast(datetime, timing[0]), cast(datetime, timing[1])

    async def _read_existing(
        self,
        connection: AsyncConnection[tuple[Any, ...]],
        digest: bytes,
        window_started_at: datetime,
    ) -> tuple[int, datetime] | None:
        cursor = await connection.execute(
            """
            SELECT event_count, clock_timestamp()
            FROM public.samsarix_rate_buckets
            WHERE scope = %s
              AND key_digest = %s
              AND window_started_at = %s
              AND expires_at > clock_timestamp()
            """,
            (self.scope, digest, window_started_at),
        )
        row = await cursor.fetchone()
        return (int(row[0]), cast(datetime, row[1])) if row is not None else None

    def _decision(
        self,
        allowed: bool,
        count: int,
        now: datetime,
        window_started_at: datetime,
    ) -> RateLimitDecision:
        reset_at = window_started_at + timedelta(seconds=self.window_seconds)
        retry_after = max(1, math.ceil((reset_at - now).total_seconds())) if not allowed else 0
        return RateLimitDecision(
            allowed=allowed,
            remaining=max(0, self.limit - count),
            retry_after_seconds=retry_after,
            reset_at=reset_at,
        )


def _digest_key(scope: RateLimitScope, key: str) -> bytes:
    if not key:
        raise ValueError("PostgreSQL rate-limit key is required")
    encoded = key.encode("utf-8")
    if len(encoded) > _MAX_RATE_KEY_BYTES:
        raise ValueError("PostgreSQL rate-limit key exceeds 1024 bytes")
    return hashlib.sha256(scope.encode("ascii") + b"\x00" + encoded).digest()
