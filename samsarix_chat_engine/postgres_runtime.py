# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Lifecycle-safe orchestration for one PostgreSQL-backed app process."""

from __future__ import annotations

import asyncio
import logging

from .postgres_connections import ConnectionCounts, ConnectionLease, PostgresConnectionRegistry
from .postgres_rate_limits import PostgresRateLimiter
from .postgres_realtime import PostgresRealtimeRelay, RealtimeTarget
from .postgres_store import PostgresChatStore
from .postgres_typing import PostgresTypingRegistry, TypingTransition

logger = logging.getLogger(__name__)


class PostgresApplicationRuntime:
    """Own the PostgreSQL store, relay, coordination registries, and maintenance."""

    def __init__(
        self,
        conninfo: str,
        target: RealtimeTarget,
        *,
        instance_id: str,
        max_rooms: int,
        max_stored_messages: int,
        max_stored_messages_per_room: int,
        max_read_states_per_room: int,
        message_retention_days: int | None,
        max_audit_events: int,
        webhook_events: tuple[str, ...],
        max_webhook_deliveries: int,
        max_connections: int,
        max_connections_per_room: int,
        messages_per_minute: int,
        searches_per_minute: int,
        typing_events_per_minute: int,
        typing_timeout_seconds: float,
        min_pool_size: int = 1,
        max_pool_size: int = 10,
        pool_timeout_seconds: float = 10.0,
        lease_seconds: int = 30,
        relay_poll_interval_seconds: float = 0.25,
        maintenance_interval_seconds: float = 1.0,
        max_rate_buckets: int = 100_000,
        max_realtime_events: int = 100_000,
        realtime_event_max_age_seconds: int = 604_800,
    ) -> None:
        if not 0.1 <= maintenance_interval_seconds <= 60:
            raise ValueError("PostgreSQL maintenance interval must be between 0.1 and 60 seconds")
        self.instance_id = instance_id
        self.lease_seconds = lease_seconds
        self.maintenance_interval_seconds = maintenance_interval_seconds
        self.max_realtime_events = max_realtime_events
        self.realtime_event_max_age_seconds = realtime_event_max_age_seconds
        self.store = PostgresChatStore(
            conninfo,
            max_rooms=max_rooms,
            max_stored_messages=max_stored_messages,
            max_stored_messages_per_room=max_stored_messages_per_room,
            max_read_states_per_room=max_read_states_per_room,
            message_retention_days=message_retention_days,
            max_audit_events=max_audit_events,
            webhook_events=webhook_events,
            max_webhook_deliveries=max_webhook_deliveries,
            webhook_worker_id=instance_id,
            min_pool_size=min_pool_size,
            max_pool_size=max_pool_size,
            pool_timeout_seconds=pool_timeout_seconds,
        )
        foundation = self.store.foundation
        self.connections = PostgresConnectionRegistry(
            foundation,
            max_connections=max_connections,
            max_connections_per_room=max_connections_per_room,
            lease_seconds=lease_seconds,
        )
        self.message_limiter = PostgresRateLimiter(
            foundation,
            scope="message",
            limit=messages_per_minute,
            max_buckets=max_rate_buckets,
        )
        self.search_limiter = PostgresRateLimiter(
            foundation,
            scope="search",
            limit=searches_per_minute,
            max_buckets=max_rate_buckets,
        )
        self.typing_limiter = PostgresRateLimiter(
            foundation,
            scope="typing",
            limit=typing_events_per_minute,
            max_buckets=max_rate_buckets,
        )
        self.typing = PostgresTypingRegistry(foundation, timeout_seconds=typing_timeout_seconds)
        self.relay = PostgresRealtimeRelay(
            foundation,
            target,
            instance_id=instance_id,
            lease_seconds=lease_seconds,
            poll_interval_seconds=relay_poll_interval_seconds,
        )
        self._stop = asyncio.Event()
        self._relay_task: asyncio.Task[None] | None = None
        self._maintenance_task: asyncio.Task[None] | None = None

    async def open(self) -> None:
        """Open storage and establish the process lease before accepting traffic."""

        await self.store.initialize()
        try:
            await self.relay.initialize()
        except Exception:
            await self.store.close()
            raise
        self._stop.clear()
        self._relay_task = asyncio.create_task(self.relay.run(), name="samsarix-postgres-realtime")
        self._maintenance_task = asyncio.create_task(
            self._run_maintenance(),
            name="samsarix-postgres-maintenance",
        )

    async def close(self) -> None:
        """Stop background work and close the shared PostgreSQL pool."""

        self.relay.stop()
        self._stop.set()
        tasks = tuple(task for task in (self._relay_task, self._maintenance_task) if task is not None)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._relay_task = None
        self._maintenance_task = None
        await self.relay.release()
        await self.store.close()

    async def check_ready(self) -> bool:
        """Require usable storage and both process coordination loops."""

        if self._relay_task is None or self._relay_task.done():
            return False
        if self._maintenance_task is None or self._maintenance_task.done():
            return False
        return await self.store.check_ready()

    async def acquire_connection(
        self,
        *,
        connection_id: str,
        room_id: str,
        username: str,
        subject: str | None,
    ) -> ConnectionLease | None:
        return await self.connections.try_acquire(
            connection_id=connection_id,
            instance_id=self.instance_id,
            room_id=room_id,
            username=username,
            subject=subject,
        )

    async def renew_connection(self, connection_id: str) -> None:
        await self.connections.renew(connection_id=connection_id, instance_id=self.instance_id)

    async def release_connection(self, connection_id: str) -> bool:
        return await self.connections.release(connection_id=connection_id, instance_id=self.instance_id)

    async def connection_counts(self, room_id: str) -> ConnectionCounts:
        return await self.connections.counts(room_id=room_id)

    async def total_connection_count(self) -> int:
        return await self.connections.total_count()

    async def set_typing(self, connection_id: str, active: bool) -> TypingTransition | None:
        if active:
            return await self.typing.start(connection_id=connection_id, instance_id=self.instance_id)
        return await self.typing.stop(connection_id=connection_id, instance_id=self.instance_id)

    async def run_maintenance_once(self) -> None:
        """Run one bounded, retry-safe coordination cleanup pass."""

        await self.typing.reap_expired(limit=100)
        await self.connections.reap_expired(limit=100)
        await self.message_limiter.prune_expired()
        await self.search_limiter.prune_expired()
        await self.typing_limiter.prune_expired()
        await self.store.foundation.prune_events(
            max_events=self.max_realtime_events,
            max_age_seconds=self.realtime_event_max_age_seconds,
            limit=1_000,
        )

    async def _run_maintenance(self) -> None:
        while not self._stop.is_set():
            try:
                await self.run_maintenance_once()
            except Exception as exc:
                logger.warning("PostgreSQL maintenance pass failed: %s", type(exc).__name__)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.maintenance_interval_seconds)
            except asyncio.TimeoutError:
                pass
