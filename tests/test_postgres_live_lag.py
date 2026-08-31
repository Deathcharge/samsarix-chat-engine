# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""A healthy lease must not indefinitely protect an obsolete live stream."""

import asyncio

import pytest

pytest.importorskip("psycopg")

from samsarix_chat_engine.postgres import POSTGRES_EVENT_SEQUENCE_LOCK_ID, PostgresFoundation  # noqa: E402
from samsarix_chat_engine.postgres_realtime import PostgresRealtimeRelay  # noqa: E402
from tests.test_postgres_realtime import RecordingTarget  # noqa: E402

pytestmark = pytest.mark.postgres


@pytest.mark.asyncio
@pytest.mark.parametrize("exceeded", ["count", "age"])
async def test_live_overdue_backlog_is_fenced_before_public_dispatch(clean_postgres_database, exceeded):
    async with PostgresFoundation(clean_postgres_database) as foundation:
        target = RecordingTarget()
        relay = PostgresRealtimeRelay(foundation, target, instance_id="lagging-but-live", poll_interval_seconds=0.01)
        await relay.initialize()
        async with foundation.transaction() as connection:
            await connection.execute("SELECT pg_advisory_xact_lock(%s)", (POSTGRES_EVENT_SEQUENCE_LOCK_ID,))
            await connection.execute(
                """
                INSERT INTO public.samsarix_realtime_events (room_id, event_type, payload, created_at)
                SELECT 'room', 'message.created', '{"type":"message.created","message":{"id":"test"}}'::jsonb,
                       clock_timestamp() - make_interval(secs => %s)
                FROM generate_series(1, %s)
                """,
                (61 if exceeded == "age" else 0, 10_001 if exceeded == "count" else 1),
            )
        await relay.heartbeat()
        assert relay.ready  # The lease is valid; the backlog itself is the failure.
        running = asyncio.create_task(relay.run())
        waits = [asyncio.create_task(target.fenced.wait()), asyncio.create_task(target.broadcasted.wait())]
        try:
            completed, _pending = await asyncio.wait(waits, timeout=3, return_when=asyncio.FIRST_COMPLETED)
            assert completed, "relay neither dispatched nor fenced"
            assert target.fenced.is_set(), "a healthy lease let the stale backlog reach clients"
            assert target.broadcasts == []
        finally:
            relay.stop()
            for waiter in waits:
                waiter.cancel()
            await asyncio.gather(*waits, return_exceptions=True)
            await asyncio.wait_for(running, 3)
