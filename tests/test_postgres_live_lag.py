# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""A healthy lease must not indefinitely protect an obsolete live stream."""

import asyncio
import threading
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

pytest.importorskip("psycopg")

from samsarix_chat_engine import Settings, create_app  # noqa: E402
from samsarix_chat_engine.postgres import (  # noqa: E402
    POSTGRES_EVENT_SEQUENCE_LOCK_ID,
    EventLogLagError,
    InstanceLeaseError,
    PostgresFoundation,
)
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


@pytest.mark.asyncio
async def test_lag_count_is_based_on_committed_rows_not_sequence_distance(clean_postgres_database):
    async with PostgresFoundation(clean_postgres_database) as foundation:
        claim = await foundation.claim_instance("row-count", lease_seconds=30)
        async with foundation.transaction() as connection:
            # Sequence values consumed by aborted transactions are not backlog.
            await connection.execute(
                "SELECT nextval('public.samsarix_realtime_events_sequence_seq') FROM generate_series(1, 20)"
            )
            first = await foundation.append_event(connection, room_id="room", event_type="test", payload={})
            second = await foundation.append_event(connection, room_id="room", event_type="test", payload={})
        events = await foundation.read_events(
            "row-count", generation=claim.generation, max_pending_events=2, max_event_age_seconds=30
        )
        assert [event.sequence for event in events] == [first, second]
        with pytest.raises(EventLogLagError) as error:
            await foundation.read_events("row-count", generation=claim.generation, max_pending_events=1)
        assert error.value.reason == "count"
        assert (
            await foundation.claim_instance("row-count", generation=claim.generation, lease_seconds=30)
        ).last_sequence == 0


@pytest.mark.asyncio
async def test_lag_age_uses_database_time_and_only_unread_events(clean_postgres_database):
    async with PostgresFoundation(clean_postgres_database) as foundation:
        claim = await foundation.claim_instance("event-age", lease_seconds=30)
        async with foundation.transaction() as connection:
            old = await foundation.append_event(connection, room_id="room", event_type="test", payload={})
            await connection.execute(
                "UPDATE public.samsarix_realtime_events SET created_at = clock_timestamp() - interval '61 seconds'"
            )
            fresh = await foundation.append_event(connection, room_id="room", event_type="test", payload={})
        with pytest.raises(EventLogLagError) as error:
            await foundation.read_events("event-age", generation=claim.generation, max_event_age_seconds=30)
        assert error.value.reason == "age"
        await foundation.acknowledge_events("event-age", generation=claim.generation, through_sequence=old)
        assert [
            event.sequence
            for event in await foundation.read_events(
                "event-age", generation=claim.generation, max_event_age_seconds=30
            )
        ] == [fresh]
        await foundation.acknowledge_events("event-age", generation=claim.generation, through_sequence=fresh)
        assert await foundation.read_events("event-age", generation=claim.generation, max_event_age_seconds=1) == []


@pytest.mark.asyncio
async def test_resynchronization_retry_preserves_newer_events_and_rejects_stale_owners(clean_postgres_database):
    async with PostgresFoundation(clean_postgres_database) as foundation:
        claim = await foundation.claim_instance("retry-cursor", lease_seconds=30)
        async with foundation.transaction() as connection:
            head = await foundation.append_event(connection, room_id="room", event_type="test", payload={})
        recovery_id = uuid4()
        recovered = await foundation.resynchronize_claimed_instance(
            "retry-cursor", generation=claim.generation, recovery_generation=recovery_id, lease_seconds=30
        )
        assert recovered.generation == recovery_id
        assert recovered.last_sequence == head
        async with foundation.transaction() as connection:
            newer = await foundation.append_event(connection, room_id="room", event_type="test", payload={})
        # Model a committed recovery whose response was lost. Retrying must not
        # take a second head snapshot and silently skip the newer event.
        assert (
            await foundation.resynchronize_claimed_instance(
                "retry-cursor", generation=claim.generation, recovery_generation=recovery_id, lease_seconds=30
            )
            == recovered
        )
        assert [
            event.sequence
            for event in await foundation.read_events("retry-cursor", generation=recovery_id, max_pending_events=1)
        ] == [newer]
        with pytest.raises(InstanceLeaseError):
            await foundation.heartbeat_claimed_instance("retry-cursor", generation=claim.generation, lease_seconds=30)
        with pytest.raises(InstanceLeaseError):
            await foundation.resynchronize_claimed_instance(
                "retry-cursor", generation=claim.generation, recovery_generation=uuid4(), lease_seconds=30
            )
        await foundation.release_instance("retry-cursor", generation=recovery_id)
        replacement = await foundation.claim_instance("retry-cursor", lease_seconds=30)
        with pytest.raises(InstanceLeaseError):
            await foundation.resynchronize_claimed_instance(
                "retry-cursor", generation=claim.generation, recovery_generation=recovery_id, lease_seconds=30
            )
        assert (
            await foundation.claim_instance("retry-cursor", generation=replacement.generation, lease_seconds=30)
        ).last_sequence == head


@pytest.mark.asyncio
async def test_resynchronization_releases_the_live_cursor_retention_floor(clean_postgres_database):
    async with PostgresFoundation(clean_postgres_database) as foundation:
        claim = await foundation.claim_instance("retention-lag", lease_seconds=30)
        async with foundation.transaction() as connection:
            for _ in range(4):
                await foundation.append_event(connection, room_id="room", event_type="test", payload={})
        assert (await foundation.prune_events(max_events=1, max_age_seconds=60)).pruned_events == 0
        await foundation.resynchronize_claimed_instance(
            "retention-lag", generation=claim.generation, recovery_generation=uuid4(), lease_seconds=30
        )
        assert (await foundation.prune_events(max_events=1, max_age_seconds=60)).pruned_events == 3


def test_lagged_member_reconnects_with_history_while_healthy_replica_keeps_serving(
    clean_postgres_database, monkeypatch
):
    key = "live-lag-operator-key-for-tests"

    def app(name):
        return create_app(
            Settings(
                storage_backend="postgres",
                postgres_url=clean_postgres_database,
                postgres_instance_id=name,
                postgres_relay_poll_seconds=0.01,
                postgres_maintenance_interval_seconds=0.1,
                postgres_relay_max_pending_events=3,
                api_key=key,
                token_signing_secret="live-lag-member-secret-at-least-32-bytes",
            )
        )

    writer_app, lagging_app = app("healthy-writer"), app("lagged-member")
    headers = {"X-API-Key": key}
    read_paused, recovery_entered = threading.Event(), threading.Event()
    read_resume, recovery_resume = asyncio.Event(), asyncio.Event()

    def member(subject):
        token = writer_app.state.token_service.issue(subject, rooms=["room"], permissions=["room:read", "room:write"])
        return {"Authorization": f"Bearer {token}"}

    def ready(websocket):
        assert websocket.receive_json()["type"] == "ready"
        history = websocket.receive_json()
        assert history["type"] == "history"
        websocket.send_json({"type": "ping"})
        assert websocket.receive_json() == {"type": "pong"}
        return history

    with TestClient(writer_app) as writer, TestClient(lagging_app) as lagging:
        assert writer.post("/v1/rooms", headers=headers, json={"id": "room", "name": "Room"}).status_code == 201
        runtime = lagging_app.state.postgres_runtime
        original_generation = runtime.relay._generation
        original_read = runtime.store.foundation.read_events
        original_recover = runtime.store.foundation.resynchronize_claimed_instance

        async def pause_read(*args, **kwargs):
            read_paused.set()
            await read_resume.wait()
            return await original_read(*args, **kwargs)

        async def pause_recovery(*args, **kwargs):
            recovery_entered.set()
            await recovery_resume.wait()
            return await original_recover(*args, **kwargs)

        async def wait_ready():
            async def poll():
                while not await runtime.check_ready():  # noqa: ASYNC110 - observe the real runtime readiness gate
                    await asyncio.sleep(0.01)

            await asyncio.wait_for(poll(), 3)

        try:
            with writer.websocket_connect("/v1/rooms/room/ws", headers=member("Alice")) as healthy:
                ready(healthy)
                with lagging.websocket_connect("/v1/rooms/room/ws", headers=member("Bob")) as stale:
                    ready(stale)
                    assert healthy.receive_json()["type"] == "presence.joined"
                    monkeypatch.setattr(runtime.store.foundation, "read_events", pause_read)
                    monkeypatch.setattr(runtime.store.foundation, "resynchronize_claimed_instance", pause_recovery)
                    assert read_paused.wait(3)

                    def post(content):
                        response = writer.post(
                            "/v1/rooms/room/messages", headers=member("Alice"), json={"content": content}
                        )
                        assert response.status_code == 201
                        assert healthy.receive_json()["message"] == response.json()
                        return response.json()

                    messages = [post(f"Backlog {index}") for index in range(4)]
                    lagging.portal.call(read_resume.set)
                    assert recovery_entered.wait(3), "lag did not reach fenced recovery"
                    with pytest.raises(WebSocketDisconnect) as closed:
                        stale.receive_json()
                    assert closed.value.code == 1012
                    assert lagging.get("/readyz").status_code == 503
                    assert writer.get("/readyz").status_code == 200
                    with lagging.websocket_connect("/v1/rooms/room/ws", headers=member("Bob")) as denied:
                        assert denied.receive_json()["code"] == "storage_unavailable"
                        with pytest.raises(WebSocketDisconnect) as rejected:
                            denied.receive_json()
                        assert rejected.value.code == 1012
                    departed = healthy.receive_json()
                    assert departed == {"type": "presence.left", "username": "Bob", "active_connections": 1}
                    messages.append(post("Healthy while recovery is paused"))
                    lagging.portal.call(recovery_resume.set)
                    lagging.portal.call(wait_ready)
                    assert runtime.relay._generation != original_generation
                with lagging.websocket_connect("/v1/rooms/room/ws", headers=member("Bob")) as restored:
                    history = ready(restored)
                    assert history["messages"] == messages
                    assert healthy.receive_json()["type"] == "presence.joined"
                    marker = post("Live after resynchronization")
                    assert restored.receive_json()["message"] == marker
            assert writer.get("/v1/stats", headers=headers).json() == {"active_connections": 0}
        finally:
            lagging.portal.call(read_resume.set)
            lagging.portal.call(recovery_resume.set)
