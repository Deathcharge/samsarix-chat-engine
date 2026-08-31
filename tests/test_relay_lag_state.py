# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Local lag fencing and retry-state contracts, without PostgreSQL."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

pytest.importorskip("psycopg")

from samsarix_chat_engine import Settings, create_app  # noqa: E402
from samsarix_chat_engine.config import ConfigurationError  # noqa: E402
from samsarix_chat_engine.postgres import (  # noqa: E402
    EventLogGapError,
    EventLogLagError,
    InstanceLeaseError,
    InstanceRegistration,
    PostgresUnavailableError,
)
from samsarix_chat_engine.postgres_realtime import PostgresRealtimeRelay  # noqa: E402


def _relay():
    foundation = SimpleNamespace(
        claim_instance=AsyncMock(return_value=InstanceRegistration(uuid4(), 0)),
        read_events=AsyncMock(return_value=[]),
        acknowledge_events=AsyncMock(),
        resynchronize_claimed_instance=AsyncMock(),
    )
    target = SimpleNamespace(close_all=AsyncMock(), broadcast=AsyncMock())
    relay = PostgresRealtimeRelay(foundation, target, instance_id="lag-state", poll_interval_seconds=0.01)
    return relay, foundation, target


@pytest.mark.asyncio
async def test_default_lag_bounds_are_forwarded_on_every_batch_read():
    relay, foundation, _target = _relay()
    await relay.initialize()
    assert await relay.process_once() == 0
    foundation.read_events.assert_awaited_once_with(
        "lag-state", limit=100, generation=relay._generation, max_pending_events=10_000, max_event_age_seconds=30
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["count", "age", "gap"])
async def test_recovery_waits_for_successful_fencing_and_reuses_uuid_after_ambiguous_reply(reason):
    relay, foundation, target = _relay()
    await relay.initialize()
    old_token = relay.admission_token
    error = EventLogGapError("gap") if reason == "gap" else EventLogLagError(reason)
    steps = []

    async def read(*_args, **_kwargs):
        if not steps:
            raise error
        assert relay.ready
        relay.stop()
        return []

    async def fence():
        assert not relay.ready
        steps.append("fence")
        if len(steps) == 1:
            raise RuntimeError("temporary close failure")

    async def resynchronize(_instance_id, **kwargs):
        assert not relay.ready
        assert steps[:2] == ["fence", "fence"]
        steps.append("resynchronize")
        if steps.count("resynchronize") == 1:
            raise PostgresUnavailableError("commit reply lost")
        return InstanceRegistration(kwargs["recovery_generation"], 100)

    foundation.read_events.side_effect = read
    foundation.resynchronize_claimed_instance.side_effect = resynchronize
    target.close_all.side_effect = fence
    await asyncio.wait_for(relay.run(), 2)
    assert steps == ["fence", "fence", "resynchronize", "resynchronize"]
    first, second = foundation.resynchronize_claimed_instance.await_args_list
    assert first == second
    assert first.kwargs["generation"] == old_token[0]
    assert first.kwargs["recovery_generation"] == relay._generation != old_token[0]
    assert relay._cursor == 100
    assert not relay._gap_recovery_required and not relay._lag_recovery_required
    assert relay._recovery_generation is None
    target.broadcast.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_failed_fence_never_advances_the_database_cursor():
    relay, foundation, target = _relay()
    await relay.initialize()
    foundation.read_events.side_effect = EventLogLagError("count")

    async def failed_fence():
        relay.stop()
        raise RuntimeError("cannot close")

    target.close_all.side_effect = failed_fence
    await asyncio.wait_for(relay.run(), 1)
    assert not relay.ready
    foundation.resynchronize_claimed_instance.assert_not_awaited()
    foundation.acknowledge_events.assert_not_awaited()


@pytest.mark.asyncio
async def test_recovery_cancellation_preserves_the_retry_uuid_and_unready_state():
    relay, foundation, _target = _relay()
    await relay.initialize()
    foundation.read_events.side_effect = EventLogLagError("age")
    entered = asyncio.Event()

    async def pending_recovery(*_args, **_kwargs):
        entered.set()
        await asyncio.Event().wait()

    foundation.resynchronize_claimed_instance.side_effect = pending_recovery
    running = asyncio.create_task(relay.run())
    try:
        await asyncio.wait_for(entered.wait(), 1)
        attempt_id = relay._recovery_generation
        assert attempt_id is not None and not relay.ready
        running.cancel()
        with pytest.raises(asyncio.CancelledError):
            await running
        assert relay._recovery_generation == attempt_id
        assert relay._lag_recovery_required
        assert not relay.ready
    finally:
        running.cancel()
        await asyncio.gather(running, return_exceptions=True)


@pytest.mark.asyncio
async def test_changed_owner_clears_stale_recovery_attempt_before_reclaiming():
    relay, foundation, target = _relay()
    await relay.initialize()
    replacement = InstanceRegistration(uuid4(), 200)
    foundation.claim_instance.side_effect = [InstanceLeaseError("another owner active"), replacement]
    foundation.resynchronize_claimed_instance.side_effect = InstanceLeaseError("generation replaced")

    async def read(*_args, **_kwargs):
        if relay._generation != replacement.generation:
            raise EventLogLagError("count")
        assert relay.ready
        relay.stop()
        return []

    foundation.read_events.side_effect = read
    await asyncio.wait_for(relay.run(), 2)
    assert relay._generation == replacement.generation
    assert relay._cursor == 200
    assert relay._recovery_generation is None
    foundation.resynchronize_claimed_instance.assert_awaited_once()
    target.close_all.assert_awaited_once()


@pytest.mark.parametrize("key,maximum", [("max_pending_events", 100_000), ("max_event_age_seconds", 3_600)])
@pytest.mark.parametrize("kind", ["zero", "negative", "large", "boolean", "float", "none"])
def test_relay_lag_limits_cannot_be_disabled_or_unbounded(key, maximum, kind):
    value = {"zero": 0, "negative": -1, "large": maximum + 1, "boolean": True, "float": 1.5, "none": None}[kind]
    with pytest.raises(ValueError):
        PostgresRealtimeRelay(SimpleNamespace(), SimpleNamespace(), **{key: value})


@pytest.mark.parametrize(
    "suffix,value", [("POSTGRES_RELAY_MAX_PENDING_EVENTS", "3"), ("POSTGRES_RELAY_MAX_EVENT_AGE", "2")]
)
def test_postgres_lag_environment_settings_cannot_silently_configure_sqlite(monkeypatch, suffix, value):
    monkeypatch.setenv(f"SAMSARIX_CHAT_{suffix}", value)
    with pytest.raises(ConfigurationError, match="PostgreSQL settings require"):
        Settings.from_env()


def test_postgres_lag_environment_settings_reach_the_actual_relay(monkeypatch):
    monkeypatch.setenv("SAMSARIX_CHAT_STORAGE", "postgres")
    monkeypatch.setenv("SAMSARIX_CHAT_POSTGRES_URL", "postgresql://localhost/samsarix_test")
    monkeypatch.setenv("SAMSARIX_CHAT_POSTGRES_INSTANCE_ID", "configured-lag")
    monkeypatch.setenv("SAMSARIX_CHAT_POSTGRES_RELAY_MAX_PENDING_EVENTS", "3")
    monkeypatch.setenv("SAMSARIX_CHAT_POSTGRES_RELAY_MAX_EVENT_AGE", "2")
    settings = Settings.from_env()
    relay = create_app(settings).state.postgres_runtime.relay
    assert relay.max_pending_events == 3
    assert relay.max_event_age_seconds == 2


@pytest.mark.parametrize("key", ["postgres_relay_max_pending_events", "postgres_relay_max_event_age_seconds"])
@pytest.mark.parametrize("value", [0, -1, 100_001, True, 1.5])
def test_direct_lag_settings_are_validated_before_runtime_initialization(key, value):
    with pytest.raises(ConfigurationError):
        Settings(
            storage_backend="postgres",
            postgres_url="postgresql://localhost/samsarix_test",
            postgres_instance_id="bounds",
            **{key: value},
        )
