# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""The measurement tool must not hide dropped work, stale state, or unsafe targets."""

from __future__ import annotations

import asyncio
import json
from collections import Counter
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from scripts.load_metrics import Profile, arrivals, distribution, merge_message, validate_target

# The checkout-only PostgreSQL harness needs the optional driver; SQLite-only
# contributors must still be able to collect and run the remaining test suite.
pytest.importorskip("psycopg")

from scripts.load_postgres import (  # noqa: E402
    CheckFailed,
    Member,
    World,
    content_for,
    direct_socket_options,
    main,
    validate_environment,
)


@pytest.mark.parametrize(
    "changes",
    [
        {"scenario": "unknown"},
        {"duration": 0},
        {"duration": 1801},
        {"duration": True},
        {"rate": 0},
        {"rate": 101},
        {"rate": float("nan")},
        {"rooms": 1},
        {"rooms": 17},
        {"clients_per_room": 3},
        {"clients_per_room": 64},
        {"rooms": 16, "clients_per_room": 16},
        {"concurrency": 0},
        {"concurrency": 129},
        {"message_bytes": 31},
        {"message_bytes": 4097},
        {"duration": 1800, "rate": 20},
        {"scenario": "count", "duration": 179},
        {"scenario": "reconnect-storm", "duration": 179},
        {"duration": 180, "rate": 100, "message_bytes": 4096},
    ],
)
def test_profile_has_hard_resource_bounds(changes: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        replace(Profile(), **changes)


@pytest.mark.parametrize(
    "url",
    [
        "",
        "dbname=samsarix_test",
        "postgresql://user:secret@db.example:5432/samsarix_test",
        "postgresql://127.0.0.1:5432/production",
        "postgresql://localhost:5432/samsarix_test",
        "postgresql://127.0.0.1/samsarix_test",
        "postgresql://127.0.0.1:0/samsarix_test",
        "postgresql://127.0.0.1:70000/samsarix_test",
        "postgresql://127.0.0.1:5432/samsarix_test?host=db.example",
        "postgresql://127.0.0.1:5432/samsarix_test?host",
        "postgresql://127.0.0.1:5432/samsarix_test#other",
        "postgresql://127.0.0.1:5432/samsarix_test/extra",
        "postgresql://127.0.0.1,db.example:5432/samsarix_test",
        "postgresql://127.0.0.1:5432/samsarix_test?options=-c%20search_path=other",
        "x" * 4097,
    ],
)
def test_target_rejection_does_not_echo_credentials(url: str) -> None:
    with pytest.raises(ValueError) as failure:
        validate_target(url)
    assert "secret" not in str(failure.value)


@pytest.mark.parametrize(
    "url",
    [
        "postgresql://user:secret@127.0.0.1:5432/samsarix_test",
        "postgres://user:p%40ss@[::1]:5432/samsarix_test",
    ],
)
def test_numeric_loopback_scratch_targets(url: str) -> None:
    validate_target(url)


def test_percentiles_are_exact_and_empty_is_not_zero() -> None:
    assert distribution([]) == {"count": 0, "p50": None, "p95": None, "p99": None, "max": None}
    assert distribution([3, 1, 2, 4]) == {"count": 4, "p50": 2, "p95": 4, "p99": 4, "max": 4}
    assert distribution(list(range(1, 101)))["p99"] == 99
    for value in (-1, float("nan"), float("inf")):
        with pytest.raises(ValueError):
            distribution([value])


async def test_open_arrivals_count_concurrency_drops_instead_of_waiting() -> None:
    now = 0.0
    release = asyncio.Event()
    completed = []

    async def sleep(delay: float) -> None:
        nonlocal now
        await asyncio.sleep(0)
        now += delay
        if now >= 1:
            release.set()
        await asyncio.sleep(0)

    async def operation(index: int, _due: float) -> None:
        await release.wait()
        completed.append(index)

    counters: Counter[str] = Counter()
    samples: list[float] = []
    drops = []
    await arrivals(
        Profile(duration=1, rate=10, concurrency=2),
        operation,
        counters,
        samples.append,
        record_drop=lambda *values: drops.append(values),
        clock=lambda: now,
        sleep=sleep,
    )
    assert counters == {"offered": 10, "started": 2, "dropped_concurrency": 8, "peak_inflight": 2}
    assert sorted(completed) == [0, 1] and samples == [0, 0]
    assert [drop[:2] for drop in drops] == [(index, "concurrency") for index in range(2, 10)]


async def test_missed_schedule_slots_are_not_a_catch_up_burst() -> None:
    now = 0.0
    first = True
    scheduled = []

    async def sleep(delay: float) -> None:
        nonlocal now, first
        await asyncio.sleep(0)
        now += 0.35 if first else delay
        first = False
        await asyncio.sleep(0)

    async def operation(index: int, due: float) -> None:
        scheduled.append((index, due))

    counters: Counter[str] = Counter()
    samples: list[float] = []
    drops = []
    await arrivals(
        Profile(duration=1, rate=10),
        operation,
        counters,
        samples.append,
        record_drop=lambda *values: drops.append(values),
        clock=lambda: now,
        sleep=sleep,
    )
    assert counters["offered"] == 10 and counters["dropped_schedule"] == 3
    assert counters["started"] == 7 and counters["dropped_concurrency"] == 0
    assert [index for index, _ in scheduled] == list(range(3, 10))
    assert max(samples) == pytest.approx(50)
    assert [drop[:2] for drop in drops] == [(0, "schedule"), (1, "schedule"), (2, "schedule")]


async def test_arrival_cancellation_awaits_every_owned_operation() -> None:
    entered = asyncio.Event()
    exited = asyncio.Event()

    async def operation(_index: int, _due: float) -> None:
        entered.set()
        try:
            await asyncio.Future()
        finally:
            exited.set()

    task = asyncio.create_task(arrivals(Profile(duration=1, rate=1), operation, Counter(), lambda _value: None))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert exited.is_set()


async def test_arrival_failure_is_not_lost_in_a_detached_task() -> None:
    async def operation(_index: int, _due: float) -> None:
        raise ValueError("deliberate failure")

    with pytest.raises(ValueError, match="deliberate failure"):
        await arrivals(Profile(duration=1, rate=100), operation, Counter(), lambda _value: None)


def message(version: int = 0, *, index: int = 0, room: str = "load-0") -> dict[str, Any]:
    return {
        "id": f"message-{index}",
        "client_message_id": f"load-{index}",
        "room_id": room,
        "sender": f"writer-{index % 4}",
        "content": content_for(index, version, 128),
        "created_at": "2026-01-01T00:00:00Z",
        "edited_at": "2026-01-01T00:00:01Z" if version else None,
        "deleted_at": "2026-01-01T00:00:02Z" if version == 2 else None,
    }


def test_history_overlap_never_resurrects_deleted_content() -> None:
    state: dict[str, dict[str, Any]] = {}
    assert merge_message(state, message(2)) == "new"
    assert merge_message(state, message(0)) == "older"
    assert merge_message(state, message(1)) == "older"
    assert merge_message(state, message(2)) == "duplicate"
    assert state["message-0"]["content"] == ""
    with pytest.raises(ValueError, match="conflicting"):
        merge_message(state, {**message(2), "content": "resurrected"})


@pytest.mark.parametrize(
    "change,label",
    [
        ({"room_id": "elsewhere"}, "cross_room_message"),
        ({"sender": "imposter"}, "incorrect_message_author"),
        ({"content": "wrong"}, "incorrect_message_content"),
        ({"client_message_id": "unknown"}, "unexpected_message_identity"),
    ],
)
def test_live_payload_validation(change: dict[str, Any], label: str) -> None:
    world = World(Profile())
    world.sent[0, 0] = 1
    with pytest.raises(CheckFailed, match=label):
        world.check_message({**message(), **change}, "load-0")


async def test_fenced_socket_cannot_hide_obsolete_lifecycle_frames() -> None:
    world = World(Profile(scenario="count"))
    world.resumed = True
    member = Member(world, 0, "load-0", "reader")
    member.expect_fence = True

    async def frames():
        yield json.dumps({"type": "room.archived", "room": {"id": "load-0"}})

    with pytest.raises(CheckFailed, match="obsolete_frame_after_resume"):
        await member.consume(frames())


def test_final_state_alone_does_not_hide_missing_intermediate_events() -> None:
    world = World(Profile())
    world.acknowledged = {(0, 0), (0, 1), (0, 2)}
    member = Member(world, 1, "load-0", "reader")
    member.state = {"message-0": message(2)}
    member.observed.update({(0, 0): 1, (0, 2): 1})
    assert member.uninterrupted() and not member.complete_stream()
    member.observed[0, 1] = 1
    assert member.complete_stream()
    member.observed[0, 1] = 2
    assert not member.complete_stream()


def test_fault_scope_does_not_excuse_unrelated_healthy_stream_loss() -> None:
    world = World(Profile(scenario="retained-gap"))
    assert not Member(world, 0, "load-1", "reader-a").uninterrupted()
    assert not Member(world, 1, "load-0", "reader-b").uninterrupted()
    assert Member(world, 1, "load-1", "reader-c").uninterrupted()

    storm = World(Profile(scenario="reconnect-storm"))
    assert not Member(storm, 0, "load-1", "reader-d").uninterrupted()
    assert not Member(storm, 1, "load-1", "reader-e").uninterrupted()


def test_count_fault_limit_admits_its_population_before_injected_lag() -> None:
    assert World(Profile(scenario="count")).effective["POSTGRES_RELAY_MAX_PENDING_EVENTS"] == "100"
    assert World(Profile(scenario="reconnect-storm")).effective["POSTGRES_RELAY_MAX_PENDING_EVENTS"] == "100"
    large = World(Profile(scenario="count", clients_per_room=32))
    assert large.effective["POSTGRES_RELAY_MAX_PENDING_EVENTS"] == "256"


@pytest.mark.parametrize(
    ("index", "expected_counter"),
    [(0, "http_control_rejections"), (1, "http_unexpected_rejections")],
)
async def test_request_classifies_lifecycle_transition_race_only_in_affected_room(
    index: int, expected_counter: str
) -> None:
    world = World(Profile(scenario="retained-gap"))
    world.ports = [8000, 8001]

    class Response:
        status_code = 409

        @staticmethod
        def json() -> dict[str, Any]:
            return {"error": {"code": "room_frozen"}}

    class FaultTransitionHttp:
        async def request(self, *_args: Any, **_kwargs: Any) -> Response:
            world.control_active = True
            return Response()

    assert not await world.request(FaultTransitionHttp(), index, 0, None)  # type: ignore[arg-type]
    assert world.counts[expected_counter] == 1
    other = "http_unexpected_rejections" if expected_counter == "http_control_rejections" else "http_control_rejections"
    assert world.counts[other] == 0


def test_deleted_message_may_scrub_unread_prior_event_content_only() -> None:
    world = World(Profile())
    world.sent = {(0, version): 1 for version in range(3)}
    member = Member(world, 0, "load-0", "reader")
    for version in range(3):
        member.apply({**message(version), "content": ""}, live=True)
    assert member.frames["scrubbed_prior_versions"] == 2
    assert member.observed == {(0, 0): 1, (0, 1): 1, (0, 2): 1}
    assert member.state["message-0"] == message(2)
    with pytest.raises(CheckFailed, match="incorrect_message_content"):
        world.check_message({**message(), "content": ""}, "load-0")
    del world.sent[0, 2]
    with pytest.raises(CheckFailed, match="incorrect_message_content"):
        member.apply({**message(), "content": ""}, live=True)


def test_redaction_overlap_does_not_resurrect_content_or_hide_other_conflicts() -> None:
    state: dict[str, dict[str, Any]] = {}
    merge_message(state, message())
    scrubbed = {**message(), "content": ""}
    assert merge_message(state, scrubbed, allow_redaction=True) == "redaction_overlap"
    assert merge_message(state, message(), allow_redaction=True) == "redaction_overlap"
    assert state["message-0"]["content"] == ""
    with pytest.raises(ValueError, match="conflicting"):
        merge_message(state, {**scrubbed, "sender": "imposter"}, allow_redaction=True)


def test_loopback_socket_disables_proxy_without_breaking_older_websockets(monkeypatch: pytest.MonkeyPatch) -> None:
    def modern(uri, *, proxy=True, **kwargs):
        pass

    def legacy(uri, **kwargs):
        pass

    monkeypatch.setattr("scripts.load_postgres.connect", modern)
    assert direct_socket_options() == {"proxy": None}
    monkeypatch.setattr("scripts.load_postgres.connect", legacy)
    assert direct_socket_options() == {}


@pytest.mark.parametrize("name", ["PGHOSTADDR", "PGSERVICE", "PGSERVICEFILE", "PGOPTIONS"])
def test_libpq_defaults_cannot_redirect_the_scratch_target(monkeypatch: pytest.MonkeyPatch, name: str) -> None:
    monkeypatch.setenv(name, "private-routing-value")
    with pytest.raises(CheckFailed, match="unset_libpq_routing_and_session_overrides") as failure:
        validate_environment()
    assert "private-routing-value" not in str(failure.value)


def test_live_event_type_must_match_the_message_version() -> None:
    world = World(Profile())
    world.sent[0, 0] = 1
    member = Member(world, 0, "load-0", "reader")
    with pytest.raises(CheckFailed, match="wrong_event_type"):
        member.apply_event({"type": "message.updated", "message": message()})
    assert not member.observed


@pytest.mark.parametrize(
    "counter", ["dropped_schedule", "dropped_concurrency", "http_unknown_outcomes", "http_unexpected_rejections"]
)
def test_report_does_not_greenwash_load_failure(monkeypatch: pytest.MonkeyPatch, counter: str) -> None:
    monkeypatch.setattr("scripts.load_postgres.os.sched_getaffinity", lambda _pid: {0, 1}, raising=False)
    world = World(Profile())
    world.counts.update(offered=3600, http_POST_accepted=3600, converged_clients=32)
    assert world.report()["accepted"]
    world.counts[counter] = 1
    assert not world.report()["accepted"]


def test_report_rejects_missing_convergence_or_no_work(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("scripts.load_postgres.os.sched_getaffinity", lambda _pid: {0}, raising=False)
    world = World(Profile())
    assert not world.report()["accepted"]
    world.counts.update(offered=3600, http_POST_accepted=3600, converged_clients=31)
    assert not world.report()["accepted"]
    world.counts["converged_clients"] = 32
    world.failures["incorrect_message_content"] = 1
    assert not world.report()["accepted"]


def test_reconnect_storm_report_requires_every_client_to_recover(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("scripts.load_postgres.os.sched_getaffinity", lambda _pid: {0, 1}, raising=False)
    world = World(Profile(scenario="reconnect-storm"))
    world.counts.update(offered=3600, http_POST_accepted=3500, converged_clients=32, reconnected_clients=31)
    assert not world.report()["accepted"]
    world.counts.update(
        reconnect_storm_clients=32,
        archive_closed_clients=16,
        archive_reconnect_refusals=16,
        stale_fenced_clients=16,
    )
    world.counts["reconnected_clients"] = 32
    assert not world.report()["accepted"]
    world.fault["reconnect_storm"] = {"clients": 32}
    assert world.report()["accepted"]


def test_ci_runs_reconnect_storm_with_digest_pinned_postgres() -> None:
    root = Path(__file__).resolve().parents[1]
    ci = (root / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    load = (root / ".github" / "workflows" / "postgres-load.yml").read_text(encoding="utf-8")
    digest = "postgres:18.6-bookworm@sha256:1c59e2c3c818eaa0f0628f695b36e7c9e362d6b219b36a54a32df645cbd7e1af"
    assert "scenario: [steady, count, age, retained-gap, reconnect-storm]" in ci
    assert "options: [steady, count, age, retained-gap, reconnect-storm]" in load
    assert ci.count(digest) == 2
    assert digest in load
    assert "image: postgres:18.6-bookworm\n" not in "\n".join(
        path.read_text(encoding="utf-8") for path in (root / ".github" / "workflows").glob("*.yml")
    )


def test_drop_evidence_is_profile_bounded_and_contains_no_absolute_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("scripts.load_postgres.os.sched_getaffinity", lambda _pid: {0}, raising=False)
    world = World(Profile(duration=1, rate=1))
    world.record_drop(0, "schedule", 51.25)
    report = world.report()
    assert report["arrival_drops"] == [
        {
            "index": 0,
            "phase": "setup",
            "observed_elapsed_s": pytest.approx(0, abs=0.1),
            "lateness_ms": 51.25,
            "reason": "schedule",
        }
    ]
    assert len(report["arrival_drops"]) <= world.profile.duration * world.profile.rate


def test_cli_requires_explicit_reset_authorization_before_access(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    destination = tmp_path / "report.json"
    monkeypatch.setattr("sys.argv", ["load_postgres", "--output", str(destination)])
    with pytest.raises(SystemExit) as failure:
        main()
    assert failure.value.code == 2 and not destination.exists()


def test_cli_never_overwrites_a_previous_report(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    destination = tmp_path / "existing.json"
    destination.write_text("preserve me")
    monkeypatch.setenv("SAMSARIX_TEST_POSTGRES_URL", "postgresql://127.0.0.1:5432/samsarix_test")
    monkeypatch.setattr("scripts.load_postgres.sys.platform", "linux")
    monkeypatch.setattr("sys.argv", ["load_postgres", "--output", str(destination), "--allow-reset-test-database"])
    with pytest.raises(FileExistsError):
        main()
    assert destination.read_text() == "preserve me"
