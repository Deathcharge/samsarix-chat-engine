# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""The measurement tool must not hide dropped work, stale state, or unsafe targets."""

from __future__ import annotations

import asyncio
import json
from collections import Counter
from dataclasses import replace
from typing import Any

import pytest

from scripts.load_metrics import Profile, arrivals, distribution, merge_message, validate_target
from scripts.load_postgres import CheckFailed, Member, World, content_for, main


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
    await arrivals(
        Profile(duration=1, rate=10, concurrency=2), operation, counters, samples, clock=lambda: now, sleep=sleep
    )
    assert counters == {"offered": 10, "started": 2, "dropped_concurrency": 8, "peak_inflight": 2}
    assert sorted(completed) == [0, 1] and samples == [0, 0]


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
    await arrivals(Profile(duration=1, rate=10), operation, counters, samples, clock=lambda: now, sleep=sleep)
    assert counters["offered"] == 10 and counters["dropped_schedule"] == 3
    assert counters["started"] == 7 and counters["dropped_concurrency"] == 0
    assert [index for index, _ in scheduled] == list(range(3, 10))
    assert max(samples) == pytest.approx(50)


async def test_arrival_cancellation_awaits_every_owned_operation() -> None:
    entered = asyncio.Event()
    exited = asyncio.Event()

    async def operation(_index: int, _due: float) -> None:
        entered.set()
        try:
            await asyncio.Future()
        finally:
            exited.set()

    task = asyncio.create_task(arrivals(Profile(duration=1, rate=1), operation, Counter(), []))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert exited.is_set()


async def test_arrival_failure_is_not_lost_in_a_detached_task() -> None:
    async def operation(_index: int, _due: float) -> None:
        raise ValueError("deliberate failure")

    with pytest.raises(ValueError, match="deliberate failure"):
        await arrivals(Profile(duration=1, rate=100), operation, Counter(), [])


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
