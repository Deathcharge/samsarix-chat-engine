# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Bounded, independently testable accounting for the development load harness."""

from __future__ import annotations

import asyncio
import math
from collections import Counter
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from urllib.parse import unquote, urlsplit


@dataclass(frozen=True)
class Profile:
    scenario: str = "steady"
    duration: int = 180
    rate: int = 20
    rooms: int = 4
    clients_per_room: int = 8
    concurrency: int = 32
    message_bytes: int = 128

    def __post_init__(self) -> None:
        if self.scenario not in {"steady", "count", "age", "retained-gap"}:
            raise ValueError("unknown scenario")
        for name, lower, upper in (
            ("duration", 1, 1800),
            ("rate", 1, 100),
            ("rooms", 2, 16),
            ("clients_per_room", 2, 32),
            ("concurrency", 1, 128),
            ("message_bytes", 32, 4096),
        ):
            value = getattr(self, name)
            if type(value) is not int or not lower <= value <= upper:
                raise ValueError(f"{name} must be an integer in [{lower}, {upper}]")
        if self.duration * self.rate > 20_000:
            raise ValueError("at most 20000 scheduled creates per run")
        if self.duration * self.rate * self.clients_per_room * self.message_bytes > 128_000_000:
            raise ValueError("retained client payload estimate exceeds the 128 MB harness bound")
        if self.rooms * self.clients_per_room > 128 or self.clients_per_room % 2:
            raise ValueError("use an even clients_per_room and at most 128 total clients")
        if self.scenario != "steady" and self.duration < 180:
            raise ValueError("fault scenarios require at least 180 seconds")


def validate_target(conninfo: str) -> None:
    """Never let this destructive development harness target an arbitrary service."""
    if not isinstance(conninfo, str) or len(conninfo) > 4096:
        raise ValueError("invalid scratch database target")
    try:
        parsed = urlsplit(conninfo)
        valid = (
            parsed.scheme in {"postgres", "postgresql"}
            and parsed.hostname in {"127.0.0.1", "::1"}
            and unquote(parsed.path) == "/samsarix_test"
            and not parsed.fragment
            and not parsed.query
            and parsed.port is not None
            and parsed.port > 0
        )
    except ValueError:
        valid = False
    if not valid:
        raise ValueError(
            "requires a numeric-loopback PostgreSQL URL with explicit port, database samsarix_test, no query"
        )


def distribution(values: list[float]) -> dict[str, float | int | None]:
    """Exact nearest-rank percentiles; empty populations are not zero-latency samples."""
    if any(not math.isfinite(value) or value < 0 for value in values):
        raise ValueError("samples must be finite and nonnegative")
    ordered = sorted(values)
    result: dict[str, float | int | None] = {"count": len(values)}
    for name, fraction in (("p50", 0.5), ("p95", 0.95), ("p99", 0.99), ("max", 1)):
        result[name] = round(ordered[max(0, math.ceil(len(ordered) * fraction) - 1)], 6) if ordered else None
    return result


async def arrivals(
    profile: Profile,
    operation: Callable[[int, float], Awaitable[None]],
    counters: Counter[str],
    record_start_delay: Callable[[float], None],
    *,
    clock: Callable[[], float] | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> None:
    """Open arrivals: never lower offered rate because an operation is slow.

    Missed schedule slots and exhausted concurrency are counted, not queued or
    replayed as a catch-up burst. No unbounded task/queue growth is possible.
    """
    clock = clock or asyncio.get_running_loop().time
    start = clock()
    pending: set[asyncio.Task[None]] = set()
    failures: list[BaseException] = []

    async def invoke(index: int, deadline: float) -> None:
        delay = max(0.0, clock() - deadline)
        if delay >= 1 / profile.rate:
            counters["started"] -= 1
            counters["dropped_schedule"] += 1
            return
        record_start_delay(delay * 1000)
        try:
            await operation(index, deadline)
        except Exception as exc:
            failures.append(exc)

    try:
        for index in range(profile.duration * profile.rate):
            deadline = start + index / profile.rate
            await sleep(max(0.0, deadline - clock()))
            counters["offered"] += 1
            if failures:
                raise failures[0]
            pending.difference_update(tuple(task for task in pending if task.done()))
            if clock() - deadline >= 1 / profile.rate:
                counters["dropped_schedule"] += 1
            elif len(pending) >= profile.concurrency:
                counters["dropped_concurrency"] += 1
            else:
                counters["started"] += 1
                task = asyncio.create_task(invoke(index, deadline))
                pending.add(task)
                task.add_done_callback(pending.discard)
            counters["peak_inflight"] = max(counters["peak_inflight"], len(pending))
        await sleep(max(0.0, start + profile.duration - clock()))
        if pending:
            await asyncio.wait_for(asyncio.gather(*pending), timeout=35)
        if failures:
            raise failures[0]
    finally:
        remaining = tuple(pending)
        for task in remaining:
            task.cancel()
        await asyncio.gather(*remaining, return_exceptions=True)


def merge_message(state: dict[str, dict[str, Any]], message: dict[str, Any], *, allow_redaction: bool = False) -> str:
    """Reconcile the harness's one-edit/one-delete messages across history overlap."""
    previous = state.get(message["id"])
    version = lambda item: 2 if item["deleted_at"] else 1 if item["edited_at"] else 0  # noqa: E731
    if previous is None or version(message) > version(previous):
        state[message["id"]] = message
        return "new"
    if version(message) < version(previous):
        return "older"
    if previous != message:
        if (
            allow_redaction
            and (previous["content"] == "" or message["content"] == "")
            and {**previous, "content": ""} == {**message, "content": ""}
        ):
            state[message["id"]] = {**message, "content": ""}
            return "redaction_overlap"
        raise ValueError("same message version has conflicting state")
    return "duplicate"
