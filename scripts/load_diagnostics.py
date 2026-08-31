# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Content-free, bounded diagnostic helpers for the checkout-only load tool."""

from __future__ import annotations

import math
import os
from collections import defaultdict
from pathlib import Path
from typing import Any

from scripts.load_metrics import distribution


def log_signals(tail: str) -> dict[str, int]:
    """Count known static warnings, never return arbitrary server log content.

    The caller provides only LoggedServer's bounded tail; these are observations,
    not complete lifetime counters and not a replacement for database evidence.
    """
    prefix = "Fencing local sockets after "
    messages = {
        "count_limit": prefix + "PostgreSQL relay lag exceeded its count limit",
        "age_limit": prefix + "PostgreSQL relay lag exceeded its age limit",
        "retained_gap": prefix + "a retained PostgreSQL event-log gap",
        "instance_claim": prefix + "the PostgreSQL instance claim became unusable",
    }
    return {name: tail.count(message) for name, message in messages.items()}


def process_counters(stat: str, io: str, ticks_per_second: int) -> dict[str, int | float]:
    # comm can contain spaces and parentheses. Fields after its final ')' begin
    # with state (field 3); utime/stime are fields 14/15, not wall-clock time.
    fields = stat.rpartition(")")[2].split()
    if ticks_per_second <= 0 or len(fields) < 20:
        raise ValueError("invalid process counters")
    user, system = int(fields[11]), int(fields[12])
    counters = {}
    for line in io.splitlines():
        key, _, value = line.partition(":")
        if key in {"rchar", "wchar", "read_bytes", "write_bytes"}:
            counters[key] = int(value)
    if len(counters) != 4 or min(user, system, *counters.values()) < 0:
        raise ValueError("invalid process counters")
    return {"cpu_user_s": user / ticks_per_second, "cpu_system_s": system / ticks_per_second, **counters}


def resource_sample(pid: int) -> dict[str, int | float]:
    root = Path(f"/proc/{pid}")
    result = process_counters((root / "stat").read_text(), (root / "io").read_text(), os.sysconf("SC_CLK_TCK"))
    memory = {}
    for line in (root / "smaps_rollup").read_text().splitlines():
        key, _, value = line.partition(":")
        if key in {"Rss", "Pss"}:
            memory[f"{key.lower()}_kib"] = int(value.split()[0])
    if len(memory) != 2 or min(memory.values()) < 0:
        raise ValueError("invalid process memory counters")
    return {**result, **memory}


def pressure_values(raw: str) -> dict[str, dict[str, float | int]]:
    result = {}
    for line in raw.splitlines():
        name, *values = line.split()
        if name not in {"some", "full"} or name in result:
            raise ValueError("invalid pressure counters")
        fields = dict(value.split("=", 1) for value in values)
        averages = {key: float(fields[key]) for key in ("avg10", "avg60", "avg300")}
        total = int(fields["total"])
        if total < 0 or any(not math.isfinite(value) or not 0 <= value <= 100 for value in averages.values()):
            raise ValueError("invalid pressure counters")
        result[name] = {**averages, "total_us": total}
    if "some" not in result:
        raise ValueError("missing pressure counters")
    return result


def host_pressure() -> dict[str, Any]:
    result = {}
    for resource in ("cpu", "io", "memory"):
        try:
            values = pressure_values(Path(f"/proc/pressure/{resource}").read_text())
            if resource == "cpu":
                values.pop("full", None)  # Undefined at system level, not zero pressure.
            result[resource] = values
        except (FileNotFoundError, PermissionError):
            result[resource] = None  # Unavailable is not an observed zero.
    return result


class Latencies:
    """Exact whole-run and completion-time 10-second windows; same float objects."""

    def __init__(self) -> None:
        self.total: dict[str, list[float]] = defaultdict(list)
        self.windows: dict[int, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))

    def record(self, name: str, value: float, elapsed: float) -> None:
        if any(not math.isfinite(number) or number < 0 for number in (value, elapsed)):
            raise ValueError("invalid latency observation")
        self.total[name].append(value)
        self.windows[int(elapsed // 10) * 10][name].append(value)

    def window_report(self) -> list[dict[str, Any]]:
        return [
            {
                "start_elapsed_s": start,
                "end_elapsed_s": start + 10,
                "latency_ms": {name: distribution(values) for name, values in sorted(groups.items())},
            }
            for start, groups in sorted(self.windows.items())
        ]


async def database_sample(observer: Any, names: tuple[str, ...]) -> dict[str, Any]:
    cursor = await observer.execute(
        "SELECT application_name, state, wait_event_type, wait_event, count(*) "
        "FROM pg_stat_activity WHERE datname = current_database() AND application_name = ANY(%s) "
        "GROUP BY application_name, state, wait_event_type, wait_event "
        "ORDER BY application_name, state, wait_event_type, wait_event",
        (list(names),),
    )
    waits = [
        dict(zip(("application", "state", "wait_type", "wait_event", "connections"), row, strict=True))
        for row in await cursor.fetchall()
    ]
    cursor = await observer.execute(
        "SELECT xact_commit, xact_rollback, blks_read, blks_hit, tup_inserted, tup_updated, tup_deleted, "
        "temp_bytes, deadlocks, stats_reset FROM pg_stat_database WHERE datname = current_database()"
    )
    row = await cursor.fetchone()
    if row is None:
        raise ValueError("missing database statistics")
    values = dict(
        zip(
            (
                "xact_commit",
                "xact_rollback",
                "blks_read",
                "blks_hit",
                "tup_inserted",
                "tup_updated",
                "tup_deleted",
                "temp_bytes",
                "deadlocks",
            ),
            row[:-1],
            strict=True,
        )
    )
    return {"activity": waits, "cumulative": values, "stats_reset": row[-1].isoformat() if row[-1] else None}
