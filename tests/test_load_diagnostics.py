# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Diagnostic evidence must remain bounded, content-free and mathematically honest."""

from __future__ import annotations

import math

import pytest

from scripts.load_diagnostics import Latencies, log_signals, pressure_values, process_counters


def test_log_signals_returns_only_known_static_counts() -> None:
    secret = "secret message body and token"
    tail = "\n".join(
        (
            secret,
            "Fencing local sockets after PostgreSQL relay lag exceeded its count limit",
            "Fencing local sockets after PostgreSQL relay lag exceeded its count limit",
            "Fencing local sockets after a retained PostgreSQL event-log gap",
        )
    )
    assert log_signals(tail) == {"count_limit": 2, "age_limit": 0, "retained_gap": 1, "instance_claim": 0}
    assert secret not in repr(log_signals(tail))


def test_process_counters_handles_parentheses_in_the_process_name() -> None:
    # state plus fields 4..22; the name's final ')' is the only safe split point.
    stat = "42 (worker (sample)) S " + " ".join(str(value) for value in range(4, 23))
    io = "rchar: 100\nwchar: 200\nsyscr: 3\nsyscw: 4\nread_bytes: 5\nwrite_bytes: 6\n"
    assert process_counters(stat, io, 100) == {
        "cpu_user_s": 0.14,
        "cpu_system_s": 0.15,
        "rchar": 100,
        "wchar": 200,
        "read_bytes": 5,
        "write_bytes": 6,
    }


@pytest.mark.parametrize(
    "stat,io,ticks",
    [
        ("1 (short) S 1", "", 100),
        ("1 (worker) S " + " ".join("1" for _ in range(20)), "rchar: -1", 100),
        ("1 (worker) S " + " ".join("1" for _ in range(20)), "", 0),
    ],
)
def test_process_counters_rejects_missing_or_invalid_values(stat: str, io: str, ticks: int) -> None:
    with pytest.raises(ValueError, match="invalid process counters"):
        process_counters(stat, io, ticks)


def test_pressure_values_preserves_unavailable_full_state_as_absent() -> None:
    parsed = pressure_values("some avg10=1.25 avg60=2.5 avg300=3 total=42")
    assert parsed == {"some": {"avg10": 1.25, "avg60": 2.5, "avg300": 3.0, "total_us": 42}}


@pytest.mark.parametrize(
    "value",
    [
        "",
        "some avg10=nan avg60=0 avg300=0 total=1",
        "some avg10=101 avg60=0 avg300=0 total=1",
        "some avg10=0 avg60=0 avg300=0 total=-1",
        "unknown avg10=0 avg60=0 avg300=0 total=1",
    ],
)
def test_pressure_values_fails_closed_on_malformed_kernel_data(value: str) -> None:
    with pytest.raises((KeyError, ValueError)):
        pressure_values(value)


def test_latency_windows_keep_completion_bucket_and_empty_semantics() -> None:
    latencies = Latencies()
    assert latencies.window_report() == []
    latencies.record("http", 2, 19.9)
    latencies.record("http", 4, 10)
    latencies.record("delivery", 7, 20)
    assert latencies.total == {"http": [2, 4], "delivery": [7]}
    assert latencies.window_report() == [
        {
            "start_elapsed_s": 10,
            "end_elapsed_s": 20,
            "latency_ms": {"http": {"count": 2, "p50": 2, "p95": 4, "p99": 4, "max": 4}},
        },
        {
            "start_elapsed_s": 20,
            "end_elapsed_s": 30,
            "latency_ms": {"delivery": {"count": 1, "p50": 7, "p95": 7, "p99": 7, "max": 7}},
        },
    ]
    for value in (-1, math.inf, math.nan):
        with pytest.raises(ValueError, match="invalid latency observation"):
            latencies.record("http", value, 1)
