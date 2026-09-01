# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Checkout-only, destructive scratch-database load/recovery acceptance tool.

Run with ``python -m scripts.load_postgres --help`` after installing [postgres,test].
Never use this tool against an application database or alongside other live tests.
"""

from __future__ import annotations

import argparse
import asyncio
import gc
import inspect
import json
import math
import os
import platform
import shutil
import subprocess
import sys
import time
from collections import Counter
from dataclasses import asdict
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlsplit

import httpx2 as httpx
import psycopg
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidStatus

from samsarix_chat_engine import AccessTokenService
from scripts.load_diagnostics import Latencies, database_sample, host_pressure, log_signals, resource_sample
from scripts.load_metrics import Profile, arrivals, distribution, merge_message, validate_target
from tests.conftest import _reset_postgres_test_database
from tests.process_helpers import LoggedServer, _stop_server, _unused_port, _wait_ready
from tests.test_postgres_process_recovery import _NAMES, _paused, _snapshot
from tests.test_postgres_processes import _OPERATOR_KEY, _SIGNING_SECRET, _start_server, _wait_connection_count


class CheckFailed(RuntimeError):
    """Only controlled, credential/content-free labels belong in report errors."""


class RetryConnection(RuntimeError):
    """A transient readiness refusal during the explicitly injected fault."""


def require(condition: bool, label: str) -> None:
    if not condition:
        raise CheckFailed(label)


def room_for(index: int, rooms: int) -> str:
    return f"load-{index % rooms}"


def content_for(index: int, version: int, size: int) -> str:
    if version == 2:
        return ""
    prefix = f"load-{index}-v{version}:"
    return prefix + "x" * (size - len(prefix))


def direct_socket_options() -> dict[str, Any]:
    # websockets 15+ discovers environment proxies; 13/14 have no proxy option.
    return {"proxy": None} if "proxy" in inspect.signature(connect).parameters else {}


def validate_environment() -> None:
    # libpq hostaddr/service defaults can redirect even an explicit numeric host.
    require(
        not any(os.environ.get(name) for name in ("PGHOSTADDR", "PGSERVICE", "PGSERVICEFILE", "PGOPTIONS")),
        "unset_libpq_routing_and_session_overrides",
    )


class World:
    def __init__(self, profile: Profile) -> None:
        self.profile = profile
        self.started = time.monotonic()
        self.phase = "setup"
        self.stopping = False
        self.resumed = False
        self.control_active = False
        self.processes: list[LoggedServer] = []
        self.ports: list[int] = []
        self.members: list[Member] = []
        self.counts: Counter[str] = Counter()
        self.measurements = Latencies()
        self.latencies = self.measurements.total
        self.sent: dict[tuple[int, int], float] = {}
        self.confirmed: dict[int, int] = {}
        self.acknowledged: set[tuple[int, int]] = set()
        self.ids: dict[int, str] = {}
        self.samples: list[dict[str, Any]] = []
        self.arrival_drops: list[dict[str, Any]] = []
        self.timeline: list[dict[str, Any]] = []
        self.failures: Counter[str] = Counter()
        self.tokens: dict[tuple[str, str], str] = {}
        self.database_version: str | None = None
        self.fault: dict[str, Any] = {}
        self.effective = self.settings()

    def settings(self) -> dict[str, str]:
        p = self.profile
        # Count-fault profiles must first admit their own presence burst. Two
        # events per subscriber leaves room for a simultaneous leave/rejoin;
        # the paused writer stream still crosses this bounded test threshold.
        count_fault_limit = max(100, p.rate * 5, p.rooms * p.clients_per_room * 2)
        return {
            "POSTGRES_MAX_POOL_SIZE": "10",
            "POSTGRES_LEASE_SECONDS": "3" if p.scenario == "retained-gap" else "120",
            "POSTGRES_RELAY_POLL": "0.05",
            "POSTGRES_MAINTENANCE_INTERVAL": "0.1",
            "POSTGRES_RELAY_MAX_PENDING_EVENTS": (
                str(count_fault_limit) if p.scenario in {"count", "reconnect-storm"} else "10000"
            ),
            "POSTGRES_RELAY_MAX_EVENT_AGE": "3" if p.scenario == "age" else "3600",
            "POSTGRES_MAX_REALTIME_EVENTS": "100" if p.scenario == "retained-gap" else "100000",
            "MAX_CONNECTIONS": str(p.rooms * p.clients_per_room),
            "MAX_CONNECTIONS_PER_ROOM": str(p.clients_per_room),
            "MAX_STORED_MESSAGES": str(max(1000, p.duration * p.rate + 100)),
            "MAX_STORED_MESSAGES_PER_ROOM": str(max(1000, math.ceil(p.duration * p.rate / p.rooms) + 100)),
            "MESSAGES_PER_MINUTE": str(max(60, math.ceil(p.rate * 60 / p.rooms) * 2)),
            "MAX_MESSAGE_CHARS": str(max(4000, p.message_bytes)),
        }

    def headers(self, subject: str, room: str) -> dict[str, str]:
        key = subject, room
        if key not in self.tokens:
            self.tokens[key] = AccessTokenService(_SIGNING_SECRET).issue(
                subject,
                rooms=[room],
                permissions=["room:read", "room:write"],
                expires_in_seconds=self.profile.duration + 300,
            )
        return {"Authorization": f"Bearer {self.tokens[key]}"}

    def change_phase(self, phase: str) -> None:
        self.phase = phase
        self.timeline.append({"phase": phase, "elapsed_s": round(time.monotonic() - self.started, 6)})

    def measure(self, name: str, value: float) -> None:
        self.measurements.record(name, value, time.monotonic() - self.started)

    def record_drop(self, index: int, reason: str, lateness_ms: float) -> None:
        self.arrival_drops.append(
            {
                "index": index,
                "phase": self.phase,
                "observed_elapsed_s": round(time.monotonic() - self.started, 6),
                "lateness_ms": round(lateness_ms, 6),
                "reason": reason,
            }
        )

    def check_message(self, message: dict[str, Any], room: str, *, live: bool = False) -> tuple[int, int]:
        require(message["room_id"] == room, "cross_room_message")
        identifier = message["client_message_id"]
        require(isinstance(identifier, str) and identifier.startswith("load-"), "unexpected_message_identity")
        index = int(identifier[5:])
        version = 2 if message["deleted_at"] else 1 if message["edited_at"] else 0
        require((index, version) in self.sent, "unsent_message_version")
        require(room_for(index, self.profile.rooms) == room, "cross_room_identity")
        scrubbed = live and version < 2 and message["content"] == "" and (index, 2) in self.sent
        require(
            scrubbed or message["content"] == content_for(index, version, self.profile.message_bytes),
            "incorrect_message_content",
        )
        require(message["sender"] == f"writer-{index % self.profile.rooms}", "incorrect_message_author")
        require(self.ids.get(index, message["id"]) == message["id"], "duplicate_persisted_identity")
        self.ids[index] = message["id"]
        return index, version

    async def request(self, http: httpx.AsyncClient, index: int, version: int, message_id: str | None) -> bool:
        room = room_for(index, self.profile.rooms)
        method = ("POST", "PATCH", "DELETE")[version]
        path = f"http://127.0.0.1:{self.ports[1]}/v1/rooms/{room}/messages"
        body = {"content": content_for(index, version, self.profile.message_bytes)}
        if version == 0:
            body["client_message_id"] = f"load-{index}"
        else:
            path += f"/{message_id}"
        started = time.monotonic()
        self.sent[index, version] = started
        phase = self.phase
        affected_control_room = room == "load-0" or self.profile.scenario == "reconnect-storm"
        control_active_at_start = self.control_active
        self.counts[f"http_{method}_attempts"] += 1
        try:
            response = await http.request(
                method,
                path,
                headers=self.headers(f"writer-{index % self.profile.rooms}", room),
                **({"json": body} if version != 2 else {}),
            )
        except httpx.HTTPError:
            self.counts["http_unknown_outcomes"] += 1
            return False
        finally:
            self.measure(f"http_{method}_all_ms", (time.monotonic() - started) * 1000)
        self.counts[f"http_status_{response.status_code}"] += 1
        if response.status_code != (201, 200, 204)[version]:
            error = response.json().get("error", {}).get("code")
            # A request can enter immediately before the fault controller flips
            # the room and receive its 409 immediately after. Treat a lifecycle
            # rejection as expected when the request overlaps either edge of the
            # intentional control window, but never excuse another room or code.
            overlaps_control = control_active_at_start or self.control_active
            if affected_control_room and overlaps_control and error in {"room_frozen", "room_archived"}:
                self.counts["http_control_rejections"] += 1
            else:
                self.counts["http_unexpected_rejections"] += 1
            return False
        if version != 2:
            self.check_message(response.json(), room)
        self.confirmed[index] = version
        self.acknowledged.add((index, version))
        self.counts[f"http_{method}_accepted"] += 1
        self.measure(f"http_{method}_{phase}_accepted_ms", (time.monotonic() - started) * 1000)
        return True

    async def cycle(self, http: httpx.AsyncClient, index: int, _deadline: float) -> None:
        if not await self.request(http, index, 0, None):
            return
        if index % 5 == 0 and not await self.request(http, index, 1, self.ids[index]):
            return
        if index % 10 == 0 and not await self.request(http, index, 2, self.ids[index]):
            return
        self.counts["cycles_completed"] += 1

    async def history(self, http: httpx.AsyncClient, port: int, subject: str, room: str) -> list[dict[str, Any]]:
        messages = []
        before = None
        seen: set[str] = set()
        for _ in range(202):
            response = await http.get(
                f"http://127.0.0.1:{port}/v1/rooms/{room}/messages",
                headers=self.headers(subject, room),
                params={"limit": 100, **({"before": before} if before else {})},
            )
            if response.status_code == 503:
                raise RetryConnection()
            require(response.status_code == 200, "history_request_failed")
            page = response.json()
            messages.extend(page["items"])
            before = page["next_before"]
            if before is None:
                return messages
            require(before not in seen, "repeated_history_cursor")
            seen.add(before)
        raise CheckFailed("history_page_bound_exceeded")

    async def sample(self, observer: Any) -> None:
        while not self.stopping:
            sample_started = time.monotonic()
            sample: dict[str, Any] = {"elapsed_s": round(sample_started - self.started, 6), "phase": self.phase}
            for index, name in enumerate(_NAMES):
                state = await _snapshot(observer, name)
                sample[name] = {
                    "backlog": state.backlog,
                    "oldest_unread_age_s": state.age,
                    "live": state.live,
                    "cursor": state.cursor,
                    "pruned_through": state.pruned,
                    **await asyncio.to_thread(resource_sample, self.processes[index].pid),
                }
            row = await (await observer.execute("SELECT pg_database_size(current_database())")).fetchone()
            sample["database_bytes"] = int(row[0])
            sample["database_statistics"] = await database_sample(observer, _NAMES)
            sample["driver"] = await asyncio.to_thread(resource_sample, os.getpid())
            sample["host_pressure"] = await asyncio.to_thread(host_pressure)
            sample["counts"] = dict(self.counts)
            sample["sample_duration_ms"] = round((time.monotonic() - sample_started) * 1000, 6)
            self.samples.append(sample)
            await asyncio.sleep(1)

    async def inject_fault(self, http: httpx.AsyncClient, observer: Any) -> None:
        p = self.profile
        if p.scenario == "steady":
            return
        await asyncio.sleep(p.duration / 4)
        reconnect_storm = p.scenario == "reconnect-storm"
        for member in self.members:
            member.allow_reconnect = reconnect_storm or member.replica == 0 or member.room == "load-0"
        self.control_active = True
        async with _paused(self.processes[0], observer):
            self.change_phase("paused")
            initial = await _snapshot(observer, _NAMES[0])
            require(initial.live, "pause_did_not_preserve_live_initial_lease")
            for member in self.members:
                if member.replica == 0:
                    member.expect_fence = True
            control_rooms = [room_for(index, p.rooms) for index in range(p.rooms)] if reconnect_storm else ["load-0"]
            updates = ({"frozen": True}, {"frozen": False}, {"archived": True})
            for update in updates:
                for room in control_rooms:
                    response = await http.patch(
                        f"http://127.0.0.1:{self.ports[1]}/v1/rooms/{room}",
                        headers={"X-API-Key": _OPERATOR_KEY},
                        json=update,
                    )
                    require(response.status_code == 200, "lifecycle_control_failed")
                    self.counts["lifecycle_controls_accepted"] += 1
                await asyncio.sleep(1)
            if reconnect_storm:
                archive_deadline = time.monotonic() + 15
                while not all(
                    member.frames["closed_4409"] >= 1 and member.frames["archive_reconnect_refusals"] >= 1
                    for member in self.members
                    if member.replica == 1
                ):
                    require(time.monotonic() < archive_deadline, "archive_reconnect_storm_timeout")
                    await asyncio.sleep(0.05)
            for room in control_rooms:
                response = await http.patch(
                    f"http://127.0.0.1:{self.ports[1]}/v1/rooms/{room}",
                    headers={"X-API-Key": _OPERATOR_KEY},
                    json={"archived": False},
                )
                require(response.status_code == 200, "lifecycle_control_failed")
                self.counts["lifecycle_controls_accepted"] += 1
            await asyncio.sleep(1)
            deadline = time.monotonic() + 75
            while True:
                current = await _snapshot(observer, _NAMES[0])
                require(
                    current.generation == initial.generation and current.cursor == initial.cursor,
                    "paused_owner_changed",
                )
                if p.scenario == "retained-gap":
                    reached = not current.live and current.pruned > initial.cursor
                else:
                    require(current.live, "live_lag_lease_expired")
                    reached = (
                        current.backlog > int(self.effective["POSTGRES_RELAY_MAX_PENDING_EVENTS"])
                        if p.scenario in {"count", "reconnect-storm"}
                        else current.age > 3
                    )
                if reached:
                    self.fault["barrier"] = {"backlog": current.backlog, "age_s": current.age, "live": current.live}
                    break
                require(time.monotonic() < deadline, "natural_fault_barrier_timeout")
                await asyncio.sleep(0.1)
            self.resumed = True
            self.change_phase("reconnecting")
        resumed_at = time.monotonic()
        deadline = resumed_at + 30
        while True:
            current = await _snapshot(observer, _NAMES[0])
            if (
                current.live
                and current.generation != initial.generation
                and current.cursor > initial.cursor
                and all(member.ready and (member.replica != 0 or member.fences == 1) for member in self.members)
            ):
                break
            require(time.monotonic() < deadline, "reconnect_convergence_timeout")
            await asyncio.sleep(0.05)
        self.fault["resumption_to_reconnected_s"] = time.monotonic() - resumed_at
        if reconnect_storm:
            affected = len(self.members)
            archived_half = sum(member.replica == 1 for member in self.members)
            reconnected = sum(member.connections >= 2 for member in self.members)
            archive_closes = sum(member.frames["closed_4409"] >= 1 for member in self.members if member.replica == 1)
            archive_refusals = sum(
                member.frames["archive_reconnect_refusals"] >= 1 for member in self.members if member.replica == 1
            )
            stale_fences = sum(member.fences == 1 for member in self.members if member.replica == 0)
            self.counts.update(
                reconnect_storm_clients=affected,
                reconnected_clients=reconnected,
                archive_closed_clients=archive_closes,
                archive_reconnect_refusals=archive_refusals,
                stale_fenced_clients=stale_fences,
            )
            require(reconnected == affected, "reconnect_storm_client_missing")
            require(archive_closes == archived_half, "archive_close_missing")
            require(archive_refusals == archived_half, "archive_reconnect_refusal_missing")
            require(stale_fences == affected - archived_half, "stale_fence_missing")
            self.fault["reconnect_storm"] = {
                "clients": affected,
                "reconnected": reconnected,
                "archive_closed": archive_closes,
                "archive_reconnect_refused": archive_refusals,
                "stale_fenced": stale_fences,
            }
        self.control_active = False
        for member in self.members:
            member.allow_reconnect = False
        self.change_phase("recovered")

    async def reconcile(self, http: httpx.AsyncClient) -> None:
        authoritative: dict[str, dict[str, dict[str, Any]]] = {}
        persisted: dict[int, int] = {}
        for index in range(self.profile.rooms):
            room = room_for(index, self.profile.rooms)
            messages = await self.history(http, self.ports[1], f"writer-{index}", room)
            authoritative[room] = {message["id"]: message for message in messages}
            require(len(messages) == len(authoritative[room]), "duplicate_history_identity")
            for message in messages:
                slot, version = self.check_message(message, room)
                require(slot not in persisted, "duplicate_client_message_identity")
                persisted[slot] = version
        require(
            all(persisted.get(slot, -1) >= version for slot, version in self.confirmed.items()),
            "acknowledged_state_lost",
        )
        self.counts["persisted_messages"] = len(persisted)
        self.counts["committed_without_create_ack"] = len(set(persisted) - set(self.confirmed))
        deadline = time.monotonic() + 30
        while not all(member.ready and member.state == authoritative[member.room] for member in self.members):
            require(not self.failures, "member_failed_before_convergence")
            require(time.monotonic() < deadline, "live_state_did_not_match_authoritative_history")
            await asyncio.sleep(0.05)
        self.counts["converged_clients"] = len(self.members)
        require(
            all(member.complete_stream() for member in self.members if member.uninterrupted()),
            "live_event_missing_or_duplicated",
        )
        for member in self.members:
            other_room = room_for(int(member.room[5:]) + 1, self.profile.rooms)
            response = await http.get(
                f"http://127.0.0.1:{self.ports[member.replica]}/v1/rooms/{other_room}/messages",
                headers=self.headers(member.subject, member.room),
            )
            require(response.status_code == 403, "cross_room_authorization_failed")
        self.counts["authorization_denials_verified"] = len(self.members)

    def report(self) -> dict[str, Any]:
        try:
            executable = shutil.which("git")
            if executable is None:
                raise FileNotFoundError()
            revision = subprocess.run(  # noqa: S603 - resolved git, fixed read-only arguments, no shell
                [executable, "rev-parse", "HEAD"],
                capture_output=True,
                text=True,
                check=True,
                timeout=3,
            ).stdout.strip()
        except (OSError, subprocess.SubprocessError):
            revision = None
        drops = self.counts["dropped_schedule"] + self.counts["dropped_concurrency"]
        storm_clients = self.profile.rooms * self.profile.clients_per_room
        storm_half = storm_clients // 2
        storm_accepted = self.profile.scenario != "reconnect-storm" or (
            self.counts["reconnect_storm_clients"] == storm_clients
            and self.counts["reconnected_clients"] == storm_clients
            and self.counts["archive_closed_clients"] == storm_half
            and self.counts["archive_reconnect_refusals"] == storm_half
            and self.counts["stale_fenced_clients"] == storm_half
            and "reconnect_storm" in self.fault
        )
        accepted = (
            not self.failures
            and drops == 0
            and self.counts["http_unexpected_rejections"] == 0
            and self.counts["http_unknown_outcomes"] == 0
            and self.counts["converged_clients"] == self.profile.rooms * self.profile.clients_per_room
            and self.counts["offered"] == self.profile.duration * self.profile.rate
            and self.counts["http_POST_accepted"] > 0
            and storm_accepted
            and (
                self.profile.scenario != "steady"
                or all(
                    member.frames[f"v{version}_{kind}"] == 0
                    for member in self.members
                    for version in range(3)
                    for kind in ("duplicate", "older")
                )
            )
        )
        return {
            "schema_version": 2,
            "accepted": accepted,
            "revision": revision,
            "profile": asdict(self.profile),
            "topology": {
                "replicas": 2,
                "write_replica": 1,
                "subscribers": "evenly split across replicas and rooms",
                "postgres": self.database_version,
                "effective_overrides": self.effective,
            },
            "environment": {
                "os": platform.system(),
                "machine": platform.machine(),
                "python": platform.python_version(),
                "cpu_count": os.cpu_count(),
                "affinity_cpus": len(os.sched_getaffinity(0)),
            },
            "elapsed_including_setup_drain_s": round(time.monotonic() - self.started, 6),
            "counts": dict(self.counts),
            "failures": dict(self.failures),
            "latency_ms": {name: distribution(values) for name, values in sorted(self.latencies.items())},
            "latency_windows": self.measurements.window_report(),
            "arrival_drops": self.arrival_drops,
            "server_log_tail_signals": [log_signals(process.output_tail()) for process in self.processes],
            "achieved_creates_per_scheduled_second": self.counts["http_POST_accepted"] / self.profile.duration,
            "fault": self.fault,
            "timeline": self.timeline,
            "samples": self.samples,
            "sampled_peaks": {
                name: {
                    field: max((sample[name][field] for sample in self.samples), default=None)
                    for field in ("rss_kib", "pss_kib", "backlog", "oldest_unread_age_s")
                }
                for name in _NAMES
            },
            "clients": [
                {
                    "replica": member.replica,
                    "room": member.room,
                    "connections": member.connections,
                    "fences": member.fences,
                    "frames": dict(member.frames),
                    "uninterrupted_stream": member.uninterrupted(),
                    "expected_acknowledged_live_versions": len(member.expected_live()),
                    "missing_acknowledged_live_versions": len(member.expected_live() - member.observed.keys()),
                    "duplicate_live_versions": sum(max(0, count - 1) for count in member.observed.values()),
                }
                for member in self.members
            ],
            "limits": [
                "single-host loopback; shared runner, database, driver and application resources",
                "no network TLS, packet-blackhole, database failover, or production capacity claim",
                "delivery samples exclude history and missing/disconnected deliveries; inspect convergence/counters",
                "memory is sampled child RSS/PSS, not whole-deployment memory or a lifetime peak",
                "20% of scheduled creates attempt one edit; 10% attempt one edit then deletion",
                "arrival rate counts create cycles, not HTTP requests; missed slots are never replayed",
                "latency windows group by completion time, including setup/drain; empty windows are omitted",
                "CPU/I/O/database counters are cumulative; PSI is host-wide, not application-attributed",
                "database waits are instantaneous samples, not wait durations; log signals cover only a bounded tail",
            ],
        }


class Member:
    def __init__(self, world: World, replica: int, room: str, subject: str) -> None:
        self.world, self.replica, self.room, self.subject = world, replica, room, subject
        self.state: dict[str, dict[str, Any]] = {}
        self.frames: Counter[str] = Counter()
        self.observed: Counter[tuple[int, int]] = Counter()
        self.ready = False
        self.connections = 0
        self.fences = 0
        self.allow_reconnect = False
        self.expect_fence = False

    def expected_live(self) -> set[tuple[int, int]]:
        return {key for key in self.world.acknowledged if room_for(key[0], self.world.profile.rooms) == self.room}

    def uninterrupted(self) -> bool:
        return self.world.profile.scenario == "steady" or (
            self.world.profile.scenario != "reconnect-storm" and self.replica == 1 and self.room != "load-0"
        )

    def complete_stream(self) -> bool:
        return self.expected_live().issubset(self.observed) and all(count == 1 for count in self.observed.values())

    def apply_event(self, event: dict[str, Any]) -> None:
        message = event["message"]
        version = 2 if message["deleted_at"] else 1 if message["edited_at"] else 0
        require(event["type"] == ("message.created", "message.updated", "message.deleted")[version], "wrong_event_type")
        self.apply(message, live=True)

    def apply(self, message: dict[str, Any], *, live: bool) -> None:
        index, version = self.world.check_message(message, self.room, live=live)
        outcome = merge_message(self.state, message, allow_redaction=(index, 2) in self.world.sent)
        if live:
            if version < 2 and message["content"] == "":
                self.frames["scrubbed_prior_versions"] += 1
            self.observed[index, version] += 1
            self.frames[f"v{version}_{outcome}"] += 1
            if outcome == "new":
                self.world.measure(
                    f"delivery_replica{self.replica}_{self.world.phase}_ms",
                    (time.monotonic() - self.world.sent[index, version]) * 1000,
                )
        else:
            self.frames["history_items"] += 1

    async def consume(self, socket: Any) -> None:
        async for raw in socket:
            event = json.loads(raw)
            kind = event["type"]
            require(not (self.expect_fence and self.world.resumed and kind != "error"), "obsolete_frame_after_resume")
            if kind.startswith("message."):
                self.apply_event(event)
            elif kind == "error":
                require(self.allow_reconnect, "unexpected_socket_error")
                self.frames["error_frames"] += 1
            else:
                require(
                    kind
                    in {"presence.joined", "presence.left", "room.frozen", "room.unfrozen", "room.archived", "pong"},
                    "unexpected_frame",
                )
                if kind.startswith("room."):
                    require(event["room"]["id"] == self.room, "cross_room_control")
                self.frames["control_frames"] += 1

    async def run(self, http: httpx.AsyncClient) -> None:
        while not self.world.stopping:
            reader = None
            try:
                started = time.monotonic()
                self.frames["connection_attempts"] += 1
                async with connect(
                    f"ws://127.0.0.1:{self.world.ports[self.replica]}/v1/rooms/{self.room}/ws",
                    additional_headers=self.world.headers(self.subject, self.room),
                    open_timeout=10,
                    close_timeout=1,
                    ping_interval=None,
                    max_queue=64,
                    **direct_socket_options(),
                ) as socket:
                    ready = json.loads(await asyncio.wait_for(socket.recv(), 10))
                    if ready["type"] == "error" and self.allow_reconnect:
                        if ready.get("code") == "room_archived":
                            self.frames["archive_reconnect_refusals"] += 1
                        await asyncio.wait_for(socket.wait_closed(), 5)
                        self.frames["rejected_reconnects"] += 1
                    else:
                        require(ready["type"] == "ready" and ready["username"] == self.subject, "incorrect_ready")
                        history = json.loads(await asyncio.wait_for(socket.recv(), 10))
                        require(history["type"] == "history", "missing_initial_history")
                        self.state.clear()
                        for message in history["items"]:
                            self.apply(message, live=False)
                        await socket.send(json.dumps({"type": "ping"}))
                        # Consume all handoff events, not just a filtered pong.
                        while True:
                            event = json.loads(await asyncio.wait_for(socket.recv(), 10))
                            if event["type"] == "pong":
                                break
                            if event["type"].startswith("message."):
                                self.apply_event(event)
                            else:
                                require(
                                    event["type"]
                                    in {"presence.joined", "presence.left", "room.frozen", "room.unfrozen"},
                                    "activation_error",
                                )
                        reader = asyncio.create_task(self.consume(socket))
                        for message in await self.world.history(
                            http, self.world.ports[self.replica], self.subject, self.room
                        ):
                            self.apply(message, live=False)
                        self.connections += 1
                        self.ready = True
                        self.world.measure("connect_history_activation_ms", (time.monotonic() - started) * 1000)
                        await reader
                        # async iteration ends normally for a clean remote close.
                        require(self.world.stopping or self.allow_reconnect, "unexpected_clean_disconnect")
            except ConnectionClosed as exc:
                self.frames[f"closed_{exc.rcvd.code if exc.rcvd else 'no_code'}"] += 1
                if self.expect_fence:
                    if exc.rcvd is None or exc.rcvd.code != 1012:
                        self.world.failures["wrong_stale_socket_close_code"] += 1
                        return
                    self.fences += 1
                    self.expect_fence = False
                else:
                    if not (self.allow_reconnect or self.world.stopping):
                        self.world.failures["unexpected_disconnect"] += 1
                        return
            except (RetryConnection, InvalidStatus) as exc:
                if not self.allow_reconnect or (isinstance(exc, InvalidStatus) and exc.response.status_code != 503):
                    self.world.failures["unexpected_admission_refusal"] += 1
                    return
                self.frames["readiness_rejections"] += 1
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.world.failures[str(exc) if isinstance(exc, CheckFailed) else type(exc).__name__] += 1
                return
            finally:
                self.ready = False
                if reader is not None:
                    reader.cancel()
                    await asyncio.gather(reader, return_exceptions=True)
            if not self.world.stopping:
                await asyncio.sleep(0.2)


async def exercise(world: World, conninfo: str, observer: Any) -> None:
    p = world.profile
    tasks: list[asyncio.Task[Any]] = []
    async with httpx.AsyncClient(timeout=10, trust_env=False, limits=httpx.Limits(max_connections=128)) as http:
        for name in _NAMES:
            port = _unused_port()
            parts = urlsplit(conninfo)
            process = _start_server(
                parts._replace(query=urlencode({"application_name": name})).geturl(),
                name,
                port,
                signing_secret=_SIGNING_SECRET,
                settings=world.effective,
            )
            world.ports.append(port)
            world.processes.append(process)
            await _wait_ready(http, f"http://127.0.0.1:{port}", process)
        require(world.processes[0].pid != world.processes[1].pid, "replicas_not_distinct")
        for index in range(p.rooms):
            room = room_for(index, p.rooms)
            response = await http.post(
                f"http://127.0.0.1:{world.ports[1]}/v1/rooms",
                headers={"X-API-Key": _OPERATOR_KEY},
                json={"id": room, "name": room},
            )
            require(response.status_code == 201, "room_setup_failed")
            for number in range(p.clients_per_room):
                world.members.append(Member(world, number % 2, room, f"reader-{index}-{number}"))
        try:
            tasks = [asyncio.create_task(member.run(http)) for member in world.members]
            deadline = time.monotonic() + 30
            while not all(member.ready for member in world.members):
                require(not world.failures and time.monotonic() < deadline, "initial_clients_not_ready")
                await asyncio.sleep(0.05)
            await _wait_connection_count(http, world.ports[1], len(world.members))
            world.change_phase("steady")
            # Separate connections avoid sharing one observer's cursor/transaction
            # with concurrent sampling and the fault controller.
            async with await psycopg.AsyncConnection.connect(conninfo, autocommit=True, connect_timeout=5) as sampling:
                await sampling.execute("SET statement_timeout = '5s'")
                await sampling.execute("SET default_transaction_read_only = on")
                sampler = asyncio.create_task(world.sample(sampling))
                fault = asyncio.create_task(world.inject_fault(http, observer))
                tasks.extend((sampler, fault))
                try:
                    # A generational collection in this client process can pause
                    # every coroutine long enough to impersonate a missed open-
                    # arrival slot during the intentionally dense recovery burst.
                    # The workload is bounded and CPython reference counting stays
                    # active; restore the caller's GC state before reconciliation.
                    collect_started = time.monotonic()
                    gc.collect()
                    world.fault["driver_gc_collect_before_arrivals_ms"] = (time.monotonic() - collect_started) * 1000
                    driver_gc_enabled = gc.isenabled()
                    if driver_gc_enabled:
                        gc.disable()
                    world.fault["driver_cyclic_gc_was_enabled"] = driver_gc_enabled
                    world.fault["driver_cyclic_gc_disabled_during_arrivals"] = not gc.isenabled()
                    try:
                        await arrivals(
                            p,
                            lambda index, due: world.cycle(http, index, due),
                            world.counts,
                            lambda delay: world.measure("start_delay_ms", delay),
                            record_drop=world.record_drop,
                        )
                    finally:
                        if driver_gc_enabled:
                            gc.enable()
                    await fault
                    require(not sampler.done(), "sampler_stopped_early")
                    require(not world.failures, "subscriber_failed")
                    world.change_phase("drain")
                    await world.reconcile(http)
                    world.stopping = True
                    await sampler
                finally:
                    # Finish observers before closing their connections, and
                    # resume any paused child before process teardown.
                    sampler.cancel()
                    fault.cancel()
                    await asyncio.gather(sampler, fault, return_exceptions=True)
        finally:
            world.stopping = True
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        await _wait_connection_count(http, world.ports[1], 0)
        world.counts["final_connection_leases"] = 0


async def run(profile: Profile, conninfo: str) -> dict[str, Any]:
    validate_target(conninfo)
    validate_environment()
    require(sys.platform == "linux", "requires_linux_procfs_and_sigstop")
    world = World(profile)
    async with await psycopg.AsyncConnection.connect(conninfo, autocommit=True, connect_timeout=5) as observer:
        await observer.execute("SET statement_timeout = '5s'")
        row = await (await observer.execute("SELECT current_database(), current_setting('server_version')")).fetchone()
        require(row is not None and row[0] == "samsarix_test", "wrong_connected_database")
        world.database_version = row[1]
        locked = await (await observer.execute("SELECT pg_try_advisory_lock(1935764833, 1819238756)")).fetchone()
        require(locked is not None and locked[0], "another_load_harness_is_running")
        others = await (
            await observer.execute(
                "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() "
                "AND pid <> pg_backend_pid() AND backend_type = 'client backend'"
            )
        ).fetchone()
        require(others is not None and others[0] == 0, "scratch_database_has_other_clients")
        await asyncio.wait_for(_reset_postgres_test_database(conninfo), 15)
        try:
            await observer.execute("SET default_transaction_read_only = on")
            await asyncio.wait_for(exercise(world, conninfo, observer), profile.duration + 180)
        except BaseException as exc:
            world.failures[str(exc) if isinstance(exc, CheckFailed) else type(exc).__name__] += 1
        finally:
            for process in reversed(world.processes):
                try:
                    await asyncio.to_thread(_stop_server, process)
                except Exception:
                    world.failures["process_cleanup_failed"] += 1
            if all(process.poll() is not None for process in world.processes):
                try:
                    await asyncio.wait_for(_reset_postgres_test_database(conninfo), 15)
                except Exception:
                    world.failures["database_cleanup_failed"] += 1
        return world.report()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario", default="steady", choices=["steady", "count", "age", "retained-gap", "reconnect-storm"]
    )
    parser.add_argument("--duration", type=int, default=180)
    parser.add_argument("--rate", type=int, default=20, help="scheduled create cycles per second, not HTTP requests")
    parser.add_argument("--rooms", type=int, default=4)
    parser.add_argument("--clients-per-room", type=int, default=8)
    parser.add_argument("--concurrency", type=int, default=32)
    parser.add_argument("--message-bytes", type=int, default=128)
    parser.add_argument(
        "--output", type=Path, required=True, help="new JSON report path; existing files are never overwritten"
    )
    parser.add_argument(
        "--allow-reset-test-database", action="store_true", help="authorize removal of samsarix_test application tables"
    )
    args = parser.parse_args()
    if not args.allow_reset_test_database:
        parser.error("--allow-reset-test-database is required; this tool destroys scratch application data")
    if not __debug__:
        parser.error("optimized Python is unsupported: helper assertions must remain enabled")
    try:
        profile = Profile(
            args.scenario,
            args.duration,
            args.rate,
            args.rooms,
            args.clients_per_room,
            args.concurrency,
            args.message_bytes,
        )
        conninfo = os.environ.get("SAMSARIX_TEST_POSTGRES_URL", "")
        validate_target(conninfo)
        validate_environment()
        require(sys.platform == "linux", "requires_linux_procfs_and_sigstop")
    except (ValueError, CheckFailed) as exc:
        parser.error(str(exc))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    # Reserve the artifact before touching PostgreSQL; fail closed on an existing report.
    with args.output.open("x", encoding="utf-8") as output:
        try:
            report = asyncio.run(run(profile, conninfo))
        except BaseException as exc:
            report = {"schema_version": 2, "accepted": False, "failures": {type(exc).__name__: 1}}
        json.dump(report, output, indent=2, allow_nan=False)
        output.write("\n")
    print(
        json.dumps({"accepted": report["accepted"], "counts": report.get("counts", {}), "failures": report["failures"]})
    )
    return 0 if report["accepted"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
