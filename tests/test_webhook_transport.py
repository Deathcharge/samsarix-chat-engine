# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Ownership and capacity invariants for the private webhook transport."""

from __future__ import annotations

import asyncio
import gc
import socket
import threading
import weakref
from typing import Any

import pytest

from samsarix_chat_engine.webhook_transport import AttemptBudget, BoundedTransport, PinnedHTTPConnection
from tests.test_webhook_deadlines import _event


@pytest.mark.timeout(10)
async def test_cancellation_keeps_descriptor_owned_until_transport_io_unwinds() -> None:
    transport: BoundedTransport[int] = BoundedTransport()
    entered, release = threading.Event(), threading.Event()
    owned, peer = socket.socketpair()
    descriptor = owned.fileno()
    threads = []

    def blocked(budget: AttemptBudget) -> int:
        threads.append(threading.current_thread())
        budget.bind(owned)
        entered.set()
        assert release.wait(5)
        return 204

    task = asyncio.create_task(transport.run(blocked, timeout=10))
    try:
        await _event(entered)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        peer.settimeout(1)
        assert peer.recv(1) == b"", "cancellation must interrupt the TCP peer"
        assert owned.fileno() == descriptor, "do not recycle a descriptor still used by native I/O"
        release.set()
        await asyncio.to_thread(threads[0].join, 0.05)
        for _ in range(100):
            if owned.fileno() == -1:
                break
            await asyncio.sleep(0.01)
        assert owned.fileno() == -1, "the transport owner must close after its I/O exits"
    finally:
        release.set()
        transport.close()
        await asyncio.gather(task, return_exceptions=True)
        if threads:
            await asyncio.to_thread(threads[0].join, 2)
        owned.close()
        peer.close()


@pytest.mark.parametrize("timeout", [True, False, 0, -1, float("nan"), float("inf"), 30.01])
def test_invalid_budget_rejected(timeout: float) -> None:
    with pytest.raises(ValueError, match="webhook timeout"):
        AttemptBudget(timeout)


@pytest.mark.parametrize("expired", [False, True])
def test_cancel_closes_owned_and_late_bound_sockets(expired: bool) -> None:
    budget = AttemptBudget(1)
    owned, peer = socket.socketpair()
    late, late_peer = socket.socketpair()
    try:
        budget.bind(owned)
        budget.cancel()
        budget.cancel()
        assert owned.fileno() == -1 and peer.recv(1) == b""
        if expired:
            budget = AttemptBudget(1)
            budget.deadline = 0
        with pytest.raises(TimeoutError):
            budget.bind(late)
        assert late.fileno() == -1 and late_peer.recv(1) == b""
        with pytest.raises(TimeoutError):
            budget.remaining()
    finally:
        for connection in (owned, peer, late, late_peer):
            connection.close()


@pytest.mark.parametrize("address,family", [("127.0.0.1", socket.AF_INET), ("::1", socket.AF_INET6)])
def test_numeric_connect_has_no_second_dns_lookup(monkeypatch: pytest.MonkeyPatch, address: str, family: int) -> None:
    operations = []

    class FakeSocket:
        def __init__(self, selected: int, kind: int) -> None:
            assert selected == family and kind == socket.SOCK_STREAM

        def settimeout(self, timeout: float) -> None:
            assert 0 < timeout <= 1

        def connect(self, target: Any) -> None:
            operations.append(target)

        def shutdown(self, how: int) -> None:
            assert how == socket.SHUT_RDWR
            raise OSError("already closed")

        def close(self) -> None:
            operations.append("closed")

    def forbidden(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("numeric connect performed another DNS lookup")

    monkeypatch.setattr(socket, "socket", FakeSocket)
    monkeypatch.setattr(socket, "getaddrinfo", forbidden)
    budget = AttemptBudget(1)
    connection = PinnedHTTPConnection(address, 8443, 1, budget=budget)
    connection.connect()
    budget.cancel()
    assert operations == [(address, 8443), "closed"]


@pytest.mark.timeout(10)
async def test_one_daemon_worker_and_one_slot_until_native_work_returns() -> None:
    transport: BoundedTransport[int] = BoundedTransport()
    entered, release, exited = threading.Event(), threading.Event(), threading.Event()
    threads = []

    def blocked(budget: AttemptBudget) -> int:
        threads.append(threading.current_thread())
        entered.set()
        try:
            assert release.wait(5)
            return 204
        finally:
            exited.set()

    task = asyncio.create_task(transport.run(blocked, timeout=0.15))
    try:
        await _event(entered)
        assert not transport.available
        for _ in range(5):
            with pytest.raises(RuntimeError, match="unavailable"):
                await transport.run(blocked, timeout=1)
        with pytest.raises(TimeoutError):
            await task
        assert not transport.available and len(threads) == 1 and threads[0].daemon
        release.set()
        await _event(exited)
        for _ in range(100):
            if transport.available:
                break
            await asyncio.sleep(0.01)
        assert await transport.run(lambda budget: threading.get_ident(), timeout=1) == threads[0].ident
        transport.close()
        transport.close()
        with pytest.raises(RuntimeError, match="unavailable"):
            await transport.run(blocked, timeout=1)
    finally:
        release.set()
        transport.close()
        await asyncio.gather(task, return_exceptions=True)
        await asyncio.to_thread(threads[0].join, 2)
        assert not threads[0].is_alive()


async def test_start_failure_and_function_error_release_capacity(monkeypatch: pytest.MonkeyPatch) -> None:
    transport: BoundedTransport[int] = BoundedTransport()

    def fail(*args: Any) -> Any:
        raise OSError("fixture failure")

    with monkeypatch.context() as patch:
        patch.setattr(threading.Thread, "start", fail)
        with pytest.raises(OSError, match="fixture failure"):
            await transport.run(lambda budget: 1, timeout=1)
    assert transport.available
    try:
        with pytest.raises(OSError, match="fixture failure"):
            await transport.run(fail, timeout=1)
        assert transport.available
        assert await transport.run(lambda budget: 2, timeout=1) == 2
    finally:
        transport.close()


async def test_idle_worker_does_not_retain_completed_delivery_closure() -> None:
    transport: BoundedTransport[int] = BoundedTransport()

    class Delivery:
        def __call__(self, budget: AttemptBudget) -> int:
            return 1

    delivery = Delivery()
    reference = weakref.ref(delivery)
    try:
        assert await transport.run(delivery, timeout=1) == 1
        del delivery
        await asyncio.sleep(0)
        gc.collect()
        assert reference() is None
    finally:
        transport.close()


def test_resolver_can_finish_after_owning_event_loop_closes() -> None:
    transport: BoundedTransport[int] = BoundedTransport()
    entered, release = threading.Event(), threading.Event()
    threads = []

    def blocked(budget: AttemptBudget) -> int:
        threads.append(threading.current_thread())
        entered.set()
        assert release.wait(5)
        return 204

    async def timed_out() -> None:
        with pytest.raises(TimeoutError):
            await transport.run(blocked, timeout=0.1)

    try:
        asyncio.run(timed_out())
        assert entered.is_set()
        transport.close()  # Cancellation callback targets an already-closed loop.
        release.set()  # Completion callback also targets that closed loop.
        threads[0].join(2)
        assert not threads[0].is_alive()
    finally:
        release.set()
        transport.close()
