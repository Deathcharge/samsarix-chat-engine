# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Internal deadline-aware sockets and one bounded daemon transport worker."""

from __future__ import annotations

import asyncio
import http.client
import io
import ipaddress
import math
import queue
import socket
import ssl
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from functools import partial
from typing import Any, Generic, TypeVar, cast

T = TypeVar("T")


def _close_socket(connection: socket.socket) -> None:
    # close() alone may not interrupt another thread's socket.makefile() read.
    try:
        connection.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    finally:
        try:
            connection.close()
        except OSError:
            pass


class AttemptBudget:
    """One monotonic attempt deadline and its cancellation-owned socket."""

    def __init__(self, timeout: float) -> None:
        if isinstance(timeout, bool) or not math.isfinite(timeout) or not 0 < timeout <= 30:
            raise ValueError("webhook timeout must be finite and between 0 (exclusive) and 30 seconds")
        self.deadline = time.monotonic() + timeout
        self._lock = threading.Lock()
        self._cancelled = False
        self._socket: socket.socket | None = None

    def remaining(self) -> float:
        with self._lock:
            remaining = self.deadline - time.monotonic()
            if self._cancelled or remaining <= 0:
                raise TimeoutError("webhook attempt deadline exceeded")
            return remaining

    def bind(self, connection: socket.socket) -> None:
        with self._lock:
            expired = self._cancelled or time.monotonic() >= self.deadline
            if not expired:
                # TLS wrapping transfers the existing descriptor to a new socket.
                self._socket = connection
        if expired:
            _close_socket(connection)
            raise TimeoutError("webhook attempt deadline exceeded")

    def cancel(self) -> None:
        with self._lock:
            self._cancelled = True
            connection, self._socket = self._socket, None
        if connection is not None:
            _close_socket(connection)


class _DeadlineReader(io.RawIOBase):
    """Reset each underlying receive to the remaining total budget, not idle time."""

    def __init__(self, connection: socket.socket, budget: AttemptBudget) -> None:
        super().__init__()
        self.connection = connection
        self.budget = budget

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: Any) -> int:
        self.connection.settimeout(self.budget.remaining())
        return self.connection.recv_into(buffer)


class _ResponseSocket:
    def __init__(self, connection: socket.socket, budget: AttemptBudget) -> None:
        self.connection = connection
        self.budget = budget

    def makefile(self, mode: str) -> io.BufferedReader:
        if mode != "rb":
            raise ValueError("webhook response reader requires binary mode")
        return io.BufferedReader(_DeadlineReader(self.connection, self.budget))


class PinnedHTTPConnection(http.client.HTTPConnection):
    def __init__(self, address: str, port: int, timeout: float, *, budget: AttemptBudget) -> None:
        super().__init__(address, port, timeout=timeout)
        self.budget = budget
        # HTTPResponse only needs makefile(); the real socket stays owned here.
        self.response_class = lambda connection, **kwargs: http.client.HTTPResponse(  # type: ignore[assignment]
            cast(socket.socket, _ResponseSocket(connection, budget)), **kwargs
        )

    def connect(self) -> None:
        self.budget.remaining()
        address = ipaddress.ip_address(self.host)
        connection = socket.socket(socket.AF_INET6 if address.version == 6 else socket.AF_INET, socket.SOCK_STREAM)
        self.sock = connection
        self.budget.bind(connection)
        connection.settimeout(self.budget.remaining())
        # The address has already passed egress validation; do not resolve again.
        connection.connect((self.host, self.port))

    def send(self, data: Any) -> None:
        if self.sock is None:
            self.connect()
        if self.sock is None:  # pragma: no cover - connect either sets the socket or raises
            raise ConnectionError("webhook connection did not create a socket")
        self.sock.settimeout(self.budget.remaining())
        super().send(data)


class PinnedHTTPSConnection(PinnedHTTPConnection):
    def __init__(self, hostname: str, address: str, port: int, timeout: float, *, budget: AttemptBudget) -> None:
        super().__init__(address, port, timeout, budget=budget)
        self._server_hostname = hostname
        self._ssl_context = ssl.create_default_context()

    def connect(self) -> None:
        super().connect()
        if self.sock is None:  # pragma: no cover - base connect guarantees this
            raise ConnectionError("HTTPS connection did not create a socket")
        self.budget.remaining()
        self.sock = self._ssl_context.wrap_socket(
            self.sock, server_hostname=self._server_hostname, do_handshake_on_connect=False
        )
        self.budget.bind(self.sock)
        self.sock.settimeout(self.budget.remaining())
        self.sock.do_handshake()


@dataclass
class _Work(Generic[T]):
    budget: AttemptBudget
    function: Callable[[AttemptBudget], T]
    loop: asyncio.AbstractEventLoop
    future: asyncio.Future[T]


class BoundedTransport(Generic[T]):
    """One worker/slot per dispatcher, even if the native resolver never returns.

    A timed-out resolver may linger, but is daemon-owned, cannot send after it
    returns, and prevents more claims/work until it releases this sole slot.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._queue: queue.Queue[_Work[T] | None] = queue.Queue(maxsize=1)
        self._active: _Work[T] | None = None
        self._closed = False
        self._thread: threading.Thread | None = None

    @property
    def available(self) -> bool:
        with self._lock:
            return not self._closed and self._active is None

    async def run(self, function: Callable[[AttemptBudget], T], *, timeout: float) -> T:
        loop = asyncio.get_running_loop()
        work = _Work(AttemptBudget(timeout), function, loop, loop.create_future())
        with self._lock:
            if self._closed or self._active is not None:
                raise RuntimeError("webhook transport is unavailable")
            self._active = work
            if self._thread is None:
                self._thread = threading.Thread(target=self._worker, name="samsarix-webhook-transport", daemon=True)
                try:
                    self._thread.start()
                except BaseException:
                    self._thread = None
                    self._active = None
                    raise
            self._queue.put_nowait(work)
        try:
            return await asyncio.wait_for(work.future, work.budget.remaining())
        finally:
            work.budget.cancel()
            work.future.cancel()

    @staticmethod
    def _post(work: _Work[T], callback: Callable[[], object]) -> None:
        try:
            work.loop.call_soon_threadsafe(callback)
        except RuntimeError:
            # The owning application loop can close while a resolver is still blocked.
            pass

    @staticmethod
    def _finish(work: _Work[T], result: T | None, error: BaseException | None) -> None:
        if work.future.done():
            return
        if error is not None:
            work.future.set_exception(error)
        else:
            work.future.set_result(cast(T, result))

    def _worker(self) -> None:
        while True:
            work = self._queue.get()
            if work is None:
                return
            result: T | None = None
            error: BaseException | None = None
            try:
                work.budget.remaining()
                result = work.function(work.budget)
                work.budget.remaining()
            except BaseException as exc:
                error = exc
            finally:
                work.budget.cancel()
                with self._lock:
                    self._active = None
                    closed = self._closed
            self._post(work, partial(self._finish, work, result, error))
            # Do not retain a delivery body, signing closure, or exception frame
            # while this persistent worker waits for its next item.
            del work, result, error
            if closed:
                return

    def close(self) -> None:
        with self._lock:
            self._closed = True
            work = self._active
            if self._thread is not None and self._queue.empty():
                self._queue.put_nowait(None)
        if work is not None:
            work.budget.cancel()
            self._post(work, work.future.cancel)
