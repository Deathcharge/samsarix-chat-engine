# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Bounded subprocess lifecycle/diagnostics shared by SQLite and PostgreSQL tests."""

from __future__ import annotations

import asyncio
import socket
import subprocess
import threading
import time
from collections import deque

import httpx2 as httpx
import pytest


def _unused_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


class LoggedServer(subprocess.Popen[str]):
    """Drain child diagnostics continuously into a bounded in-memory tail."""

    def __init__(self, command: list[str], *, env: dict[str, str]) -> None:
        super().__init__(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        self._output: deque[str] = deque(maxlen=32)
        self._output_lock = threading.Lock()
        self.reader = threading.Thread(target=self._drain, name=f"test-server-output-{self.pid}", daemon=True)
        self.reader.start()

    def _drain(self) -> None:
        assert self.stdout is not None
        try:
            while chunk := self.stdout.readline(4096):
                with self._output_lock:
                    self._output.append(chunk)
        finally:
            self.stdout.close()

    def output_tail(self) -> str:
        with self._output_lock:
            return "".join(self._output)[-4_000:]


def _stop_server(process: LoggedServer) -> None:
    try:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=12)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
    finally:
        process.reader.join(timeout=2)


async def _wait_ready(client: httpx.AsyncClient, base_url: str, process: LoggedServer) -> None:
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if process.poll() is not None:
            pytest.fail(f"server exited before readiness:\n{process.output_tail()}")
        try:
            response = await client.get(f"{base_url}/readyz")
            if response.status_code == 200:
                return
        except httpx.HTTPError:
            pass
        await asyncio.sleep(0.05)
    pytest.fail(f"server did not become ready:\n{process.output_tail()}")
