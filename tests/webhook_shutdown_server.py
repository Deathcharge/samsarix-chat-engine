# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Test-only real Uvicorn process with an uninterruptible resolver fixture."""

from __future__ import annotations

import argparse
import socket
import threading
from pathlib import Path
from typing import Any

import uvicorn

from samsarix_chat_engine import Settings, create_app
from tests.test_webhooks import API_KEY, SECRET


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--database", type=Path, required=True)
    args = parser.parse_args()
    original = socket.getaddrinfo
    blocked = threading.Event()

    def resolve(host: str, port: int, **kwargs: Any) -> Any:
        if host == "hooks.invalid":
            print("resolver-entered", flush=True)
            blocked.wait(600)
        return original(host, port, **kwargs)

    socket.getaddrinfo = resolve  # type: ignore[assignment]
    app = create_app(
        Settings(
            database_path=args.database,
            api_key=API_KEY,
            webhook_url="https://hooks.invalid/events",
            webhook_signing_secret=SECRET,
            webhook_events=("message.created",),
            webhook_timeout_seconds=30,
        )
    )
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=args.port, log_level="warning"))

    @app.post("/_test/stop")
    async def stop() -> dict[str, bool]:
        # This route exists only in this fixture, never in the distributed engine.
        server.should_exit = True
        return {"stopping": True}

    server.run()
    print("server-exited", flush=True)


if __name__ == "__main__":
    main()
