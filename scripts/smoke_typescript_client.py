# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Run the built TypeScript client against a real loopback server."""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen


def _available_port() -> int:
    with socket.socket() as candidate:
        candidate.bind(("127.0.0.1", 0))
        return int(candidate.getsockname()[1])


def _request(url: str, *, api_key: str | None = None, body: object | None = None) -> object:
    headers = {"Accept": "application/json"}
    if api_key is not None:
        headers["X-API-Key"] = api_key
    encoded = None
    if body is not None:
        headers["Content-Type"] = "application/json"
        encoded = json.dumps(body).encode()
    request = Request(url, data=encoded, headers=headers, method="POST" if body is not None else "GET")  # noqa: S310
    with urlopen(request, timeout=5) as response:  # noqa: S310 - loopback smoke target only
        return json.loads(response.read())


def main() -> int:
    repository = Path(__file__).resolve().parents[1]
    client_directory = repository / "clients" / "typescript"
    node = shutil.which("node")
    if node is None:
        raise RuntimeError("Node.js is required for the TypeScript client smoke")
    port = _available_port()
    base_url = f"http://127.0.0.1:{port}"
    api_key = "typescript-smoke-operator-key"
    signing_secret = "typescript-smoke-signing-secret-32-bytes"  # noqa: S105 - isolated smoke fixture
    with tempfile.TemporaryDirectory(prefix="samsarix-typescript-smoke-") as temporary:
        database = Path(temporary) / "smoke.db"
        environment = os.environ.copy()
        environment.update(
            {
                "SAMSARIX_CHAT_API_KEY": api_key,
                "SAMSARIX_CHAT_TOKEN_SIGNING_SECRET": signing_secret,
                "SAMSARIX_CHAT_DATABASE": str(database),
            }
        )
        server = subprocess.Popen(  # noqa: S603 - fixed interpreter/module and controlled arguments
            [
                sys.executable,
                "-m",
                "samsarix_chat_engine",
                "serve",
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
                "--database",
                str(database),
                "--log-level",
                "warning",
            ],
            env=environment,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            readiness_deadline = time.monotonic() + 30
            while time.monotonic() < readiness_deadline:
                if server.poll() is not None:
                    raise RuntimeError("Samsarix server exited before becoming ready")
                try:
                    if _request(base_url + "/readyz") == {"status": "ready"}:
                        break
                except URLError:
                    pass
                time.sleep(0.1)
            else:
                raise RuntimeError("Samsarix server did not become ready")

            _request(
                base_url + "/v1/rooms",
                api_key=api_key,
                body={"id": "sdk-room", "name": "SDK Room"},
            )
            _request(
                base_url + "/v1/rooms/sdk-room/messages",
                api_key=api_key,
                body={"sender": "Operator", "content": "Unread SDK seed"},
            )
            token = subprocess.run(  # noqa: S603 - fixed interpreter/module and controlled arguments
                [
                    sys.executable,
                    "-m",
                    "samsarix_chat_engine",
                    "token",
                    "issue",
                    "--subject",
                    "sdk-user",
                    "--room",
                    "sdk-room",
                    "--expires-in",
                    "300",
                ],
                env=environment,
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
            subprocess.run(  # noqa: S603 - resolved Node executable and repository-owned script
                [node, "test/integration.mjs", base_url, token],
                cwd=client_directory,
                check=True,
            )
        finally:
            server.terminate()
            try:
                server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait(timeout=5)
            lock_path = database.with_name(f"{database.name}.lock")
            for _ in range(50):
                try:
                    lock_path.unlink(missing_ok=True)
                    break
                except PermissionError:
                    time.sleep(0.1)
            else:
                raise RuntimeError("Samsarix server did not release its database lifecycle lock")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
