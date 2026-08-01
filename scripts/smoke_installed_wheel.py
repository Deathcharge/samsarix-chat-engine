"""Exercise an installed Samsarix Chat Engine over real HTTP and WebSockets."""

from __future__ import annotations

import asyncio
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen

import uvicorn
from websockets.asyncio.client import connect

from samsarix_chat_engine import Settings, create_app


def _available_port() -> int:
    with socket.socket() as candidate:
        candidate.bind(("127.0.0.1", 0))
        return int(candidate.getsockname()[1])


def _request(
    url: str,
    *,
    method: str = "GET",
    credential: tuple[str, str] | None = None,
    headers: dict[str, str] | None = None,
    body: Any = None,
) -> Any:
    request_headers = {"Content-Type": "application/json", **(headers or {})}
    if credential is not None:
        request_headers[credential[0]] = credential[1]
    encoded = json.dumps(body).encode() if body is not None else None
    request = Request(url, data=encoded, headers=request_headers, method=method)  # noqa: S310 - loopback URL only
    with urlopen(request, timeout=5) as response:  # noqa: S310 - loopback URL only
        content = response.read()
        if not content:
            return None
        if response.headers.get_content_type() == "application/json":
            return json.loads(content)
        return content.decode()


async def _websocket_round_trip(base_url: str, token: str) -> str:
    websocket_url = base_url.replace("http://", "ws://") + "/v1/rooms/wheel-room/ws"
    async with connect(websocket_url, max_size=16_384) as websocket:
        required = json.loads(await websocket.recv())
        if required["type"] != "auth.required":
            raise RuntimeError("WebSocket did not request authentication")
        await websocket.send(json.dumps({"type": "auth", "token": token}))
        ready = json.loads(await websocket.recv())
        history = json.loads(await websocket.recv())
        if ready["username"] != "wheel-user" or history["items"][0]["sender"] != "wheel-user":
            raise RuntimeError("WebSocket identity or recovery mismatch")
        await websocket.send(json.dumps({"type": "message", "content": "installed wheel WebSocket"}))
        created = json.loads(await websocket.recv())
        if created["message"]["sender"] != "wheel-user":
            raise RuntimeError("WebSocket sender was not derived from the token")
        return created["message"]["sender"]


def main() -> int:
    port = _available_port()
    base_url = f"http://127.0.0.1:{port}"
    operator_key = "installed-wheel-operator-key"
    signing_secret = "installed-wheel-signing-secret-32-bytes"  # noqa: S105 - isolated smoke fixture
    with tempfile.TemporaryDirectory(prefix="samsarix-wheel-smoke-") as temporary:
        database = Path(temporary) / "smoke.db"
        environment = os.environ.copy()
        environment.update(
            {
                "SAMSARIX_CHAT_API_KEY": operator_key,
                "SAMSARIX_CHAT_TOKEN_SIGNING_SECRET": signing_secret,
                "SAMSARIX_CHAT_TOKEN_ISSUER": "samsarix-chat-engine",
                "SAMSARIX_CHAT_TOKEN_AUDIENCE": "samsarix-chat",
                "SAMSARIX_CHAT_TOKEN_MAX_LIFETIME": "86400",
                "SAMSARIX_CHAT_TOKEN_CLOCK_SKEW": "30",
                "SAMSARIX_CHAT_DATABASE": str(database),
            }
        )
        token_result = subprocess.run(  # noqa: S603 - fixed interpreter/module and controlled arguments
            [
                sys.executable,
                "-m",
                "samsarix_chat_engine",
                "token",
                "issue",
                "--subject",
                "wheel-user",
                "--room",
                "wheel-room",
                "--expires-in",
                "300",
            ],
            env=environment,
            check=True,
            capture_output=True,
            text=True,
        )
        token = token_result.stdout.strip()
        settings = Settings(
            database_path=database,
            api_key=operator_key,
            token_signing_secret=signing_secret,
            token_issuer=environment["SAMSARIX_CHAT_TOKEN_ISSUER"],
            token_audience=environment["SAMSARIX_CHAT_TOKEN_AUDIENCE"],
            token_max_lifetime_seconds=86_400,
            token_clock_skew_seconds=30,
        )
        server = uvicorn.Server(
            uvicorn.Config(
                create_app(settings),
                host="127.0.0.1",
                port=port,
                log_level="warning",
                ws_max_size=settings.websocket_max_bytes,
                timeout_graceful_shutdown=10,
            )
        )
        server_thread = threading.Thread(target=server.run, name="installed-wheel-server", daemon=True)
        server_thread.start()
        try:
            for _ in range(50):
                if not server_thread.is_alive():
                    break
                try:
                    if _request(base_url + "/readyz")["status"] == "ready":
                        break
                except URLError:
                    time.sleep(0.2)
            else:
                raise RuntimeError("installed-wheel server did not become ready")
            if not server_thread.is_alive():
                raise RuntimeError("installed-wheel server exited before becoming ready")

            room = _request(
                base_url + "/v1/rooms",
                method="POST",
                credential=("X-API-Key", operator_key),
                body={"id": "wheel-room", "name": "Wheel Room"},
            )
            message = _request(
                base_url + "/v1/rooms/wheel-room/messages",
                method="POST",
                credential=("Authorization", f"Bearer {token}"),
                body={"content": "installed wheel HTTP"},
            )
            history = _request(
                base_url + "/v1/rooms/wheel-room/messages",
                credential=("Authorization", f"Bearer {token}"),
            )
            websocket_sender = asyncio.run(_websocket_round_trip(base_url, token))
            if room["id"] != "wheel-room" or message["sender"] != "wheel-user" or len(history["items"]) != 1:
                raise RuntimeError("installed-wheel HTTP journey mismatch")
            exported = _request(
                base_url + "/v1/rooms/wheel-room/export",
                credential=("X-API-Key", operator_key),
            )
            export_lines = [json.loads(line) for line in exported.splitlines()]
            if (
                export_lines[0]["schema_version"] != 1
                or export_lines[1]["message"]["content"] != "installed wheel HTTP"
            ):
                raise RuntimeError("installed-wheel export journey mismatch")
            backup = Path(temporary) / "smoke-backup.db"
            subprocess.run(  # noqa: S603 - fixed interpreter/module and controlled arguments
                [sys.executable, "-m", "samsarix_chat_engine", "database", "backup", str(backup)],
                env=environment,
                check=True,
                capture_output=True,
                text=True,
            )
            archived = _request(
                base_url + "/v1/rooms/wheel-room",
                method="PATCH",
                credential=("X-API-Key", operator_key),
                body={"archived": True},
            )
            deleted = _request(
                base_url + "/v1/rooms/wheel-room",
                method="DELETE",
                credential=("X-API-Key", operator_key),
                headers={"X-Confirm-Room-Delete": "wheel-room"},
            )
            if archived["archived_at"] is None or deleted is not None or not backup.is_file():
                raise RuntimeError("installed-wheel lifecycle or backup journey mismatch")
            if not database.is_file() or database.stat().st_size == 0:
                raise RuntimeError("installed-wheel database was not persisted")
            print(
                f"http=ok websocket=ok export=ok lifecycle=ok backup=ok "
                f"sender={websocket_sender} history={len(history['items'])}"
            )
        finally:
            server.should_exit = True
            server_thread.join(timeout=10)
            if server_thread.is_alive():
                raise RuntimeError("installed-wheel server did not stop within 10 seconds")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
