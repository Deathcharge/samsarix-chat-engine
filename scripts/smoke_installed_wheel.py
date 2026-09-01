"""Exercise an installed Samsarix Chat Engine over real HTTP and WebSockets."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen

import uvicorn
from websockets.asyncio.client import connect

from samsarix_chat_engine import Settings, create_app


class _WebhookReceiver(BaseHTTPRequestHandler):
    secret = b"0123456789abcdef0123456789abcdef"
    deliveries: list[dict[str, Any]] = []

    def do_POST(self) -> None:  # noqa: N802 - stdlib handler contract
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        delivery_id = self.headers.get("webhook-id")
        timestamp = self.headers.get("webhook-timestamp")
        signature = self.headers.get("webhook-signature")
        try:
            timestamp_value = int(timestamp or "")
        except ValueError:
            timestamp_value = 0
        if (
            not delivery_id
            or not timestamp
            or not signature
            or abs(int(time.time()) - timestamp_value) > 300
            or self.headers.get_content_type() != "application/json"
        ):
            self.send_response(400)
            self.end_headers()
            return
        signed = delivery_id.encode("ascii") + b"." + timestamp.encode("ascii") + b"." + body
        expected = base64.b64encode(hmac.new(self.secret, signed, hashlib.sha256).digest()).decode("ascii")
        candidates = [part.removeprefix("v1,") for part in signature.split() if part.startswith("v1,")]
        if not any(hmac.compare_digest(expected, candidate) for candidate in candidates):
            self.send_response(400)
            self.end_headers()
            return
        self.deliveries.append(json.loads(body))
        self.send_response(204)
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:
        pass


def _available_port() -> int:
    with socket.socket() as candidate:
        candidate.bind(("127.0.0.1", 0))
        return int(candidate.getsockname()[1])


@contextmanager
def _running_webhook_receiver() -> Iterator[ThreadingHTTPServer]:
    receiver = ThreadingHTTPServer(("127.0.0.1", 0), _WebhookReceiver)
    thread = threading.Thread(target=receiver.serve_forever, name="wheel-webhook", daemon=True)
    started = False
    try:
        _WebhookReceiver.deliveries = []
        thread.start()
        started = True
        yield receiver
    finally:
        if started:
            receiver.shutdown()
        receiver.server_close()
        if started:
            thread.join(timeout=5)
            if thread.is_alive():
                raise RuntimeError("installed-wheel webhook receiver did not stop within 5 seconds")


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
    async def receive_json(websocket: Any) -> Any:
        return json.loads(await asyncio.wait_for(websocket.recv(), timeout=5))

    websocket_url = base_url.replace("http://", "ws://") + "/v1/rooms/wheel-room/ws"
    async with (
        connect(websocket_url, max_size=16_384) as websocket,
        connect(websocket_url, max_size=16_384) as observer,
    ):
        required = await receive_json(websocket)
        if required["type"] != "auth.required":
            raise RuntimeError("WebSocket did not request authentication")
        await websocket.send(json.dumps({"type": "auth", "token": token}))
        ready = await receive_json(websocket)
        history = await receive_json(websocket)
        if ready["username"] != "wheel-user" or history["items"][0]["sender"] != "wheel-user":
            raise RuntimeError("WebSocket identity or recovery mismatch")

        observer_required = await receive_json(observer)
        if observer_required["type"] != "auth.required":
            raise RuntimeError("WebSocket observer did not request authentication")
        await observer.send(json.dumps({"type": "auth", "token": token}))
        observer_ready = await receive_json(observer)
        observer_history = await receive_json(observer)
        joined = await receive_json(websocket)
        if (
            observer_ready["username"] != "wheel-user"
            or observer_history["type"] != "history"
            or joined["type"] != "presence.joined"
        ):
            raise RuntimeError("WebSocket observer setup mismatch")

        await websocket.send(json.dumps({"type": "typing", "active": True}))
        typing_started = await receive_json(observer)
        if typing_started["type"] != "typing.started" or typing_started["username"] != "wheel-user":
            raise RuntimeError("WebSocket typing transition mismatch")
        await websocket.send(json.dumps({"type": "message", "content": "installed wheel WebSocket"}))
        typing_stopped = await receive_json(observer)
        observer_created = await receive_json(observer)
        created = await receive_json(websocket)
        if (
            typing_stopped["type"] != "typing.stopped"
            or observer_created["type"] != "message.created"
            or created["message"]["sender"] != "wheel-user"
        ):
            raise RuntimeError("WebSocket typing stop or sender identity mismatch")
        sender = created["message"]["sender"]
        if not isinstance(sender, str):
            raise RuntimeError("WebSocket sender was not a string")
        return sender


def _wait_for_webhooks(base_url: str, operator_key: str, expected_count: int) -> None:
    for _ in range(100):
        webhook_page = _request(
            base_url + "/v1/admin/webhook-deliveries",
            credential=("X-API-Key", operator_key),
        )
        webhook_items = webhook_page["items"]
        if (
            len(webhook_items) == expected_count
            and all(item["delivered_at"] is not None for item in webhook_items)
            and len(_WebhookReceiver.deliveries) == expected_count
        ):
            return
        time.sleep(0.05)
    raise RuntimeError(f"installed-wheel webhook delivery count {expected_count} did not settle")


def main() -> int:
    port = _available_port()
    base_url = f"http://127.0.0.1:{port}"
    operator_key = "installed-wheel-operator-key"
    signing_secret = "installed-wheel-signing-secret-32-bytes"  # noqa: S105 - isolated smoke fixture
    webhook_secret = "whsec_" + base64.b64encode(_WebhookReceiver.secret).decode("ascii")
    with (
        _running_webhook_receiver() as webhook_receiver,
        tempfile.TemporaryDirectory(prefix="samsarix-wheel-smoke-") as temporary,
    ):
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
                "--permission",
                "room:read",
                "--permission",
                "room:write",
                "--permission",
                "room:pin",
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
            webhook_url=f"http://127.0.0.1:{webhook_receiver.server_port}/chat",
            webhook_signing_secret=webhook_secret,
            webhook_events=(
                "member.moderation.updated",
                "message.created",
                "message.deleted",
                "message.pin.updated",
                "message.reaction.updated",
                "message.updated",
            ),
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
            _request(
                base_url + "/v1/rooms/wheel-room/messages",
                method="POST",
                credential=("X-API-Key", operator_key),
                body={"sender": "Support agent", "content": "installed wheel unread"},
            )
            history = _request(
                base_url + "/v1/rooms/wheel-room/messages",
                credential=("Authorization", f"Bearer {token}"),
            )
            search = _request(
                base_url + "/v1/rooms/wheel-room/messages/search?q=wheel+unread",
                credential=("Authorization", f"Bearer {token}"),
            )
            read_before = _request(
                base_url + "/v1/rooms/wheel-room/read-state",
                credential=("Authorization", f"Bearer {token}"),
            )
            read_after = _request(
                base_url + "/v1/rooms/wheel-room/read-state",
                method="PUT",
                credential=("Authorization", f"Bearer {token}"),
                body={},
            )
            cleared_read = _request(
                base_url + "/v1/rooms/wheel-room/read-state",
                method="DELETE",
                credential=("Authorization", f"Bearer {token}"),
            )
            if read_before["unread_count"] != 1 or read_after["unread_count"] != 0 or cleared_read is not None:
                raise RuntimeError("installed-wheel read-state journey mismatch")
            _request(
                base_url + "/v1/rooms/wheel-room/read-state",
                method="PUT",
                credential=("Authorization", f"Bearer {token}"),
                body={},
            )
            reply = _request(
                base_url + "/v1/rooms/wheel-room/messages",
                method="POST",
                credential=("Authorization", f"Bearer {token}"),
                body={"content": "installed wheel reply", "parent_message_id": message["id"]},
            )
            replies = _request(
                base_url + f"/v1/rooms/wheel-room/messages/{message['id']}/replies",
                credential=("Authorization", f"Bearer {token}"),
            )
            reaction = _request(
                base_url + f"/v1/rooms/wheel-room/messages/{reply['id']}/reactions/ack",
                method="PUT",
                credential=("Authorization", f"Bearer {token}"),
                body={},
            )
            pin = _request(
                base_url + f"/v1/rooms/wheel-room/messages/{reply['id']}/pin",
                method="PUT",
                credential=("Authorization", f"Bearer {token}"),
                body={},
            )
            pins = _request(
                base_url + "/v1/rooms/wheel-room/messages/pins",
                credential=("Authorization", f"Bearer {token}"),
            )
            websocket_sender = asyncio.run(_websocket_round_trip(base_url, token))
            if (
                room["id"] != "wheel-room"
                or message["sender"] != "wheel-user"
                or len(history["items"]) != 2
                or [item["content"] for item in search["items"]] != ["installed wheel unread"]
                or reply["parent_message_id"] != message["id"]
                or [item["id"] for item in replies["items"]] != [reply["id"]]
                or reaction["message"]["reactions"] != [{"key": "ack", "count": 1}]
                or pin["message"]["pinned_by"] != "wheel-user"
                or [item["id"] for item in pins["items"]] != [reply["id"]]
            ):
                raise RuntimeError("installed-wheel HTTP journey mismatch")
            _wait_for_webhooks(base_url, operator_key, 6)
            edited = _request(
                base_url + f"/v1/rooms/wheel-room/messages/{message['id']}",
                method="PATCH",
                credential=("Authorization", f"Bearer {token}"),
                body={"content": "installed wheel edited"},
            )
            _wait_for_webhooks(base_url, operator_key, 7)
            deleted_message = _request(
                base_url + f"/v1/rooms/wheel-room/messages/{message['id']}",
                method="DELETE",
                credential=("Authorization", f"Bearer {token}"),
            )
            frozen = _request(
                base_url + "/v1/rooms/wheel-room",
                method="PATCH",
                credential=("X-API-Key", operator_key),
                body={"frozen": True},
            )
            unfrozen = _request(
                base_url + "/v1/rooms/wheel-room",
                method="PATCH",
                credential=("X-API-Key", operator_key),
                body={"frozen": False},
            )
            muted = _request(
                base_url + "/v1/rooms/wheel-room/members/wheel-user/moderation",
                method="PATCH",
                credential=("X-API-Key", operator_key),
                body={"muted_for_seconds": 60},
            )
            cleared = _request(
                base_url + "/v1/rooms/wheel-room/members/wheel-user/moderation",
                method="PATCH",
                credential=("X-API-Key", operator_key),
                body={"muted_for_seconds": 0},
            )
            if edited["edited_at"] is None:
                raise RuntimeError("installed-wheel edit control mismatch")
            if deleted_message is not None:
                raise RuntimeError("installed-wheel delete control mismatch")
            if frozen["frozen_at"] is None:
                raise RuntimeError("installed-wheel freeze control mismatch")
            if unfrozen["frozen_at"] is not None:
                raise RuntimeError("installed-wheel unfreeze control mismatch")
            if muted["muted_until"] is None:
                raise RuntimeError("installed-wheel mute control mismatch")
            if cleared["muted_until"] is not None:
                raise RuntimeError("installed-wheel clear-control mismatch")
            _wait_for_webhooks(base_url, operator_key, 10)
            webhook_types = sorted(delivery["type"] for delivery in _WebhookReceiver.deliveries)
            if webhook_types != sorted(
                [
                    "message.created",
                    "message.created",
                    "message.created",
                    "message.created",
                    "message.updated",
                    "message.deleted",
                    "message.pin.updated",
                    "message.reaction.updated",
                    "member.moderation.updated",
                    "member.moderation.updated",
                ]
            ):
                raise RuntimeError("installed-wheel signed webhook journey mismatch")
            exported = _request(
                base_url + "/v1/rooms/wheel-room/export",
                credential=("X-API-Key", operator_key),
            )
            export_lines = [json.loads(line) for line in exported.splitlines()]
            if (
                export_lines[0]["schema_version"] != 5
                or export_lines[1]["message"]["content"] != ""
                or export_lines[1]["message"]["deleted_at"] is None
                or export_lines[3]["message"]["parent_message_id"] != message["id"]
                or export_lines[3]["message"]["reactions"] != [{"key": "ack", "count": 1}]
                or export_lines[3]["message"]["pinned_by"] != "wheel-user"
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
                f"http=ok search=ok threads=ok reactions=ok pins=ok websocket=ok read_state=ok typing=ok controls=ok "
                f"webhook=ok export=ok "
                f"lifecycle=ok backup=ok "
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
