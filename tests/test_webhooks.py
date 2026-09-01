# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Durable signed-webhook outbox, transport, retry, and operations coverage."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from typing import Any

import pytest
from fastapi.testclient import TestClient

from samsarix_chat_engine import Settings, create_app
from samsarix_chat_engine.config import ConfigurationError
from samsarix_chat_engine.models import MemberModerationUpdate, RoomCreate
from samsarix_chat_engine.store import ChatStore, WebhookCapacityError, WebhookPayloadUnavailableError
from samsarix_chat_engine.webhooks import (
    WebhookAttemptResult,
    WebhookDispatcher,
    _retry_after_seconds,
    _send_request,
    sign_webhook,
)

API_KEY = "operator-key-for-webhook-tests"
SECRET_BYTES = b"samsarix-webhook-secret-32-bytes!"
SECRET = "whsec_" + base64.b64encode(SECRET_BYTES).decode("ascii")
EVENTS = (
    "member.moderation.updated",
    "message.created",
    "message.deleted",
    "message.updated",
)


def _store(path: Path, *, max_deliveries: int = 100) -> ChatStore:
    return ChatStore(
        path,
        max_rooms=10,
        max_stored_messages=100,
        max_stored_messages_per_room=100,
        webhook_events=EVENTS,
        max_webhook_deliveries=max_deliveries,
    )


@pytest.mark.asyncio
async def test_outbox_is_transactional_idempotent_and_covers_committed_events(tmp_path: Path) -> None:
    store = _store(tmp_path / "outbox.db")
    await store.initialize()
    await store.create_room(RoomCreate(id="support", name="Support"))

    created, was_created = await store.create_message(
        room_id="support",
        sender="alice",
        content="hello",
        metadata={"ticket.id": "SUP-42"},
        client_message_id="client-1",
        allow_frozen=False,
        author_subject="alice",
    )
    assert was_created is True
    replay, was_created = await store.create_message(
        room_id="support",
        sender="alice",
        content="ignored replay body",
        client_message_id="client-1",
        allow_frozen=False,
        author_subject="alice",
    )
    assert was_created is False
    assert replay == created
    first_delivery = await store.next_webhook_delivery(datetime.now(timezone.utc) + timedelta(seconds=1))
    assert first_delivery is not None
    assert json.loads(first_delivery.payload)["data"]["message"]["metadata"] == {"ticket.id": "SUP-42"}
    await store.record_webhook_attempt(
        first_delivery.delivery.id,
        attempted_at=datetime.now(timezone.utc),
        status_code=204,
        error=None,
        next_attempt_at=None,
        delivered=True,
        failed=False,
    )

    updated = await store.update_message(
        room_id="support",
        message_id=created.id,
        actor="alice",
        content="updated",
        metadata={"ticket.status": "resolved"},
        is_admin=False,
        member_subject="alice",
    )
    update_delivery = await store.next_webhook_delivery(datetime.now(timezone.utc) + timedelta(seconds=1))
    assert update_delivery is not None
    assert json.loads(update_delivery.payload)["data"]["message"]["metadata"] == {"ticket.status": "resolved"}
    await store.record_webhook_attempt(
        update_delivery.delivery.id,
        attempted_at=datetime.now(timezone.utc),
        status_code=204,
        error=None,
        next_attempt_at=None,
        delivered=True,
        failed=False,
    )
    deleted, changed = await store.delete_message(
        room_id="support",
        message_id=created.id,
        actor="alice",
        is_admin=False,
        member_subject="alice",
    )
    assert changed is True
    moderation = await store.set_member_moderation(
        "support",
        "bob",
        MemberModerationUpdate(muted_for_seconds=60),
        actor="operator-api-key",
    )

    deliveries, cursor = await store.list_webhook_deliveries(limit=10)
    assert cursor is None
    assert [item.event_type for item in reversed(deliveries)] == [
        "message.created",
        "message.updated",
        "message.deleted",
        "member.moderation.updated",
    ]
    assert [item.replayable for item in reversed(deliveries)] == [False, False, True, True]
    pending = await store.next_webhook_delivery(datetime.now(timezone.utc) + timedelta(seconds=1))
    assert pending is not None
    envelope = json.loads(pending.payload)
    assert envelope["id"] == pending.delivery.id
    assert envelope["type"] == "message.deleted"
    assert envelope["data"]["message"]["content"] == ""
    assert envelope["data"]["message"]["metadata"] == {}
    assert envelope["data"]["room_id"] == "support"
    assert updated.content == "updated"
    assert deleted.content == ""
    assert moderation.subject == "bob"

    await store.set_room_archived("support", archived=True, actor="operator-api-key")
    await store.delete_room("support", actor="operator-api-key")
    after_room_delete, _ = await store.list_webhook_deliveries(limit=10)
    assert len(after_room_delete) == 2
    assert all(item.replayable is False for item in after_room_delete)
    with pytest.raises(WebhookPayloadUnavailableError):
        await store.retry_webhook_delivery(after_room_delete[0].id)


@pytest.mark.asyncio
async def test_pin_webhook_is_idempotent_and_tombstone_scrubbed(tmp_path: Path) -> None:
    store = ChatStore(
        tmp_path / "pin-outbox.db",
        max_rooms=10,
        max_stored_messages=100,
        max_stored_messages_per_room=100,
        webhook_events=("message.pin.updated",),
        max_webhook_deliveries=10,
    )
    await store.initialize()
    await store.create_room(RoomCreate(id="support", name="Support"))
    message, _ = await store.create_message(
        room_id="support",
        sender="agent",
        content="Resolution",
        client_message_id=None,
        allow_frozen=False,
        author_subject="agent",
    )
    pinned = await store.set_message_pin(
        room_id="support",
        message_id=message.id,
        pinner="agent",
        actor="agent",
        pinned=True,
        allow_frozen=False,
        member_subject="agent",
    )
    replay = await store.set_message_pin(
        room_id="support",
        message_id=message.id,
        pinner="agent",
        actor="agent",
        pinned=True,
        allow_frozen=False,
        member_subject="agent",
    )
    assert pinned.changed and not replay.changed
    pending = await store.next_webhook_delivery(datetime.now(timezone.utc) + timedelta(seconds=1))
    assert pending is not None
    envelope = json.loads(pending.payload)
    assert envelope["type"] == "message.pin.updated"
    assert envelope["data"]["pinner"] == "agent"
    assert envelope["data"]["message"]["pinned_by"] == "agent"
    await store.record_webhook_attempt(
        pending.delivery.id,
        attempted_at=datetime.now(timezone.utc),
        status_code=204,
        error=None,
        next_attempt_at=None,
        delivered=True,
        failed=False,
    )
    await store.set_message_pin(
        room_id="support",
        message_id=message.id,
        pinner="agent",
        actor="agent",
        pinned=False,
        allow_frozen=False,
        member_subject="agent",
    )
    await store.delete_message(
        room_id="support",
        message_id=message.id,
        actor="agent",
        is_admin=False,
        member_subject="agent",
    )
    deliveries, _ = await store.list_webhook_deliveries(limit=10)
    assert len(deliveries) == 1
    assert deliveries[0].event_type == "message.pin.updated"
    assert deliveries[0].replayable is False


@pytest.mark.asyncio
async def test_pending_outbox_cap_rolls_back_the_chat_write(tmp_path: Path) -> None:
    store = _store(tmp_path / "capacity.db", max_deliveries=1)
    await store.initialize()
    await store.create_room(RoomCreate(id="room", name="Room"))
    await store.create_message(
        room_id="room",
        sender="alice",
        content="first",
        client_message_id=None,
        allow_frozen=False,
    )
    with pytest.raises(WebhookCapacityError):
        await store.create_message(
            room_id="room",
            sender="alice",
            content="must roll back",
            client_message_id=None,
            allow_frozen=False,
        )
    messages, _ = await store.list_messages("room")
    assert [message.content for message in messages] == ["first"]


@pytest.mark.asyncio
async def test_retention_scrubs_delivered_payloads_and_cancels_pending_payloads(tmp_path: Path) -> None:
    store = ChatStore(
        tmp_path / "retention.db",
        max_rooms=10,
        max_stored_messages=100,
        max_stored_messages_per_room=1,
        webhook_events=EVENTS,
        max_webhook_deliveries=100,
    )
    await store.initialize()
    await store.create_room(RoomCreate(id="room", name="Room"))
    await store.create_message(
        room_id="room",
        sender="alice",
        content="first",
        client_message_id=None,
        allow_frozen=False,
    )
    first = await store.next_webhook_delivery(datetime.now(timezone.utc) + timedelta(seconds=1))
    assert first is not None
    await store.create_message(
        room_id="room",
        sender="alice",
        content="second",
        client_message_id=None,
        allow_frozen=False,
    )
    second = await store.next_webhook_delivery(datetime.now(timezone.utc) + timedelta(seconds=1))
    assert second is not None
    assert second.delivery.id != first.delivery.id
    after_pending_trim, _ = await store.list_webhook_deliveries(limit=10)
    assert [delivery.id for delivery in after_pending_trim] == [second.delivery.id]
    await store.record_webhook_attempt(
        second.delivery.id,
        attempted_at=datetime.now(timezone.utc),
        status_code=204,
        error=None,
        next_attempt_at=None,
        delivered=True,
        failed=False,
    )

    await store.create_message(
        room_id="room",
        sender="alice",
        content="third",
        client_message_id=None,
        allow_frozen=False,
    )
    deliveries, _ = await store.list_webhook_deliveries(limit=10)
    assert len(deliveries) == 2
    assert deliveries[0].replayable is True
    assert deliveries[1].id == second.delivery.id
    assert deliveries[1].replayable is False
    with pytest.raises(WebhookPayloadUnavailableError):
        await store.retry_webhook_delivery(second.delivery.id)


def test_standard_signature_covers_stable_id_timestamp_and_exact_payload() -> None:
    payload = b'{"type":"message.created","data":{"unicode":"\xe2\x98\x83"}}'
    signature = sign_webhook("wh_delivery", 1_700_000_000, payload, (SECRET_BYTES,))
    expected = base64.b64encode(
        hmac.new(SECRET_BYTES, b"wh_delivery.1700000000." + payload, hashlib.sha256).digest()
    ).decode("ascii")
    assert signature == f"v1,{expected}"


def test_signature_lists_current_and_previous_secrets_in_order() -> None:
    previous = b"samsarix-webhook-previous-32-byte"
    payload = b'{"type":"message.created"}'
    signature = sign_webhook("wh_delivery", 1_700_000_000, payload, (SECRET_BYTES, previous))
    parts = signature.split(" ")
    assert parts == [
        sign_webhook("wh_delivery", 1_700_000_000, payload, (SECRET_BYTES,)),
        sign_webhook("wh_delivery", 1_700_000_000, payload, (previous,)),
    ]


def test_retry_after_rejects_non_finite_receiver_values() -> None:
    now = datetime.now(timezone.utc)
    assert _retry_after_seconds("NaN", now) is None
    assert _retry_after_seconds("Infinity", now) is None


@pytest.mark.asyncio
async def test_dispatcher_retries_then_delivers_and_manual_replay_reuses_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path / "retry.db")
    await store.initialize()
    await store.create_room(RoomCreate(id="room", name="Room"))
    await store.create_message(
        room_id="room",
        sender="alice",
        content="retry me",
        client_message_id=None,
        allow_frozen=False,
    )
    attempts: list[str] = []

    def fake_send(**kwargs: Any) -> WebhookAttemptResult:
        attempts.append(kwargs["delivery"].delivery.id)
        if len(attempts) == 1:
            return WebhookAttemptResult(status_code=503, error="http_status_503", retry_after_seconds=20)
        return WebhookAttemptResult(status_code=204, error=None)

    monkeypatch.setattr("samsarix_chat_engine.webhooks._send_request", fake_send)
    dispatcher = WebhookDispatcher(
        store,
        url="https://hooks.example.com/chat",
        secrets=(SECRET_BYTES,),
        timeout=1,
        max_attempts=3,
        allow_private_targets=False,
    )
    started = datetime.now(timezone.utc) + timedelta(seconds=1)
    assert await dispatcher.process_due_once(now=started) is True
    pending, _ = await store.list_webhook_deliveries(status="pending")
    assert pending[0].attempt_count == 1
    assert pending[0].next_attempt_at == started + timedelta(seconds=20)
    assert await dispatcher.process_due_once(now=started + timedelta(seconds=19)) is False
    assert await dispatcher.process_due_once(now=started + timedelta(seconds=20)) is True
    delivered, _ = await store.list_webhook_deliveries(status="delivered")
    assert delivered[0].attempt_count == 2
    assert attempts == [delivered[0].id, delivered[0].id]

    replay = await store.retry_webhook_delivery(delivered[0].id)
    assert replay.id == delivered[0].id
    assert replay.attempt_count == 0
    dispatcher.stop()


class _CaptureHandler(BaseHTTPRequestHandler):
    body = b""
    headers_received: dict[str, str] = {}
    response_status = 204

    def do_POST(self) -> None:  # noqa: N802 - stdlib handler contract
        length = int(self.headers.get("Content-Length", "0"))
        type(self).body = self.rfile.read(length)
        type(self).headers_received = dict(self.headers.items())
        self.send_response(type(self).response_status)
        if type(self).response_status == 302:
            self.send_header("Location", "http://127.0.0.1:1/redirected")
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:
        pass


@pytest.mark.asyncio
async def test_real_loopback_transport_sends_exact_signed_body_and_rejects_redirect(tmp_path: Path) -> None:
    store = _store(tmp_path / "transport.db")
    await store.initialize()
    await store.create_room(RoomCreate(id="room", name="Room"))
    await store.create_message(
        room_id="room",
        sender="alice",
        content="signed",
        client_message_id=None,
        allow_frozen=False,
    )
    pending = await store.next_webhook_delivery(datetime.now(timezone.utc) + timedelta(seconds=1))
    assert pending is not None
    server = ThreadingHTTPServer(("127.0.0.1", 0), _CaptureHandler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    attempted_at = datetime(2026, 8, 2, 12, 0, tzinfo=timezone.utc)
    url = f"http://127.0.0.1:{server.server_port}/hook"
    try:
        _CaptureHandler.response_status = 204
        result = _send_request(
            url=url,
            delivery=pending,
            secrets=(SECRET_BYTES,),
            timeout=2,
            attempted_at=attempted_at,
            allow_private_targets=False,
        )
        assert result.delivered is True
        assert _CaptureHandler.body == pending.payload
        received = {key.lower(): value for key, value in _CaptureHandler.headers_received.items()}
        assert received["webhook-id"] == pending.delivery.id
        assert received["webhook-timestamp"] == str(int(attempted_at.timestamp()))
        assert received["webhook-signature"] == sign_webhook(
            pending.delivery.id,
            int(attempted_at.timestamp()),
            pending.payload,
            (SECRET_BYTES,),
        )

        _CaptureHandler.response_status = 302
        redirected = _send_request(
            url=url,
            delivery=pending,
            secrets=(SECRET_BYTES,),
            timeout=2,
            attempted_at=attempted_at,
            allow_private_targets=False,
        )
        assert redirected.status_code == 302
        assert redirected.error == "http_status_302"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@pytest.mark.asyncio
async def test_remote_private_resolution_is_blocked_before_connect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path / "private-target.db")
    await store.initialize()
    await store.create_room(RoomCreate(id="room", name="Room"))
    await store.create_message(
        room_id="room",
        sender="alice",
        content="do not leak",
        client_message_id=None,
        allow_frozen=False,
    )
    pending = await store.next_webhook_delivery(datetime.now(timezone.utc) + timedelta(seconds=1))
    assert pending is not None
    monkeypatch.setattr(
        "samsarix_chat_engine.webhooks.socket.getaddrinfo",
        lambda *args, **kwargs: [(2, 1, 6, "", ("10.0.0.7", 443))],
    )
    result = _send_request(
        url="https://hooks.example.com/chat",
        delivery=pending,
        secrets=(SECRET_BYTES,),
        timeout=1,
        attempted_at=datetime.now(timezone.utc),
        allow_private_targets=False,
    )
    assert result.status_code is None
    assert result.error == "private_target_blocked"


@pytest.mark.asyncio
async def test_https_transport_connects_to_the_validated_address_with_original_host(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path / "pinned-target.db")
    await store.initialize()
    await store.create_room(RoomCreate(id="room", name="Room"))
    await store.create_message(
        room_id="room",
        sender="alice",
        content="pin destination",
        client_message_id=None,
        allow_frozen=False,
    )
    pending = await store.next_webhook_delivery(datetime.now(timezone.utc) + timedelta(seconds=1))
    assert pending is not None
    captured: dict[str, Any] = {}

    class FakeResponse:
        status = 204

        def getheader(self, name: str) -> None:
            return None

        def close(self) -> None:
            pass

    class FakePinnedConnection:
        def __init__(self, hostname: str, address: str, port: int, timeout: float, *, budget: Any) -> None:
            assert 0 < budget.remaining() <= timeout
            captured.update(hostname=hostname, address=address, port=port, timeout=timeout)

        def request(self, method: str, path: str, *, body: bytes, headers: dict[str, str]) -> None:
            captured.update(method=method, path=path, body=body, headers=headers)

        def getresponse(self) -> FakeResponse:
            return FakeResponse()

        def close(self) -> None:
            pass

    monkeypatch.setattr(
        "samsarix_chat_engine.webhooks.socket.getaddrinfo",
        lambda *args, **kwargs: [(2, 1, 6, "", ("93.184.216.34", 443))],
    )
    monkeypatch.setattr("samsarix_chat_engine.webhooks._PinnedHTTPSConnection", FakePinnedConnection)
    result = _send_request(
        url="https://hooks.example.com/events",
        delivery=pending,
        secrets=(SECRET_BYTES,),
        timeout=4,
        attempted_at=datetime.now(timezone.utc),
        allow_private_targets=False,
    )
    assert result.delivered is True
    assert captured["hostname"] == "hooks.example.com"
    assert captured["address"] == "93.184.216.34"
    assert captured["port"] == 443
    assert captured["path"] == "/events"
    assert captured["headers"]["Host"] == "hooks.example.com"
    assert captured["body"] == pending.payload


def test_webhook_configuration_rejects_unsafe_or_incomplete_values() -> None:
    with pytest.raises(ConfigurationError, match="required"):
        Settings(webhook_url="https://hooks.example.com/chat", webhook_events=("message.created",))
    with pytest.raises(ConfigurationError, match="HTTPS"):
        Settings(
            webhook_url="http://hooks.example.com/chat",
            webhook_signing_secret=SECRET,
            webhook_events=("message.created",),
        )
    with pytest.raises(ConfigurationError, match="whsec"):
        Settings(
            webhook_url="https://hooks.example.com/chat",
            webhook_signing_secret="not-a-standard-secret",
            webhook_events=("message.created",),
        )
    with pytest.raises(ConfigurationError, match="query"):
        Settings(
            webhook_url="https://hooks.example.com/chat?token=secret",
            webhook_signing_secret=SECRET,
            webhook_events=("message.created",),
        )
    configured = Settings(
        webhook_url="http://127.0.0.1:9000/chat",
        webhook_signing_secret=SECRET,
        webhook_events=("message.created",),
    )
    assert configured.webhook_url == "http://127.0.0.1:9000/chat"


def test_operations_api_exposes_metadata_without_payload_and_replays(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def succeed(**kwargs: Any) -> WebhookAttemptResult:
        return WebhookAttemptResult(status_code=202, error=None)

    monkeypatch.setattr("samsarix_chat_engine.webhooks._send_request", succeed)
    settings = Settings(
        database_path=tmp_path / "api.db",
        api_key=API_KEY,
        webhook_url="https://hooks.example.com/chat",
        webhook_signing_secret=SECRET,
        webhook_events=("message.created",),
    )
    with TestClient(create_app(settings)) as client:
        headers = {"X-API-Key": API_KEY}
        assert client.post("/v1/rooms", headers=headers, json={"id": "room", "name": "Room"}).status_code == 201
        assert (
            client.post(
                "/v1/rooms/room/messages",
                headers=headers,
                json={"sender": "operator", "content": "notify"},
            ).status_code
            == 201
        )
        deadline = time.monotonic() + 3
        items: list[dict[str, Any]] = []
        while time.monotonic() < deadline:
            response = client.get("/v1/admin/webhook-deliveries", headers=headers)
            assert response.status_code == 200
            items = response.json()["items"]
            if items and items[0]["delivered_at"] is not None:
                break
            time.sleep(0.02)
        assert len(items) == 1
        assert "payload" not in items[0]
        assert items[0]["last_status_code"] == 202
        delivery_id = items[0]["id"]
        replay = client.post(f"/v1/admin/webhook-deliveries/{delivery_id}/retry", headers=headers)
        assert replay.status_code == 202
        assert replay.json()["id"] == delivery_id
        assert client.get("/v1/admin/webhook-deliveries").status_code == 401
