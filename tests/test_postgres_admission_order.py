# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Delayed relay events must not override a later committed admission."""

import asyncio
import threading

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from samsarix_chat_engine import Settings, create_app

pytestmark = pytest.mark.postgres

_KEY = "admission-order-operator-key"
_SECRET = "admission-order-signing-secret-at-least-32-bytes"


def _app(conninfo, name):
    return create_app(
        Settings(
            storage_backend="postgres",
            postgres_url=conninfo,
            postgres_instance_id=name,
            postgres_lease_seconds=300,
            postgres_relay_poll_seconds=0.01,
            postgres_maintenance_interval_seconds=0.1,
            api_key=_KEY,
            token_signing_secret=_SECRET,
        )
    )


def _pause_event(application, monkeypatch, event_type):
    paused = threading.Event()
    resume = asyncio.Event()
    relay = application.state.postgres_runtime.relay
    original = relay._dispatch

    async def delayed(event):
        if event.event_type == event_type:
            paused.set()
            await resume.wait()
        await original(event)

    monkeypatch.setattr(relay, "_dispatch", delayed)
    return paused, resume


def _member_headers(application, subject="alice"):
    token = application.state.token_service.issue(subject, rooms=["room"], permissions=["room:read", "room:write"])
    return {"Authorization": f"Bearer {token}"}


def _ready(websocket):
    ready = websocket.receive_json()
    assert ready["type"] == "ready"
    assert ready["room"]["archived_at"] is None
    assert ready["room"]["frozen_at"] is None
    assert websocket.receive_json()["type"] == "history"
    return ready


def _sentinel(writer):
    result = writer.post(
        "/v1/rooms/room/messages",
        headers={"X-API-Key": _KEY},
        json={
            "sender": "Operator",
            "content": "After admission",
            "client_message_id": "fence-sentinel",
        },
    )
    assert result.status_code == 201
    return result.json()


@pytest.mark.parametrize("change", ["archive", "recreate", "ban", "freeze"])
def test_lifecycle_from_another_replica_cannot_override_a_later_admission(
    clean_postgres_database,
    monkeypatch,
    change,
):
    application = _app(clean_postgres_database, "delayed-member-replica")
    writer_app = _app(clean_postgres_database, "lifecycle-writer-replica")
    headers = {"X-API-Key": _KEY}
    with TestClient(application) as lagging, TestClient(writer_app) as writer:
        assert writer.post("/v1/rooms", headers=headers, json={"id": "room", "name": "Room"}).status_code == 201
        member = _member_headers(application)
        with lagging.websocket_connect("/v1/rooms/room/ws", headers=member) as older:
            _ready(older)
            held_type = {
                "archive": "room.archived",
                "recreate": "room.archived",
                "ban": "member.moderation.updated",
                "freeze": "room.frozen",
            }[change]
            paused, resume = _pause_event(application, monkeypatch, held_type)
            try:
                if change == "ban":
                    path = "/v1/rooms/room/members/alice/moderation"
                    assert writer.patch(path, headers=headers, json={"banned_for_seconds": 300}).status_code == 200
                    assert paused.wait(5), "ban relay did not reach barrier"
                    assert writer.patch(path, headers=headers, json={"banned_for_seconds": 0}).status_code == 200
                else:
                    field = "frozen" if change == "freeze" else "archived"
                    assert writer.patch("/v1/rooms/room", headers=headers, json={field: True}).status_code == 200
                    assert paused.wait(5), "room relay did not reach barrier"
                    if change == "recreate":
                        assert (
                            writer.delete(
                                "/v1/rooms/room", headers={**headers, "X-Confirm-Room-Delete": "room"}
                            ).status_code
                            == 204
                        )
                        assert (
                            writer.post(
                                "/v1/rooms", headers=headers, json={"id": "room", "name": "Replacement"}
                            ).status_code
                            == 201
                        )
                    else:
                        assert writer.patch("/v1/rooms/room", headers=headers, json={field: False}).status_code == 200
                with lagging.websocket_connect("/v1/rooms/room/ws", headers=member) as newer:
                    ready = _ready(newer)
                    if change == "recreate":
                        assert ready["room"]["name"] == "Replacement"
                    sentinel = _sentinel(writer)
                    lagging.portal.call(resume.set)
                    # The marker was committed after admission. All earlier
                    # lifecycle events must be filtered before it is delivered.
                    assert newer.receive_json() == {
                        "type": "message.created",
                        "message": sentinel,
                        "idempotent_replay": False,
                    }
                    if change == "freeze":
                        assert older.receive_json()["type"] == "room.frozen"
                        assert older.receive_json()["type"] == "room.unfrozen"
                    else:
                        assert older.receive_json()["type"] == ("member.banned" if change == "ban" else "room.archived")
                        with pytest.raises(WebSocketDisconnect) as closed:
                            older.receive_json()
                        assert closed.value.code == (4403 if change == "ban" else 4409)
                    newer.send_json({"type": "ping"})
                    event = newer.receive_json()
                    if event["type"] == "presence.left":
                        # The older socket's release can commit after the new
                        # admission; that newer departure must still be visible.
                        assert event["username"] == "alice"
                        assert event["active_connections"] == 1
                        event = newer.receive_json()
                    assert event == {"type": "pong"}
                    assert lagging.get("/readyz").status_code == writer.get("/readyz").status_code == 200
            finally:
                lagging.portal.call(resume.set)
        assert writer.get("/v1/stats", headers=headers).json() == {"active_connections": 0}


def test_older_presence_replay_is_excluded_but_newer_join_reaches_existing_peer(clean_postgres_database, monkeypatch):
    application = _app(clean_postgres_database, "delayed-presence-replica")
    with TestClient(application) as client:
        assert (
            client.post("/v1/rooms", headers={"X-API-Key": _KEY}, json={"id": "room", "name": "Room"}).status_code
            == 201
        )
        paused, resume = _pause_event(application, monkeypatch, "presence.joined")
        try:
            with client.websocket_connect("/v1/rooms/room/ws", headers=_member_headers(application, "alice")) as older:
                _ready(older)
                assert paused.wait(5), "presence relay did not reach barrier"
                with client.websocket_connect(
                    "/v1/rooms/room/ws", headers=_member_headers(application, "bob")
                ) as newer:
                    assert _ready(newer)["active_connections"] == 2
                    sentinel = _sentinel(client)
                    client.portal.call(resume.set)
                    assert older.receive_json() == {
                        "type": "presence.joined",
                        "username": "bob",
                        "active_connections": 2,
                    }
                    assert newer.receive_json() == {
                        "type": "message.created",
                        "message": sentinel,
                        "idempotent_replay": False,
                    }
                    newer.send_json({"type": "ping"})
                    assert newer.receive_json() == {"type": "pong"}
        finally:
            client.portal.call(resume.set)
