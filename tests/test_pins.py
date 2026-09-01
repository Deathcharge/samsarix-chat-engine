# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""End-to-end coverage for durable, least-privilege shared message pins."""

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from samsarix_chat_engine import Settings, create_app
from samsarix_chat_engine.auth import AccessTokenService
from samsarix_chat_engine.models import Message


def _message(client: TestClient, content: str) -> dict[str, object]:
    response = client.post(
        "/v1/rooms/general/messages",
        json={"sender": "Author", "content": content},
    )
    assert response.status_code == 201
    return response.json()


def _pin(client: TestClient, message_id: object, pinner: str, *, method: str = "PUT"):
    return client.request(
        method,
        f"/v1/rooms/general/messages/{message_id}/pin",
        json={"pinner": pinner},
    )


def test_pins_are_idempotent_realtime_paginated_and_persistent(
    client: TestClient,
    room: dict[str, str],
) -> None:
    first = _message(client, "First runbook")
    second = _message(client, "Second runbook")
    third = _message(client, "Current resolution")

    with client.websocket_connect("/v1/rooms/general/ws?username=Observer") as websocket:
        websocket.receive_json()
        websocket.receive_json()
        first_pin = _pin(client, first["id"], "lead")
        first_event = websocket.receive_json()
        replay = _pin(client, first["id"], "lead")
        second_pin = _pin(client, second["id"], "teacher")
        second_event = websocket.receive_json()
        third_pin = _pin(client, third["id"], "agent")
        third_event = websocket.receive_json()

    assert first_pin.status_code == 200
    assert first_pin.json()["changed"] is True
    assert first_pin.json()["pinned"] is True
    assert first_pin.json()["message"]["pinned_by"] == "lead"
    assert first_pin.json()["message"]["pinned_at"] is not None
    assert first_event["type"] == "message.pin.updated"
    assert first_event["message"] == first_pin.json()["message"]
    assert replay.json()["changed"] is False
    assert second_event["message"] == second_pin.json()["message"]
    assert third_event["message"] == third_pin.json()["message"]
    incomplete_pin = {**first_pin.json()["message"], "pinned_by": None}
    with pytest.raises(ValidationError, match="must both be set"):
        Message.model_validate(incomplete_pin)
    deleted_pin = {**first_pin.json()["message"], "deleted_at": first_pin.json()["updated_at"]}
    with pytest.raises(ValidationError, match="cannot remain pinned"):
        Message.model_validate(deleted_pin)

    page = client.get("/v1/rooms/general/messages/pins", params={"limit": 2}).json()
    expected_ids = [
        item["id"]
        for item in sorted(
            [first_pin.json()["message"], second_pin.json()["message"], third_pin.json()["message"]],
            key=lambda item: (item["pinned_at"], item["id"]),
            reverse=True,
        )
    ]
    assert [item["id"] for item in page["items"]] == expected_ids[:2]
    assert page["next_before"] == expected_ids[1]
    older = client.get(
        "/v1/rooms/general/messages/pins",
        params={"limit": 2, "before": page["next_before"]},
    ).json()
    assert [item["id"] for item in older["items"]] == expected_ids[2:]
    assert older["next_before"] is None

    removed = _pin(client, second["id"], "teacher", method="DELETE")
    assert removed.json()["changed"] is True
    assert removed.json()["pinned"] is False
    assert removed.json()["message"]["pinned_at"] is None
    assert removed.json()["message"]["pinned_by"] is None
    history = client.get("/v1/rooms/general/messages").json()["items"]
    assert next(item for item in history if item["id"] == first["id"])["pinned_by"] == "lead"


def test_pin_permission_identity_lifecycle_and_tombstone_privacy(tmp_path) -> None:
    secret = "pin-test-signing-secret-that-is-long-enough"
    settings = Settings(
        database_path=tmp_path / "pin-auth.db",
        api_key="pin-test-operator-key",
        token_signing_secret=secret,
        token_issuer="pin-tests",
        token_audience="pin-client",
    )
    tokens = AccessTokenService(secret, issuer="pin-tests", audience="pin-client")
    operator = {"X-API-Key": "pin-test-operator-key"}

    def headers(subject: str, permissions: list[str]) -> dict[str, str]:
        token = tokens.issue(
            subject,
            rooms=["general"],
            permissions=permissions,  # type: ignore[arg-type]
            expires_in_seconds=300,
        )
        return {"Authorization": f"Bearer {token}"}

    reader = headers("reader", ["room:read"])
    pin_only = headers("blind-pinner", ["room:pin"])
    pinner = headers("agent-7", ["room:read", "room:pin"])
    with TestClient(create_app(settings)) as client:
        client.post("/v1/rooms", headers=operator, json={"id": "general", "name": "General"})
        message = client.post(
            "/v1/rooms/general/messages",
            headers=operator,
            json={"sender": "Customer", "content": "Resolution"},
        ).json()
        missing_operator_actor = client.put(
            f"/v1/rooms/general/messages/{message['id']}/pin",
            headers=operator,
            json={},
        )
        denied = client.put(
            f"/v1/rooms/general/messages/{message['id']}/pin",
            headers=reader,
            json={},
        )
        blind = client.put(
            f"/v1/rooms/general/messages/{message['id']}/pin",
            headers=pin_only,
            json={},
        )
        mismatch = client.put(
            f"/v1/rooms/general/messages/{message['id']}/pin",
            headers=pinner,
            json={"pinner": "someone-else"},
        )
        missing = client.put(
            "/v1/rooms/general/messages/missing/pin",
            headers=pinner,
            json={},
        )
        pinned = client.put(
            f"/v1/rooms/general/messages/{message['id']}/pin",
            headers=pinner,
            json={},
        )
        reader_list = client.get("/v1/rooms/general/messages/pins", headers=reader)
        client.patch(
            "/v1/rooms/general/members/agent-7/moderation",
            headers=operator,
            json={"muted_for_seconds": 60},
        )
        muted = client.request(
            "DELETE",
            f"/v1/rooms/general/messages/{message['id']}/pin",
            headers=pinner,
            json={},
        )
        client.patch(
            "/v1/rooms/general/members/agent-7/moderation",
            headers=operator,
            json={"muted_for_seconds": 0},
        )
        client.patch("/v1/rooms/general", headers=operator, json={"frozen": True})
        frozen = client.request(
            "DELETE",
            f"/v1/rooms/general/messages/{message['id']}/pin",
            headers=pinner,
            json={},
        )
        admin_unpin = client.request(
            "DELETE",
            f"/v1/rooms/general/messages/{message['id']}/pin",
            headers=operator,
            json={"pinner": "support-lead"},
        )
        admin_repin = client.put(
            f"/v1/rooms/general/messages/{message['id']}/pin",
            headers=operator,
            json={"pinner": "support-lead"},
        )
        client.patch("/v1/rooms/general", headers=operator, json={"frozen": False})
        client.delete(f"/v1/rooms/general/messages/{message['id']}", headers=operator)
        tombstone = client.get("/v1/rooms/general/messages", headers=operator).json()["items"][0]
        pins = client.get("/v1/rooms/general/messages/pins", headers=operator).json()["items"]
        rejected = client.put(
            f"/v1/rooms/general/messages/{message['id']}/pin",
            headers=pinner,
            json={},
        )
        client.patch("/v1/rooms/general", headers=operator, json={"archived": True})
        archived = client.put(
            f"/v1/rooms/general/messages/{message['id']}/pin",
            headers=pinner,
            json={},
        )
        audit = client.get("/v1/admin/audit-events", headers=operator).json()["items"]

    assert missing_operator_actor.status_code == 422
    assert missing_operator_actor.json()["error"]["code"] == "pinner_required"
    assert denied.status_code == 403 and denied.json()["error"]["code"] == "authorization_denied"
    assert blind.status_code == 403 and blind.json()["error"]["code"] == "authorization_denied"
    assert mismatch.status_code == 403 and mismatch.json()["error"]["code"] == "identity_mismatch"
    assert missing.status_code == 404 and missing.json()["error"]["code"] == "message_not_found"
    assert pinned.json()["pinner"] == "agent-7"
    assert [item["id"] for item in reader_list.json()["items"]] == [message["id"]]
    assert muted.status_code == 403 and muted.json()["error"]["code"] == "room_muted"
    assert frozen.status_code == 409 and frozen.json()["error"]["code"] == "room_frozen"
    assert admin_unpin.json()["changed"] is True and admin_unpin.json()["pinned"] is False
    assert admin_repin.json()["changed"] is True and admin_repin.json()["message"]["pinned_by"] == "support-lead"
    assert tombstone["content"] == ""
    assert tombstone["pinned_at"] is None and tombstone["pinned_by"] is None
    assert pins == []
    assert rejected.status_code == 409 and rejected.json()["error"]["code"] == "message_deleted"
    assert archived.status_code == 409 and archived.json()["error"]["code"] == "room_archived"
    pin_audit = next(event for event in audit if event["action"] == "message.pin.updated")
    assert pin_audit["actor"] == "agent-7"
    assert pin_audit["details"] == {"message_id": message["id"], "pinned": True}
    operator_audits = [
        event for event in audit if event["action"] == "message.pin.updated" and event["actor"] == "operator-api-key"
    ]
    assert [event["details"]["pinned"] for event in operator_audits] == [False, True]


def test_pin_cursor_must_still_identify_a_room_pin(client: TestClient, room: dict[str, str]) -> None:
    message = _message(client, "Not pinned")
    invalid = client.get("/v1/rooms/general/messages/pins", params={"before": message["id"]})
    assert invalid.status_code == 400
    assert invalid.json()["error"]["code"] == "invalid_cursor"
