# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""End-to-end coverage for bounded durable message reactions."""

from fastapi.testclient import TestClient

from samsarix_chat_engine import Settings, create_app
from samsarix_chat_engine.auth import AccessTokenService


def _message(client: TestClient) -> dict[str, object]:
    response = client.post(
        "/v1/rooms/general/messages",
        json={"sender": "Author", "content": "Please acknowledge"},
    )
    assert response.status_code == 201
    return response.json()


def _reaction(client: TestClient, message_id: object, key: str, reactor: str, *, method: str = "PUT"):
    return client.request(
        method,
        f"/v1/rooms/general/messages/{message_id}/reactions/{key}",
        json={"reactor": reactor},
    )


def test_reactions_are_idempotent_grouped_realtime_and_persistent(
    client: TestClient,
    room: dict[str, str],
) -> None:
    message = _message(client)
    with client.websocket_connect("/v1/rooms/general/ws?username=Observer") as websocket:
        websocket.receive_json()
        websocket.receive_json()

        added = _reaction(client, message["id"], "ack", "alice")
        event = websocket.receive_json()
        replay = _reaction(client, message["id"], "ack", "alice")
        second = _reaction(client, message["id"], "ack", "bob")
        second_event = websocket.receive_json()
        resolved = _reaction(client, message["id"], "resolved", "alice")
        websocket.receive_json()
        removed = _reaction(client, message["id"], "ack", "alice", method="DELETE")
        removed_event = websocket.receive_json()

    assert added.status_code == 200
    assert added.json()["changed"] is True
    assert added.json()["present"] is True
    assert added.json()["message"]["reactions"] == [{"key": "ack", "count": 1}]
    assert event["type"] == "message.reaction.updated"
    assert event["reactor"] == "alice"
    assert event["message"] == added.json()["message"]
    assert replay.json()["changed"] is False
    assert second.json()["message"]["reactions"] == [{"key": "ack", "count": 2}]
    assert second_event["reactor"] == "bob"
    assert resolved.json()["message"]["reactions"] == [
        {"key": "ack", "count": 2},
        {"key": "resolved", "count": 1},
    ]
    assert removed.json()["present"] is False
    assert removed.json()["message"]["reactions"] == [
        {"key": "ack", "count": 1},
        {"key": "resolved", "count": 1},
    ]
    assert removed_event["present"] is False
    history = client.get("/v1/rooms/general/messages").json()["items"]
    assert history[0]["reactions"] == removed.json()["message"]["reactions"]


def test_reaction_identity_validation_and_tombstone_privacy(tmp_path) -> None:
    secret = "reaction-test-signing-secret-that-is-long-enough"
    settings = Settings(
        database_path=tmp_path / "reaction-auth.db",
        api_key="reaction-test-operator-key",
        token_signing_secret=secret,
        token_issuer="reaction-tests",
        token_audience="reaction-client",
    )
    tokens = AccessTokenService(secret, issuer="reaction-tests", audience="reaction-client")
    operator = {"X-API-Key": "reaction-test-operator-key"}
    alice = tokens.issue(
        "alice",
        rooms=["general"],
        permissions=["room:read", "room:write"],
        expires_in_seconds=300,
    )
    signed = {"Authorization": f"Bearer {alice}"}
    with TestClient(create_app(settings)) as client:
        client.post("/v1/rooms", headers=operator, json={"id": "general", "name": "General"})
        message = client.post(
            "/v1/rooms/general/messages",
            headers=operator,
            json={"sender": "Operator", "content": "Status"},
        ).json()
        mismatch = client.put(
            f"/v1/rooms/general/messages/{message['id']}/reactions/ack",
            headers=signed,
            json={"reactor": "bob"},
        )
        added = client.put(
            f"/v1/rooms/general/messages/{message['id']}/reactions/ack",
            headers=signed,
            json={},
        )
        client.delete(
            f"/v1/rooms/general/messages/{message['id']}",
            headers=operator,
        )
        tombstone = client.get("/v1/rooms/general/messages", headers=operator).json()["items"][0]
        rejected = client.put(
            f"/v1/rooms/general/messages/{message['id']}/reactions/ack",
            headers=signed,
            json={},
        )

    assert mismatch.status_code == 403
    assert mismatch.json()["error"]["code"] == "identity_mismatch"
    assert added.json()["reactor"] == "alice"
    assert tombstone["content"] == ""
    assert tombstone["reactions"] == []
    assert rejected.status_code == 409
    assert rejected.json()["error"]["code"] == "message_deleted"


def test_reaction_key_and_distinct_key_caps_are_enforced(tmp_path) -> None:
    settings = Settings(
        database_path=tmp_path / "reaction-cap.db",
        messages_per_minute=100,
        max_stored_messages=100,
        max_stored_messages_per_room=100,
    )
    with TestClient(create_app(settings)) as client:
        client.post("/v1/rooms", json={"id": "general", "name": "General"})
        message = _message(client)
        invalid = _reaction(client, message["id"], "Not Valid", "alice")
        for index in range(20):
            result = _reaction(client, message["id"], f"k{index}", f"user-{index}")
            assert result.status_code == 200
        capacity = _reaction(client, message["id"], "overflow", "alice")
        existing_key = _reaction(client, message["id"], "k0", "late-user")

    assert invalid.status_code == 422
    assert invalid.json()["error"]["code"] == "invalid_request"
    assert capacity.status_code == 409
    assert capacity.json()["error"]["code"] == "reaction_capacity_reached"
    assert existing_key.status_code == 200
    assert existing_key.json()["message"]["reactions"][0] == {"key": "k0", "count": 2}
