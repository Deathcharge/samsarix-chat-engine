# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""End-to-end coverage for bounded one-depth message threads."""

from fastapi.testclient import TestClient

from samsarix_chat_engine import Settings, create_app
from samsarix_chat_engine.auth import AccessTokenService


def _post(client: TestClient, content: str, **extra: str) -> dict[str, object]:
    response = client.post(
        "/v1/rooms/general/messages",
        json={"sender": "Author", "content": content, **extra},
    )
    assert response.status_code == 201
    return response.json()


def test_http_thread_creation_pagination_and_depth_errors(client: TestClient, room: dict[str, str]) -> None:
    parent = _post(client, "Question")
    first = _post(client, "First answer", parent_message_id=str(parent["id"]))
    second = _post(client, "Second answer", parent_message_id=str(parent["id"]))

    assert parent["parent_message_id"] is None
    assert first["parent_message_id"] == parent["id"]
    assert second["parent_message_id"] == parent["id"]

    newest = client.get(
        f"/v1/rooms/general/messages/{parent['id']}/replies",
        params={"limit": 1},
    )
    assert newest.status_code == 200
    assert [item["content"] for item in newest.json()["items"]] == ["Second answer"]
    assert newest.json()["next_before"] == second["id"]

    older = client.get(
        f"/v1/rooms/general/messages/{parent['id']}/replies",
        params={"limit": 1, "before": newest.json()["next_before"]},
    )
    assert [item["content"] for item in older.json()["items"]] == ["First answer"]
    assert older.json()["next_before"] is None

    nested = client.post(
        "/v1/rooms/general/messages",
        json={"sender": "Author", "content": "Nested", "parent_message_id": first["id"]},
    )
    reply_collection = client.get(f"/v1/rooms/general/messages/{first['id']}/replies")
    missing_parent = client.post(
        "/v1/rooms/general/messages",
        json={"sender": "Author", "content": "Missing", "parent_message_id": "unknown"},
    )
    invalid_cursor = client.get(
        f"/v1/rooms/general/messages/{parent['id']}/replies",
        params={"before": parent["id"]},
    )

    assert nested.status_code == 409
    assert nested.json()["error"]["code"] == "thread_depth_exceeded"
    assert reply_collection.status_code == 409
    assert reply_collection.json()["error"]["code"] == "thread_depth_exceeded"
    assert missing_parent.status_code == 404
    assert missing_parent.json()["error"]["code"] == "parent_message_not_found"
    assert invalid_cursor.status_code == 400
    assert invalid_cursor.json()["error"]["code"] == "invalid_cursor"

    history = client.get("/v1/rooms/general/messages").json()["items"]
    assert [item["id"] for item in history] == [parent["id"], first["id"], second["id"]]
    matches = client.get("/v1/rooms/general/messages/search", params={"q": "answer"}).json()["items"]
    assert [item["id"] for item in matches] == [first["id"], second["id"]]


def test_thread_parent_is_room_scoped(client: TestClient, room: dict[str, str]) -> None:
    client.post("/v1/rooms", json={"id": "other", "name": "Other"})
    other_parent = client.post(
        "/v1/rooms/other/messages",
        json={"sender": "Author", "content": "Other room"},
    ).json()

    create = client.post(
        "/v1/rooms/general/messages",
        json={"sender": "Author", "content": "Cross-room", "parent_message_id": other_parent["id"]},
    )
    listing = client.get(f"/v1/rooms/general/messages/{other_parent['id']}/replies")

    assert create.status_code == 404
    assert create.json()["error"]["code"] == "parent_message_not_found"
    assert listing.status_code == 404
    assert listing.json()["error"]["code"] == "parent_message_not_found"


def test_deleted_parent_preserves_existing_replies_and_idempotent_replay(
    client: TestClient,
    room: dict[str, str],
) -> None:
    parent = _post(client, "Original")
    created = client.post(
        "/v1/rooms/general/messages",
        headers={"Idempotency-Key": "reply-1"},
        json={"sender": "Author", "content": "Existing reply", "parent_message_id": parent["id"]},
    )
    assert created.status_code == 201
    assert client.delete(f"/v1/rooms/general/messages/{parent['id']}").status_code == 204

    rejected = client.post(
        "/v1/rooms/general/messages",
        json={"sender": "Author", "content": "Too late", "parent_message_id": parent["id"]},
    )
    replay = client.post(
        "/v1/rooms/general/messages",
        headers={"Idempotency-Key": "reply-1"},
        json={"sender": "Author", "content": "Ignored", "parent_message_id": parent["id"]},
    )
    replies = client.get(f"/v1/rooms/general/messages/{parent['id']}/replies")

    assert rejected.status_code == 409
    assert rejected.json()["error"]["code"] == "parent_message_deleted"
    assert replay.status_code == 200
    assert replay.json() == created.json()
    assert [item["content"] for item in replies.json()["items"]] == ["Existing reply"]


def test_websocket_reply_is_broadcast_and_recovered(client: TestClient, room: dict[str, str]) -> None:
    parent = _post(client, "WebSocket parent")
    with client.websocket_connect("/v1/rooms/general/ws?username=Writer") as websocket:
        websocket.receive_json()
        websocket.receive_json()
        websocket.send_json(
            {
                "type": "message",
                "content": "Socket reply",
                "parent_message_id": parent["id"],
                "client_message_id": "socket-reply",
            }
        )
        created = websocket.receive_json()

    assert created["type"] == "message.created"
    assert created["message"]["parent_message_id"] == parent["id"]
    replies = client.get(f"/v1/rooms/general/messages/{parent['id']}/replies").json()
    assert [item["id"] for item in replies["items"]] == [created["message"]["id"]]


def test_websocket_invalid_parent_is_non_terminal(client: TestClient, room: dict[str, str]) -> None:
    with client.websocket_connect("/v1/rooms/general/ws?username=Writer") as websocket:
        websocket.receive_json()
        websocket.receive_json()
        websocket.send_json({"type": "message", "content": "Reply", "parent_message_id": "missing"})
        error = websocket.receive_json()
        websocket.send_json({"type": "message", "content": "Still connected"})
        created = websocket.receive_json()

    assert error["type"] == "error"
    assert error["code"] == "parent_message_not_found"
    assert created["type"] == "message.created"
    assert created["message"]["parent_message_id"] is None


def test_reply_listing_enforces_room_read_authorization(tmp_path) -> None:
    secret = "thread-test-signing-secret-that-is-long-enough"
    settings = Settings(
        database_path=tmp_path / "thread-auth.db",
        api_key="thread-test-operator-key",
        token_signing_secret=secret,
        token_issuer="thread-tests",
        token_audience="thread-client",
    )
    tokens = AccessTokenService(secret, issuer="thread-tests", audience="thread-client")
    operator = {"X-API-Key": "thread-test-operator-key"}
    with TestClient(create_app(settings)) as client:
        client.post("/v1/rooms", headers=operator, json={"id": "general", "name": "General"})
        parent = client.post(
            "/v1/rooms/general/messages",
            headers=operator,
            json={"sender": "Operator", "content": "Parent"},
        ).json()
        reader = tokens.issue(
            "reader",
            rooms=["general"],
            permissions=["room:read"],
            expires_in_seconds=300,
        )
        other_room = tokens.issue(
            "outsider",
            rooms=["other"],
            permissions=["room:read"],
            expires_in_seconds=300,
        )
        allowed = client.get(
            f"/v1/rooms/general/messages/{parent['id']}/replies",
            headers={"Authorization": f"Bearer {reader}"},
        )
        denied = client.get(
            f"/v1/rooms/general/messages/{parent['id']}/replies",
            headers={"Authorization": f"Bearer {other_room}"},
        )

    assert allowed.status_code == 200
    assert denied.status_code == 403
    assert denied.json()["error"]["code"] == "authorization_denied"


def test_retention_promotes_reply_when_parent_expires(tmp_path) -> None:
    settings = Settings(
        database_path=tmp_path / "thread-cap.db",
        max_stored_messages=2,
        max_stored_messages_per_room=2,
    )
    with TestClient(create_app(settings)) as client:
        client.post("/v1/rooms", json={"id": "general", "name": "General"})
        parent = _post(client, "Parent")
        reply = _post(client, "Reply", parent_message_id=str(parent["id"]))
        _post(client, "Newest")
        history = client.get("/v1/rooms/general/messages").json()["items"]
        missing_thread = client.get(f"/v1/rooms/general/messages/{parent['id']}/replies")

    retained_reply = next(item for item in history if item["id"] == reply["id"])
    assert retained_reply["parent_message_id"] is None
    assert missing_thread.status_code == 404
