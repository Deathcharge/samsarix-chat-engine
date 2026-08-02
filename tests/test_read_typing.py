"""Signed read-state and bounded ephemeral typing workflow coverage."""

from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from samsarix_chat_engine import AccessTokenService, Settings, create_app

SIGNING_SECRET = "test-only-signing-secret-that-is-long-enough"
OPERATOR_KEY = "test-only-operator-api-key"


@pytest.fixture
def workflow_client(tmp_path: Path) -> Iterator[tuple[TestClient, AccessTokenService]]:
    settings = Settings(
        database_path=tmp_path / "application-workflows.db",
        api_key=OPERATOR_KEY,
        token_signing_secret=SIGNING_SECRET,
        token_issuer="workflow-test-issuer",
        token_audience="workflow-test-audience",
        token_max_lifetime_seconds=3_600,
        token_clock_skew_seconds=0,
        typing_timeout_seconds=1,
        typing_events_per_minute=20,
        max_read_states_per_room=10,
    )
    service = AccessTokenService(
        SIGNING_SECRET,
        issuer="workflow-test-issuer",
        audience="workflow-test-audience",
        max_lifetime_seconds=3_600,
        clock_skew_seconds=0,
    )
    with TestClient(create_app(settings)) as client:
        created = client.post(
            "/v1/rooms",
            headers={"X-API-Key": OPERATOR_KEY},
            json={"id": "support", "name": "Support"},
        )
        assert created.status_code == 201
        yield client, service


def _token(service: AccessTokenService, subject: str, *, write: bool = True) -> str:
    permissions = ["room:read", "room:write"] if write else ["room:read"]
    return service.issue(subject, rooms=["support"], permissions=permissions, expires_in_seconds=300)


def _bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def test_read_state_is_subject_scoped_monotonic_and_self_clearable(
    workflow_client: tuple[TestClient, AccessTokenService],
) -> None:
    client, service = workflow_client
    alice = _token(service, "alice")
    bob = _token(service, "bob")

    assert client.get("/v1/rooms/support/read-state", headers=_bearer(alice)).json()["unread_count"] == 0
    alice_message = client.post(
        "/v1/rooms/support/messages", headers=_bearer(alice), json={"content": "Need help"}
    ).json()
    assert client.get("/v1/rooms/support/read-state", headers=_bearer(alice)).json()["unread_count"] == 0
    assert client.get("/v1/rooms/support/read-state", headers=_bearer(bob)).json()["unread_count"] == 1

    bob_message = client.post("/v1/rooms/support/messages", headers=_bearer(bob), json={"content": "I can help"}).json()
    before = client.get("/v1/rooms/support/read-state", headers=_bearer(alice)).json()
    assert before["unread_count"] == 1

    marked = client.put(
        "/v1/rooms/support/read-state",
        headers=_bearer(alice),
        json={"message_id": bob_message["id"]},
    ).json()
    assert marked["subject"] == "alice"
    assert marked["last_read_message_id"] == bob_message["id"]
    assert marked["last_read_at"] is not None
    assert marked["unread_count"] == 0

    later_bob_message = client.post(
        "/v1/rooms/support/messages", headers=_bearer(bob), json={"content": "Temporary follow-up"}
    ).json()
    assert client.get("/v1/rooms/support/read-state", headers=_bearer(alice)).json()["unread_count"] == 1
    assert (
        client.delete(
            f"/v1/rooms/support/messages/{later_bob_message['id']}",
            headers=_bearer(bob),
        ).status_code
        == 204
    )
    assert client.get("/v1/rooms/support/read-state", headers=_bearer(alice)).json()["unread_count"] == 0

    regressed = client.put(
        "/v1/rooms/support/read-state",
        headers=_bearer(alice),
        json={"message_id": alice_message["id"]},
    ).json()
    assert regressed["last_read_message_id"] == bob_message["id"]

    assert client.delete("/v1/rooms/support/read-state", headers=_bearer(alice)).status_code == 204
    cleared = client.get("/v1/rooms/support/read-state", headers=_bearer(alice)).json()
    assert cleared["last_read_message_id"] is None
    assert cleared["last_read_at"] is None
    assert cleared["unread_count"] == 1


def test_read_cursor_rejects_a_message_from_another_room(
    workflow_client: tuple[TestClient, AccessTokenService],
) -> None:
    client, service = workflow_client
    assert (
        client.post(
            "/v1/rooms",
            headers={"X-API-Key": OPERATOR_KEY},
            json={"id": "other", "name": "Other"},
        ).status_code
        == 201
    )
    token = service.issue(
        "alice",
        rooms=["support", "other"],
        permissions=["room:read", "room:write"],
        expires_in_seconds=300,
    )
    message = client.post(
        "/v1/rooms/other/messages",
        headers=_bearer(token),
        json={"content": "Other-room message"},
    ).json()

    response = client.put(
        "/v1/rooms/support/read-state",
        headers=_bearer(token),
        json={"message_id": message["id"]},
    )
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "message_not_found"


def test_read_state_requires_stable_identity_and_enforces_capacity(tmp_path: Path) -> None:
    settings = Settings(
        database_path=tmp_path / "read-cap.db",
        api_key=OPERATOR_KEY,
        token_signing_secret=SIGNING_SECRET,
        token_issuer="workflow-test-issuer",
        token_audience="workflow-test-audience",
        max_read_states_per_room=1,
    )
    service = AccessTokenService(
        SIGNING_SECRET,
        issuer="workflow-test-issuer",
        audience="workflow-test-audience",
    )
    with TestClient(create_app(settings)) as client:
        client.post("/v1/rooms", headers={"X-API-Key": OPERATOR_KEY}, json={"id": "support", "name": "Support"})
        operator = client.get("/v1/rooms/support/read-state", headers={"X-API-Key": OPERATOR_KEY})
        assert operator.status_code == 403
        assert operator.json()["error"]["code"] == "stable_subject_required"

        first = _token(service, "first")
        second = _token(service, "second")
        assert client.put("/v1/rooms/support/read-state", headers=_bearer(first), json={}).status_code == 200
        rejected = client.put("/v1/rooms/support/read-state", headers=_bearer(second), json={})
        assert rejected.status_code == 507
        assert rejected.json()["error"]["code"] == "read_state_capacity_reached"


def test_typing_is_transition_only_auto_expires_and_stops_on_publish(
    workflow_client: tuple[TestClient, AccessTokenService],
) -> None:
    client, service = workflow_client
    alice = _token(service, "alice")
    bob = _token(service, "bob")

    with client.websocket_connect("/v1/rooms/support/ws") as alice_socket:
        alice_socket.receive_json()
        alice_socket.send_json({"type": "auth", "token": alice})
        alice_socket.receive_json()
        alice_socket.receive_json()
        with client.websocket_connect("/v1/rooms/support/ws") as bob_socket:
            bob_socket.receive_json()
            bob_socket.send_json({"type": "auth", "token": bob})
            bob_socket.receive_json()
            bob_socket.receive_json()
            assert alice_socket.receive_json()["type"] == "presence.joined"

            alice_socket.send_json({"type": "typing", "active": True})
            started = bob_socket.receive_json()
            assert started == {"type": "typing.started", "username": "alice", "expires_in": 1.0}
            stopped = bob_socket.receive_json()
            assert stopped == {"type": "typing.stopped", "username": "alice"}

            alice_socket.send_json({"type": "typing", "active": True})
            assert bob_socket.receive_json()["type"] == "typing.started"
            alice_socket.send_json({"type": "message", "content": "Here are the details"})
            assert bob_socket.receive_json() == {"type": "typing.stopped", "username": "alice"}
            assert bob_socket.receive_json()["type"] == "message.created"
            assert alice_socket.receive_json()["type"] == "message.created"


def test_read_only_tokens_cannot_emit_typing(workflow_client: tuple[TestClient, AccessTokenService]) -> None:
    client, service = workflow_client
    read_only = _token(service, "reader", write=False)
    with client.websocket_connect("/v1/rooms/support/ws") as websocket:
        websocket.receive_json()
        websocket.send_json({"type": "auth", "token": read_only})
        websocket.receive_json()
        websocket.receive_json()
        websocket.send_json({"type": "typing", "active": True})
        error = websocket.receive_json()
        assert error["type"] == "error"
        assert error["code"] == "authorization_denied"


def test_frozen_and_muted_members_cannot_emit_typing(
    workflow_client: tuple[TestClient, AccessTokenService],
) -> None:
    client, service = workflow_client
    alice = _token(service, "alice")
    with client.websocket_connect("/v1/rooms/support/ws") as websocket:
        websocket.receive_json()
        websocket.send_json({"type": "auth", "token": alice})
        websocket.receive_json()
        websocket.receive_json()

        assert (
            client.patch(
                "/v1/rooms/support",
                headers={"X-API-Key": OPERATOR_KEY},
                json={"frozen": True},
            ).status_code
            == 200
        )
        assert websocket.receive_json()["type"] == "room.frozen"
        websocket.send_json({"type": "typing", "active": True})
        assert websocket.receive_json()["code"] == "room_frozen"

        assert (
            client.patch(
                "/v1/rooms/support",
                headers={"X-API-Key": OPERATOR_KEY},
                json={"frozen": False},
            ).status_code
            == 200
        )
        assert websocket.receive_json()["type"] == "room.unfrozen"
        assert (
            client.patch(
                "/v1/rooms/support/members/alice/moderation",
                headers={"X-API-Key": OPERATOR_KEY},
                json={"muted_for_seconds": 60},
            ).status_code
            == 200
        )
        websocket.send_json({"type": "typing", "active": True})
        assert websocket.receive_json()["code"] == "room_muted"
