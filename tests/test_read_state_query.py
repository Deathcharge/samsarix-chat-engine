"""Bounded cross-room read-state query coverage."""

from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from samsarix_chat_engine import AccessTokenService, Settings, create_app

SIGNING_SECRET = "test-only-inbox-signing-secret-that-is-long-enough"
OPERATOR_KEY = "test-only-inbox-operator-key"
ROOM_IDS = ("support", "incident", "empty")


@pytest.fixture
def inbox_client(tmp_path: Path) -> Iterator[tuple[TestClient, AccessTokenService]]:
    settings = Settings(
        database_path=tmp_path / "inbox.db",
        api_key=OPERATOR_KEY,
        token_signing_secret=SIGNING_SECRET,
        token_issuer="inbox-test-issuer",
        token_audience="inbox-test-audience",
        read_state_queries_per_minute=20,
    )
    service = AccessTokenService(
        SIGNING_SECRET,
        issuer="inbox-test-issuer",
        audience="inbox-test-audience",
    )
    with TestClient(create_app(settings)) as client:
        for room_id in ROOM_IDS:
            response = client.post(
                "/v1/rooms",
                headers={"X-API-Key": OPERATOR_KEY},
                json={"id": room_id, "name": room_id.title()},
            )
            assert response.status_code == 201
        yield client, service


def _token(service: AccessTokenService, subject: str, rooms: tuple[str, ...] = ROOM_IDS) -> str:
    return service.issue(
        subject,
        rooms=rooms,
        permissions=["room:read", "room:write"],
        expires_in_seconds=300,
    )


def _bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _post_message(client: TestClient, token: str, room_id: str, content: str) -> dict[str, object]:
    response = client.post(
        f"/v1/rooms/{room_id}/messages",
        headers=_bearer(token),
        json={"content": content},
    )
    assert response.status_code == 201
    return response.json()


def test_query_read_states_returns_ordered_content_free_inbox_state(
    inbox_client: tuple[TestClient, AccessTokenService],
) -> None:
    client, service = inbox_client
    alice = _token(service, "alice")
    bob = _token(service, "bob")

    _post_message(client, alice, "support", "Alice's own message")
    first_support = _post_message(client, bob, "support", "First support reply")
    incident = _post_message(client, bob, "incident", "Incident update")
    marked = client.put(
        "/v1/rooms/support/read-state",
        headers=_bearer(alice),
        json={"message_id": first_support["id"]},
    )
    assert marked.status_code == 200
    latest_support = _post_message(client, bob, "support", "Latest support reply")

    response = client.post(
        "/v1/read-states/query",
        headers=_bearer(alice),
        json={"room_ids": ["incident", "support", "empty"]},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["subject"] == "alice"
    assert body["total_unread_count"] == 2
    assert body["unread_room_count"] == 2
    assert [item["room_id"] for item in body["items"]] == ["incident", "support", "empty"]

    incident_state, support_state, empty_state = body["items"]
    assert incident_state == {
        "room_id": "incident",
        "last_read_message_id": None,
        "last_read_at": None,
        "unread_count": 1,
        "latest_message_id": incident["id"],
        "latest_message_at": incident["created_at"],
    }
    assert support_state["last_read_message_id"] == first_support["id"]
    assert support_state["last_read_at"] is not None
    assert support_state["unread_count"] == 1
    assert support_state["latest_message_id"] == latest_support["id"]
    assert support_state["latest_message_at"] == latest_support["created_at"]
    assert empty_state == {
        "room_id": "empty",
        "last_read_message_id": None,
        "last_read_at": None,
        "unread_count": 0,
        "latest_message_id": None,
        "latest_message_at": None,
    }
    assert all("content" not in item and "sender" not in item and "subject" not in item for item in body["items"])

    deleted = client.delete(
        f"/v1/rooms/incident/messages/{incident['id']}",
        headers=_bearer(bob),
    )
    assert deleted.status_code == 204
    after_delete = client.post(
        "/v1/read-states/query",
        headers=_bearer(alice),
        json={"room_ids": ["incident"]},
    ).json()
    assert after_delete["total_unread_count"] == 0
    assert after_delete["unread_room_count"] == 0
    assert after_delete["items"][0]["latest_message_id"] is None
    assert after_delete["items"][0]["latest_message_at"] is None


def test_query_read_states_validates_identity_authorization_rooms_and_bans(
    inbox_client: tuple[TestClient, AccessTokenService],
) -> None:
    client, service = inbox_client
    support_only = _token(service, "alice", ("support",))

    operator = client.post(
        "/v1/read-states/query",
        headers={"X-API-Key": OPERATOR_KEY},
        json={"room_ids": ["support"]},
    )
    assert operator.status_code == 403
    assert operator.json()["error"]["code"] == "stable_subject_required"

    unauthorized = client.post(
        "/v1/read-states/query",
        headers=_bearer(support_only),
        json={"room_ids": ["support", "incident"]},
    )
    assert unauthorized.status_code == 403
    assert unauthorized.json()["error"]["code"] == "authorization_denied"

    unknown_token = _token(service, "alice", ("support", "missing-room"))
    unknown = client.post(
        "/v1/read-states/query",
        headers=_bearer(unknown_token),
        json={"room_ids": ["support", "missing-room"]},
    )
    assert unknown.status_code == 404
    assert unknown.json()["error"]["code"] == "room_not_found"

    all_rooms = _token(service, "alice")
    banned = client.patch(
        "/v1/rooms/incident/members/alice/moderation",
        headers={"X-API-Key": OPERATOR_KEY},
        json={"banned_for_seconds": 60},
    )
    assert banned.status_code == 200
    denied = client.post(
        "/v1/read-states/query",
        headers=_bearer(all_rooms),
        json={"room_ids": ["support", "incident"]},
    )
    assert denied.status_code == 403
    assert denied.json()["error"]["code"] == "room_banned"


@pytest.mark.parametrize(
    "payload",
    [
        {"room_ids": []},
        {"room_ids": ["support", "support"]},
        {"room_ids": ["Support"]},
        {"room_ids": [f"room-{index}" for index in range(101)]},
        {"room_ids": ["support"], "unexpected": True},
    ],
)
def test_query_read_states_rejects_invalid_bodies(
    inbox_client: tuple[TestClient, AccessTokenService],
    payload: dict[str, object],
) -> None:
    client, service = inbox_client
    response = client.post(
        "/v1/read-states/query",
        headers=_bearer(_token(service, "alice")),
        json=payload,
    )
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "invalid_request"


def test_query_read_states_has_a_dedicated_subject_rate_limit(tmp_path: Path) -> None:
    settings = Settings(
        database_path=tmp_path / "inbox-rate.db",
        api_key=OPERATOR_KEY,
        token_signing_secret=SIGNING_SECRET,
        token_issuer="inbox-test-issuer",
        token_audience="inbox-test-audience",
        max_read_states_per_room=1,
        read_state_queries_per_minute=1,
    )
    service = AccessTokenService(
        SIGNING_SECRET,
        issuer="inbox-test-issuer",
        audience="inbox-test-audience",
    )
    with TestClient(create_app(settings)) as client:
        assert (
            client.post(
                "/v1/rooms",
                headers={"X-API-Key": OPERATOR_KEY},
                json={"id": "support", "name": "Support"},
            ).status_code
            == 201
        )
        token = _token(service, "alice", ("support",))
        payload = {"room_ids": ["support"]}
        assert client.post("/v1/read-states/query", headers=_bearer(token), json=payload).status_code == 200
        bob = _token(service, "bob", ("support",))
        assert client.put("/v1/rooms/support/read-state", headers=_bearer(bob), json={}).status_code == 200
        limited = client.post("/v1/read-states/query", headers=_bearer(token), json=payload)
        assert limited.status_code == 429
        assert limited.headers["Retry-After"] == "60"
        assert limited.json()["error"]["code"] == "read_state_query_rate_limit_exceeded"
