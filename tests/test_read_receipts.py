"""Least-privilege participant read-receipt snapshots and realtime updates."""

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from samsarix_chat_engine import AccessTokenService, Settings, create_app

SIGNING_SECRET = "test-only-read-receipt-signing-secret"
OPERATOR_KEY = "test-only-read-receipt-operator"


@pytest.fixture
def receipt_client(tmp_path: Path) -> Iterator[tuple[TestClient, AccessTokenService]]:
    settings = Settings(
        database_path=tmp_path / "read-receipts.db",
        api_key=OPERATOR_KEY,
        token_signing_secret=SIGNING_SECRET,
        token_issuer="read-receipt-test",
        token_audience="read-receipt-client",
        token_max_lifetime_seconds=3_600,
        token_clock_skew_seconds=0,
        read_state_queries_per_minute=20,
    )
    service = AccessTokenService(
        SIGNING_SECRET,
        issuer="read-receipt-test",
        audience="read-receipt-client",
        max_lifetime_seconds=3_600,
        clock_skew_seconds=0,
    )
    with TestClient(create_app(settings)) as client:
        response = client.post(
            "/v1/rooms",
            headers={"X-API-Key": OPERATOR_KEY},
            json={"id": "support", "name": "Support"},
        )
        assert response.status_code == 201
        yield client, service


def _token(service: AccessTokenService, subject: str, *permissions: str) -> str:
    return service.issue(
        subject,
        rooms=["support"],
        permissions=list(permissions),  # type: ignore[arg-type]
        expires_in_seconds=300,
    )


def _bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _initialize_socket(websocket: Any, token: str) -> None:
    assert websocket.receive_json()["type"] == "auth.required"
    websocket.send_json({"type": "auth", "token": token})
    assert websocket.receive_json()["type"] == "ready"
    assert websocket.receive_json()["type"] == "history"


def test_receipt_query_is_explicit_ordered_and_least_privilege(
    receipt_client: tuple[TestClient, AccessTokenService],
) -> None:
    client, service = receipt_client
    alice = _token(service, "alice", "room:read", "room:write")
    bob = _token(service, "bob", "room:read", "room:write")
    viewer = _token(service, "agent", "room:read", "room:read-receipts")
    ordinary_reader = _token(service, "reader", "room:read")

    message = client.post(
        "/v1/rooms/support/messages",
        headers=_bearer(bob),
        json={"content": "The issue is resolved"},
    ).json()
    marked = client.put(
        "/v1/rooms/support/read-state",
        headers=_bearer(alice),
        json={"message_id": message["id"]},
    )
    assert marked.status_code == 200

    forbidden = client.post(
        "/v1/rooms/support/read-receipts/query",
        headers=_bearer(ordinary_reader),
        json={"subjects": ["alice"]},
    )
    assert forbidden.status_code == 403
    assert forbidden.json()["error"]["code"] == "authorization_denied"

    response = client.post(
        "/v1/rooms/support/read-receipts/query",
        headers=_bearer(viewer),
        json={"subjects": ["unknown", "alice", "bob"]},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["room_id"] == "support"
    assert [item["subject"] for item in body["items"]] == ["unknown", "alice", "bob"]
    assert body["items"][0] == {
        "subject": "unknown",
        "last_read_message_id": None,
        "last_read_message_at": None,
        "last_read_at": None,
    }
    assert body["items"][1]["last_read_message_id"] == message["id"]
    assert body["items"][1]["last_read_message_at"] == message["created_at"]
    assert body["items"][1]["last_read_at"] is not None
    assert body["items"][2]["last_read_message_id"] is None

    operator = client.post(
        "/v1/rooms/support/read-receipts/query",
        headers={"X-API-Key": OPERATOR_KEY},
        json={"subjects": ["alice"]},
    )
    assert operator.status_code == 200


@pytest.mark.parametrize(
    "subjects",
    [[], ["alice", "alice"], [" alice"], ["x" * 65], [str(index) for index in range(101)]],
)
def test_receipt_query_rejects_unbounded_or_ambiguous_subjects(
    receipt_client: tuple[TestClient, AccessTokenService],
    subjects: list[str],
) -> None:
    client, service = receipt_client
    viewer = _token(service, "agent", "room:read", "room:read-receipts")
    response = client.post(
        "/v1/rooms/support/read-receipts/query",
        headers=_bearer(viewer),
        json={"subjects": subjects},
    )
    assert response.status_code == 422


def test_changed_read_state_emits_authorized_realtime_updates_and_clear(
    receipt_client: tuple[TestClient, AccessTokenService],
) -> None:
    client, service = receipt_client
    alice = _token(service, "alice", "room:read", "room:write")
    viewer = _token(service, "agent", "room:read", "room:read-receipts")
    message = client.post(
        "/v1/rooms/support/messages",
        headers=_bearer(alice),
        json={"content": "Please review"},
    ).json()

    with client.websocket_connect("/v1/rooms/support/ws") as websocket:
        _initialize_socket(websocket, viewer)
        marked = client.put(
            "/v1/rooms/support/read-state",
            headers=_bearer(alice),
            json={"message_id": message["id"]},
        )
        assert marked.status_code == 200
        event = websocket.receive_json()
        assert event["type"] == "read.updated"
        assert event["receipt"]["subject"] == "alice"
        assert event["receipt"]["last_read_message_id"] == message["id"]
        assert event["receipt"]["last_read_message_at"] == message["created_at"]
        assert event["receipt"]["last_read_at"] is not None

        assert client.delete("/v1/rooms/support/read-state", headers=_bearer(alice)).status_code == 204
        cleared = websocket.receive_json()
        assert cleared == {
            "type": "read.updated",
            "receipt": {
                "subject": "alice",
                "last_read_message_id": None,
                "last_read_message_at": None,
                "last_read_at": None,
            },
        }


def test_receipt_queries_and_updates_have_independent_bounded_budgets(tmp_path: Path) -> None:
    settings = Settings(
        database_path=tmp_path / "receipt-rate.db",
        api_key=OPERATOR_KEY,
        token_signing_secret=SIGNING_SECRET,
        token_issuer="read-receipt-test",
        token_audience="read-receipt-client",
        read_state_queries_per_minute=1,
    )
    service = AccessTokenService(
        SIGNING_SECRET,
        issuer="read-receipt-test",
        audience="read-receipt-client",
    )
    with TestClient(create_app(settings)) as client:
        client.post(
            "/v1/rooms",
            headers={"X-API-Key": OPERATOR_KEY},
            json={"id": "support", "name": "Support"},
        )
        token = _token(service, "alice", "room:read", "room:read-receipts")
        headers = _bearer(token)
        assert (
            client.post(
                "/v1/rooms/support/read-receipts/query", headers=headers, json={"subjects": ["alice"]}
            ).status_code
            == 200
        )
        query_limited = client.post(
            "/v1/rooms/support/read-receipts/query", headers=headers, json={"subjects": ["alice"]}
        )
        assert query_limited.status_code == 429
        assert query_limited.json()["error"]["code"] == "read_receipt_query_rate_limit_exceeded"

        assert client.put("/v1/rooms/support/read-state", headers=headers, json={}).status_code == 200
        update_limited = client.delete("/v1/rooms/support/read-state", headers=headers)
        assert update_limited.status_code == 429
        assert update_limited.json()["error"]["code"] == "read_receipt_update_rate_limit_exceeded"
