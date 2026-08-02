# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Authorized, bounded message-search behavior."""

from pathlib import Path

from fastapi.testclient import TestClient

from samsarix_chat_engine import AccessTokenService, Settings, create_app

SIGNING_SECRET = "search-tests-use-a-secret-long-enough-for-hmac"


def _create_message(client: TestClient, content: str) -> dict[str, object]:
    response = client.post("/v1/rooms/support/messages", json={"sender": "agent", "content": content})
    assert response.status_code == 201
    return response.json()


def test_search_is_unicode_aware_paginated_and_tracks_current_message_state(
    client: TestClient,
) -> None:
    assert client.post("/v1/rooms", json={"id": "support", "name": "Support"}).status_code == 201
    first = _create_message(client, "ＰＡＹＭＥＮＴ failed for Café account")
    _create_message(client, "Customer supplied a shipping address")
    third = _create_message(client, "Payment retry is pending")
    fourth = _create_message(client, "PAYMENT resolved")

    first_page = client.get("/v1/rooms/support/messages/search", params={"q": "payment", "limit": 2})
    assert first_page.status_code == 200
    assert [item["id"] for item in first_page.json()["items"]] == [third["id"], fourth["id"]]
    assert first_page.json()["next_before"] == third["id"]

    older = client.get(
        "/v1/rooms/support/messages/search",
        params={"q": "CAFÉ", "limit": 2, "before": first_page.json()["next_before"]},
    )
    assert [item["id"] for item in older.json()["items"]] == [first["id"]]
    assert older.json()["next_before"] is None

    updated = client.patch(
        f"/v1/rooms/support/messages/{first['id']}", json={"content": "Invoice settled without retry"}
    )
    deleted = client.delete(f"/v1/rooms/support/messages/{third['id']}")
    remaining = client.get("/v1/rooms/support/messages/search", params={"q": "payment"})

    assert updated.status_code == 200
    assert deleted.status_code == 204
    assert [item["id"] for item in remaining.json()["items"]] == [fourth["id"]]


def test_search_validates_queries_cursors_and_room(client: TestClient) -> None:
    assert client.post("/v1/rooms", json={"id": "support", "name": "Support"}).status_code == 201
    whitespace = client.get("/v1/rooms/support/messages/search", params={"q": "   "})
    too_short = client.get("/v1/rooms/support/messages/search", params={"q": "x"})
    invalid_cursor = client.get("/v1/rooms/support/messages/search", params={"q": "valid", "before": "unknown"})
    missing_room = client.get("/v1/rooms/missing/messages/search", params={"q": "valid"})

    assert whitespace.status_code == 422
    assert whitespace.json()["error"]["code"] == "invalid_search_query"
    assert too_short.status_code == 422
    assert too_short.json()["error"]["code"] == "invalid_search_query"
    assert invalid_cursor.status_code == 400
    assert invalid_cursor.json()["error"]["code"] == "invalid_cursor"
    assert missing_room.status_code == 404


def test_search_has_an_independent_principal_rate_limit(tmp_path: Path) -> None:
    settings = Settings(database_path=tmp_path / "search-limit.db", searches_per_minute=1)
    with TestClient(create_app(settings)) as client:
        assert client.post("/v1/rooms", json={"id": "support", "name": "Support"}).status_code == 201
        _create_message(client, "payment retry")
        invalid = client.get("/v1/rooms/support/messages/search", params={"q": "x"})
        first = client.get("/v1/rooms/support/messages/search", params={"q": "payment"})
        limited = client.get("/v1/rooms/support/messages/search", params={"q": "retry"})

    assert invalid.status_code == 422
    assert first.status_code == 200
    assert limited.status_code == 429
    assert limited.headers["retry-after"] == "60"
    assert limited.json()["error"]["code"] == "search_rate_limit_exceeded"


def test_search_honors_room_scoped_read_authorization(tmp_path: Path) -> None:
    settings = Settings(
        database_path=tmp_path / "search-auth.db",
        api_key="search-test-operator-key",
        token_signing_secret=SIGNING_SECRET,
        token_issuer="search-tests",
        token_audience="search-client",
        token_clock_skew_seconds=0,
    )
    service = AccessTokenService(SIGNING_SECRET, issuer="search-tests", audience="search-client", clock_skew_seconds=0)
    token = service.issue("agent", rooms=["support"], permissions=["room:read"], expires_in_seconds=300)
    headers = {"Authorization": f"Bearer {token}"}
    with TestClient(create_app(settings)) as client:
        for room_id in ("support", "private"):
            assert (
                client.post(
                    "/v1/rooms",
                    headers={"X-API-Key": "search-test-operator-key"},
                    json={"id": room_id, "name": room_id.title()},
                ).status_code
                == 201
            )
        allowed = client.get("/v1/rooms/support/messages/search", params={"q": "case"}, headers=headers)
        denied = client.get("/v1/rooms/private/messages/search", params={"q": "case"}, headers=headers)

    assert allowed.status_code == 200
    assert denied.status_code == 403
    assert denied.json()["error"]["code"] == "authorization_denied"
