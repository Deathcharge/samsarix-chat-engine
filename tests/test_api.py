"""HTTP integration tests against the real application and SQLite store."""

from fastapi.testclient import TestClient

from samsarix_chat_engine import Settings, create_app
from samsarix_chat_engine.app import RequestBodyLimitMiddleware


def test_operations_endpoints_and_security_headers(client: TestClient) -> None:
    index = client.get("/")
    health = client.get("/healthz")
    ready = client.get("/readyz")
    stats = client.get("/v1/stats")

    assert index.json()["name"] == "Samsarix Chat Engine"
    assert index.json()["version"] == "0.6.0"
    assert health.status_code == 200
    assert health.json() == {"status": "ok"}
    assert ready.status_code == 200
    assert ready.json() == {"status": "ready"}
    assert health.headers["cache-control"] == "no-store"
    assert health.headers["x-content-type-options"] == "nosniff"
    assert health.headers["x-request-id"]
    assert stats.json() == {"active_connections": 0}


def test_room_lifecycle_and_validation(client: TestClient) -> None:
    created = client.post(
        "/v1/rooms",
        json={"id": "project_alpha", "name": "Project Alpha", "description": "A focused room"},
    )

    assert created.status_code == 201
    assert created.headers["location"] == "/v1/rooms/project_alpha"
    assert created.json()["id"] == "project_alpha"
    assert client.get("/v1/rooms/project_alpha").json() == created.json()
    assert [item["id"] for item in client.get("/v1/rooms").json()] == ["project_alpha"]

    duplicate = client.post("/v1/rooms", json={"id": "project_alpha", "name": "Again"})
    assert duplicate.status_code == 409
    assert duplicate.json()["error"]["code"] == "room_already_exists"

    invalid = client.post("/v1/rooms", json={"id": "Not valid", "name": ""})
    assert invalid.status_code == 422
    assert invalid.json()["error"]["code"] == "invalid_request"
    assert client.get("/v1/rooms/missing").status_code == 404


def test_message_round_trip_idempotency_and_pagination(client: TestClient, room: dict[str, str]) -> None:
    first = client.post(
        "/v1/rooms/general/messages",
        headers={"Idempotency-Key": "client-1"},
        json={"sender": "Andrew", "content": "First"},
    )
    replay = client.post(
        "/v1/rooms/general/messages",
        headers={"Idempotency-Key": "client-1"},
        json={"sender": "Andrew", "content": "This payload is ignored on replay"},
    )
    second = client.post(
        "/v1/rooms/general/messages",
        json={"sender": "Sam", "content": "Second", "client_message_id": "client-2"},
    )
    third = client.post(
        "/v1/rooms/general/messages",
        json={"sender": "Lee", "content": "Third", "client_message_id": "client-3"},
    )

    assert first.status_code == 201
    assert replay.status_code == 200
    assert replay.json() == first.json()
    assert second.status_code == 201
    assert third.status_code == 201

    newest = client.get("/v1/rooms/general/messages", params={"limit": 2}).json()
    assert [item["content"] for item in newest["items"]] == ["Second", "Third"]
    assert newest["next_before"] == second.json()["id"]

    older = client.get(
        "/v1/rooms/general/messages",
        params={"limit": 2, "before": newest["next_before"]},
    ).json()
    assert [item["content"] for item in older["items"]] == ["First"]
    assert older["next_before"] is None


def test_message_failures_do_not_echo_private_input(client: TestClient, room: dict[str, str]) -> None:
    missing_room = client.post("/v1/rooms/missing/messages", json={"sender": "A", "content": "hello"})
    oversized = client.post(
        "/v1/rooms/general/messages",
        json={"sender": "A", "content": "x" * 65},
    )
    conflict = client.post(
        "/v1/rooms/general/messages",
        headers={"Idempotency-Key": "header-id"},
        json={"sender": "A", "content": "hello", "client_message_id": "body-id"},
    )
    bad_cursor = client.get("/v1/rooms/general/messages", params={"before": "unknown"})
    private_validation = client.post(
        "/v1/rooms/general/messages",
        json={"content": "private-message-content"},
    )
    oversized_body = client.post(
        "/v1/rooms/general/messages",
        content=b'{"padding":"' + b"x" * 17_000 + b'"}',
        headers={"Content-Type": "application/json"},
    )

    assert missing_room.status_code == 404
    assert oversized.status_code == 413
    assert oversized.json()["error"]["code"] == "message_too_large"
    assert conflict.status_code == 400
    assert bad_cursor.status_code == 400
    assert private_validation.status_code == 422
    assert "private-message-content" not in private_validation.text
    assert oversized_body.status_code == 413
    assert oversized_body.json()["error"]["code"] == "request_too_large"


def test_api_key_protects_chat_data_but_not_health(tmp_path) -> None:
    settings = Settings(database_path=tmp_path / "auth.db", api_key="correct-horse-battery-staple")
    with TestClient(create_app(settings)) as client:
        assert client.get("/healthz").status_code == 200
        unauthorized = client.get("/v1/rooms")
        authorized = client.post(
            "/v1/rooms",
            headers={"Authorization": "Bearer correct-horse-battery-staple"},
            json={"id": "secure", "name": "Secure"},
        )

    assert unauthorized.status_code == 401
    assert unauthorized.json()["error"]["code"] == "authentication_required"
    assert authorized.status_code == 201


def test_persistence_survives_application_restart(settings: Settings) -> None:
    first_app = create_app(settings)
    with TestClient(first_app) as client:
        client.post("/v1/rooms", json={"id": "persistent", "name": "Persistent"})
        client.post(
            "/v1/rooms/persistent/messages",
            json={"sender": "Writer", "content": "Still here"},
        )

    with TestClient(create_app(settings)) as client:
        messages = client.get("/v1/rooms/persistent/messages").json()["items"]

    assert [message["content"] for message in messages] == ["Still here"]


def test_configured_room_and_history_caps_are_enforced(tmp_path) -> None:
    settings = Settings(
        database_path=tmp_path / "bounded.db",
        max_rooms=1,
        max_stored_messages=2,
        max_stored_messages_per_room=2,
    )
    with TestClient(create_app(settings)) as client:
        assert client.post("/v1/rooms", json={"id": "only", "name": "Only"}).status_code == 201
        capacity = client.post("/v1/rooms", json={"id": "extra", "name": "Extra"})
        for index in range(3):
            assert (
                client.post(
                    "/v1/rooms/only/messages",
                    json={"sender": "Writer", "content": f"message-{index}"},
                ).status_code
                == 201
            )
        history = client.get("/v1/rooms/only/messages").json()["items"]

    assert capacity.status_code == 507
    assert [message["content"] for message in history] == ["message-1", "message-2"]


def test_rate_limit_and_cors_are_enforced(tmp_path) -> None:
    settings = Settings(
        database_path=tmp_path / "limited.db",
        allowed_origins=("https://chat.example",),
        messages_per_minute=1,
    )
    with TestClient(create_app(settings)) as client:
        preflight = client.options(
            "/v1/rooms",
            headers={
                "Origin": "https://chat.example",
                "Access-Control-Request-Method": "POST",
            },
        )
        client.post("/v1/rooms", json={"id": "limited", "name": "Limited"})
        with client.websocket_connect(
            "/v1/rooms/limited/ws?username=Browser",
            headers={"Origin": "https://chat.example"},
        ) as websocket:
            assert websocket.receive_json()["type"] == "ready"
            assert websocket.receive_json()["type"] == "history"
        first = client.post(
            "/v1/rooms/limited/messages",
            json={"sender": "Writer", "content": "first"},
        )
        limited = client.post(
            "/v1/rooms/limited/messages",
            json={"sender": "Writer", "content": "second"},
        )

    assert preflight.status_code == 200
    assert preflight.headers["access-control-allow-origin"] == "https://chat.example"
    assert first.status_code == 201
    assert limited.status_code == 429
    assert limited.headers["retry-after"] == "60"


def test_store_does_not_leave_sqlite_handles_open(client: TestClient, settings: Settings, room: dict[str, str]) -> None:
    moved = settings.database_path.with_suffix(".moved")
    settings.database_path.rename(moved)
    moved.rename(settings.database_path)


async def test_streamed_request_body_is_bounded_without_content_length() -> None:
    chunks = iter(
        [
            {"type": "http.request", "body": b"123", "more_body": True},
            {"type": "http.request", "body": b"456", "more_body": False},
        ]
    )
    sent: list[dict[str, object]] = []

    async def receive() -> dict[str, object]:
        return next(chunks)

    async def send(message: dict[str, object]) -> None:
        sent.append(message)

    async def downstream(_scope: object, receive_body: object, _send: object) -> None:
        await receive_body()  # type: ignore[operator]
        await receive_body()  # type: ignore[operator]

    middleware = RequestBodyLimitMiddleware(downstream, max_body_bytes=5)  # type: ignore[arg-type]
    await middleware({"type": "http", "headers": []}, receive, send)  # type: ignore[arg-type]

    assert sent[0]["type"] == "http.response.start"
    assert sent[0]["status"] == 413
