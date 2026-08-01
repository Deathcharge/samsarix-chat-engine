"""Access-token unit tests and authorization integration coverage."""

import time
from collections.abc import Iterator
from pathlib import Path

import jwt
import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from samsarix_chat_engine import AccessTokenService, AuthenticationError, Principal, Settings, create_app
from samsarix_chat_engine.auth import TOKEN_TYPE
from samsarix_chat_engine.cli import main

SIGNING_SECRET = "test-only-signing-secret-that-is-long-enough"
OPERATOR_KEY = "test-only-operator-api-key"


def _service(**overrides: object) -> AccessTokenService:
    values = {
        "secret": SIGNING_SECRET,
        "issuer": "test-issuer",
        "audience": "test-audience",
        "max_lifetime_seconds": 3_600,
        "clock_skew_seconds": 0,
        **overrides,
    }
    return AccessTokenService(**values)  # type: ignore[arg-type]


def _token(service: AccessTokenService, *, rooms: list[str] | None = None, permissions: list[str] | None = None) -> str:
    return service.issue(
        "user-123",
        rooms=rooms or ["alpha"],
        permissions=permissions or ["room:read", "room:write"],  # type: ignore[arg-type]
        expires_in_seconds=300,
    )


def test_token_round_trip_and_principal_permissions() -> None:
    service = _service()
    principal = service.verify(_token(service))

    assert principal.subject == "user-123"
    assert principal.authentication == "token"
    assert principal.allows("room:read", "alpha")
    assert principal.allows("room:write", "alpha")
    assert not principal.allows("room:read", "other")
    assert not principal.is_admin

    admin = service.verify(_token(service, rooms=[], permissions=["admin"]))
    assert admin.is_admin
    assert admin.allows("room:write", "any-room")
    assert Principal.api_key_operator().is_admin
    assert Principal.local_operator().authentication == "none"


@pytest.mark.parametrize(
    ("rooms", "permissions", "subject", "expires", "match"),
    [
        ([], ["room:read"], "user", 300, "at least one room"),
        (["Not Valid"], ["room:read"], "user", 300, "valid room IDs"),
        (["alpha"], ["unknown"], "user", 300, "unknown permission"),
        (["alpha"], [], "user", 300, "at least one permission"),
        (["alpha"], ["room:read"], "", 300, "subject"),
        (["alpha"], ["room:read"], "x" * 65, 300, "subject"),
        (["alpha"], ["room:read"], "user", 59, "lifetime"),
        (["alpha"], ["room:read"], "user", 3_601, "lifetime"),
    ],
)
def test_token_issuance_rejects_unsafe_claims(
    rooms: list[str], permissions: list[str], subject: str, expires: int, match: str
) -> None:
    with pytest.raises(ValueError, match=match):
        _service().issue(
            subject,
            rooms=rooms,
            permissions=permissions,  # type: ignore[arg-type]
            expires_in_seconds=expires,
        )


def test_token_service_configuration_is_bounded() -> None:
    with pytest.raises(ValueError, match="32 and 4096 bytes"):
        AccessTokenService("too-short")
    with pytest.raises(ValueError, match="token issuer"):
        AccessTokenService(SIGNING_SECRET, issuer="")
    with pytest.raises(ValueError, match="maximum lifetime"):
        AccessTokenService(SIGNING_SECRET, max_lifetime_seconds=59)
    with pytest.raises(ValueError, match="clock skew"):
        AccessTokenService(SIGNING_SECRET, clock_skew_seconds=301)


def test_token_settings_load_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SAMSARIX_CHAT_TOKEN_SIGNING_SECRET", SIGNING_SECRET)
    monkeypatch.setenv("SAMSARIX_CHAT_TOKEN_ISSUER", "custom-issuer")
    monkeypatch.setenv("SAMSARIX_CHAT_TOKEN_AUDIENCE", "custom-audience")
    monkeypatch.setenv("SAMSARIX_CHAT_TOKEN_MAX_LIFETIME", "1800")
    monkeypatch.setenv("SAMSARIX_CHAT_TOKEN_CLOCK_SKEW", "5")

    settings = Settings.from_env()

    assert settings.token_signing_secret == SIGNING_SECRET
    assert settings.token_issuer == "custom-issuer"
    assert settings.token_audience == "custom-audience"
    assert settings.token_max_lifetime_seconds == 1800
    assert settings.token_clock_skew_seconds == 5
    with pytest.raises(ValueError, match="32 and 4096 bytes"):
        Settings(token_signing_secret="too-short")


def test_token_verification_rejects_tampering_expiry_and_context_confusion() -> None:
    service = _service()
    valid = _token(service)
    payload = jwt.decode(valid, options={"verify_signature": False})

    with pytest.raises(AuthenticationError):
        service.verify(valid + "tampered")
    with pytest.raises(AuthenticationError):
        _service(audience="different").verify(valid)
    with pytest.raises(AuthenticationError):
        _service(issuer="different").verify(valid)
    with pytest.raises(AuthenticationError):
        service.verify("x" * 8_193)

    expired = service.issue(
        "user-123",
        rooms=["alpha"],
        permissions=["room:read"],
        expires_in_seconds=60,
        now=int(time.time()) - 120,
    )
    with pytest.raises(AuthenticationError):
        service.verify(expired)

    wrong_type = jwt.encode(payload, SIGNING_SECRET, algorithm="HS256", headers={"typ": "JWT"})
    with pytest.raises(AuthenticationError):
        service.verify(wrong_type)


@pytest.mark.parametrize(
    ("claim", "value"),
    [
        ("sub", " "),
        ("jti", ""),
        ("iat", True),
        ("exp", True),
        ("nbf", True),
        ("rooms", "alpha"),
        ("rooms", ["alpha", "alpha"]),
        ("rooms", ["Not Valid"]),
        ("permissions", "room:read"),
        ("permissions", ["unknown"]),
        ("permissions", []),
    ],
)
def test_token_verification_rejects_malformed_signed_claims(claim: str, value: object) -> None:
    service = _service()
    payload = jwt.decode(_token(service), options={"verify_signature": False})
    payload[claim] = value
    malformed = jwt.encode(payload, SIGNING_SECRET, algorithm="HS256", headers={"typ": TOKEN_TYPE})

    with pytest.raises(AuthenticationError):
        service.verify(malformed)


def test_issuer_refuses_token_that_cannot_fit_its_transport_contract() -> None:
    rooms = [f"room-{index:04d}-" + "x" * 50 for index in range(200)]
    with pytest.raises(ValueError, match="transport limit"):
        _service().issue(
            "user-123",
            rooms=rooms,
            permissions=["room:read"],
            expires_in_seconds=300,
        )


@pytest.fixture
def secured_client(tmp_path: Path) -> Iterator[tuple[TestClient, AccessTokenService]]:
    settings = Settings(
        database_path=tmp_path / "authorized.db",
        api_key=OPERATOR_KEY,
        token_signing_secret=SIGNING_SECRET,
        token_issuer="test-issuer",
        token_audience="test-audience",
        token_max_lifetime_seconds=3_600,
        token_clock_skew_seconds=0,
    )
    client = TestClient(create_app(settings))
    client.__enter__()
    response = client.post(
        "/v1/rooms",
        headers={"X-API-Key": OPERATOR_KEY},
        json={"id": "alpha", "name": "Alpha"},
    )
    assert response.status_code == 201
    yield client, _service()
    client.__exit__(None, None, None)


def test_rest_tokens_enforce_room_permissions_and_identity(
    secured_client: tuple[TestClient, AccessTokenService],
) -> None:
    client, service = secured_client
    writer = _token(service)
    reader = _token(service, permissions=["room:read"])
    other_room = _token(service, rooms=["elsewhere"])

    assert client.get("/v1/rooms/alpha").status_code == 401
    assert client.get("/v1/rooms", headers={"Authorization": f"Bearer {writer}"}).status_code == 403
    assert client.get("/v1/rooms/alpha", headers={"Authorization": f"Bearer {writer}"}).status_code == 200
    assert client.get("/v1/rooms/alpha", headers={"Authorization": f"Bearer {other_room}"}).status_code == 403
    assert (
        client.post(
            "/v1/rooms/alpha/messages",
            headers={"Authorization": f"Bearer {reader}"},
            json={"content": "blocked"},
        ).status_code
        == 403
    )

    spoofed = client.post(
        "/v1/rooms/alpha/messages",
        headers={"Authorization": f"Bearer {writer}"},
        json={"sender": "someone-else", "content": "spoofed"},
    )
    created = client.post(
        "/v1/rooms/alpha/messages",
        headers={"Authorization": f"Bearer {writer}"},
        json={"content": "trusted identity"},
    )

    assert spoofed.status_code == 403
    assert spoofed.json()["error"]["code"] == "identity_mismatch"
    assert created.status_code == 201
    assert created.json()["sender"] == "user-123"
    history = client.get(
        "/v1/rooms/alpha/messages",
        headers={"Authorization": f"Bearer {reader}"},
    )
    assert [item["content"] for item in history.json()["items"]] == ["trusted identity"]


def test_openapi_advertises_both_security_schemes(
    secured_client: tuple[TestClient, AccessTokenService],
) -> None:
    client, _service = secured_client
    schema = client.get("/openapi.json").json()

    assert schema["components"]["securitySchemes"] == {
        "OperatorKey": {"type": "apiKey", "in": "header", "name": "X-API-Key"},
        "AccessToken": {"type": "http", "scheme": "bearer"},
    }
    assert schema["paths"]["/v1/rooms"]["post"]["security"] == [
        {"OperatorKey": []},
        {"AccessToken": []},
    ]


def test_websocket_tokens_support_browser_handshake_and_read_only_sessions(
    secured_client: tuple[TestClient, AccessTokenService],
) -> None:
    client, service = secured_client
    reader = _token(service, permissions=["room:read"])

    with client.websocket_connect("/v1/rooms/alpha/ws") as websocket:
        assert websocket.receive_json()["type"] == "auth.required"
        websocket.send_json({"type": "auth", "token": reader})
        ready = websocket.receive_json()
        assert ready["type"] == "ready"
        assert ready["username"] == "user-123"
        assert websocket.receive_json()["type"] == "history"
        websocket.send_json({"type": "message", "content": "not allowed"})
        assert websocket.receive_json()["code"] == "authorization_denied"
        websocket.send_json({"type": "ping"})
        assert websocket.receive_json() == {"type": "pong"}


def test_websocket_rejects_room_and_identity_escalation(
    secured_client: tuple[TestClient, AccessTokenService],
) -> None:
    client, service = secured_client
    token = _token(service)

    with pytest.raises(WebSocketDisconnect) as mismatch:
        with client.websocket_connect("/v1/rooms/alpha/ws?username=spoofed") as websocket:
            websocket.receive_json()
            websocket.send_json({"type": "auth", "token": token})
            assert websocket.receive_json()["code"] == "identity_mismatch"
            websocket.receive_json()
    assert mismatch.value.code == 4403

    wrong_room = _token(service, rooms=["elsewhere"])
    with pytest.raises(WebSocketDisconnect) as denied:
        with client.websocket_connect("/v1/rooms/alpha/ws") as websocket:
            websocket.receive_json()
            websocket.send_json({"type": "auth", "token": wrong_room})
            assert websocket.receive_json()["code"] == "authorization_denied"
            websocket.receive_json()
    assert denied.value.code == 4403


def test_cli_issues_verifiable_token(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setenv("SAMSARIX_CHAT_TOKEN_SIGNING_SECRET", SIGNING_SECRET)
    monkeypatch.setenv("SAMSARIX_CHAT_TOKEN_ISSUER", "test-issuer")
    monkeypatch.setenv("SAMSARIX_CHAT_TOKEN_AUDIENCE", "test-audience")
    monkeypatch.setenv("SAMSARIX_CHAT_TOKEN_MAX_LIFETIME", "3600")

    result = main(["token", "issue", "--subject", "cli-user", "--room", "alpha", "--expires-in", "300"])
    issued = capsys.readouterr().out.strip()

    assert result == 0
    principal = _service().verify(issued)
    assert principal.subject == "cli-user"
    assert principal.permissions == frozenset({"room:read", "room:write"})


def test_cli_refuses_token_issuance_without_signing_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SAMSARIX_CHAT_TOKEN_SIGNING_SECRET", raising=False)
    with pytest.raises(SystemExit) as exit_info:
        main(["token", "issue", "--subject", "cli-user", "--room", "alpha"])
    assert exit_info.value.code == 2


def test_remote_browser_origins_require_an_explicit_allowlist(tmp_path: Path) -> None:
    settings = Settings(database_path=tmp_path / "origin.db", api_key=OPERATOR_KEY)
    with TestClient(create_app(settings)) as client:
        with pytest.raises(WebSocketDisconnect) as rejected:
            with client.websocket_connect(
                "/v1/rooms/unknown/ws?username=Browser",
                headers={"Origin": "https://unlisted.example", "X-API-Key": OPERATOR_KEY},
            ):
                pass
    assert rejected.value.code == 4403
