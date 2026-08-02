"""Static-JWKS verification and asymmetric authorization coverage."""

import json
import time
import uuid
from pathlib import Path
from typing import Any

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import ed25519, rsa
from fastapi.testclient import TestClient
from jwt.algorithms import OKPAlgorithm, RSAAlgorithm

from samsarix_chat_engine import (
    AuthenticationError,
    JWKSAccessTokenVerifier,
    Settings,
    TokenKeySetError,
    create_app,
)
from samsarix_chat_engine.auth import TOKEN_TYPE
from samsarix_chat_engine.config import ConfigurationError

ISSUER = "asymmetric-test-issuer"
AUDIENCE = "asymmetric-test-audience"
OPERATOR_KEY = "asymmetric-test-operator-key"


def _public_jwk(private_key: Any, *, kid: str, algorithm: str) -> dict[str, Any]:
    converter = OKPAlgorithm if algorithm == "EdDSA" else RSAAlgorithm
    value = converter.to_jwk(private_key.public_key(), as_dict=True)
    value.update({"kid": kid, "alg": algorithm, "use": "sig", "key_ops": ["verify"]})
    return value


def _payload(*, rooms: list[str] | None = None, permissions: list[str] | None = None) -> dict[str, Any]:
    now = int(time.time())
    return {
        "iss": ISSUER,
        "aud": AUDIENCE,
        "sub": "asymmetric-user",
        "iat": now,
        "nbf": now,
        "exp": now + 300,
        "jti": uuid.uuid4().hex,
        "rooms": rooms or ["alpha"],
        "permissions": permissions or ["room:read", "room:write"],
    }


def _token(
    private_key: Any,
    *,
    kid: str = "current-key",
    algorithm: str = "EdDSA",
    headers: dict[str, Any] | None = None,
) -> str:
    protected = {"typ": TOKEN_TYPE, "kid": kid, **(headers or {})}
    return jwt.encode(_payload(), private_key, algorithm=algorithm, headers=protected)


def _verifier(keys: list[dict[str, Any]]) -> JWKSAccessTokenVerifier:
    return JWKSAccessTokenVerifier(
        {"keys": keys},
        issuer=ISSUER,
        audience=AUDIENCE,
        max_lifetime_seconds=3_600,
        clock_skew_seconds=0,
    )


def _write_jwks(path: Path, keys: list[dict[str, Any]]) -> Path:
    path.write_text(json.dumps({"keys": keys}), encoding="utf-8")
    return path


def test_eddsa_verification_supports_overlapping_key_rotation() -> None:
    previous = ed25519.Ed25519PrivateKey.generate()
    current = ed25519.Ed25519PrivateKey.generate()
    verifier = _verifier(
        [
            _public_jwk(previous, kid="previous-key", algorithm="EdDSA"),
            _public_jwk(current, kid="current-key", algorithm="EdDSA"),
        ]
    )

    assert verifier.verify(_token(previous, kid="previous-key")).subject == "asymmetric-user"
    principal = verifier.verify(_token(current))
    assert principal.authentication == "token"
    assert principal.allows("room:write", "alpha")


def test_rs256_verification_requires_a_modern_key() -> None:
    private_key = rsa.generate_private_key(public_exponent=65_537, key_size=2_048)
    verifier = _verifier([_public_jwk(private_key, kid="rsa-key", algorithm="RS256")])

    assert verifier.verify(_token(private_key, kid="rsa-key", algorithm="RS256")).subject == "asymmetric-user"

    weak_key = rsa.generate_private_key(public_exponent=65_537, key_size=1_024)  # noqa: S505
    with pytest.raises(TokenKeySetError, match="at least 2048 bits"):
        _verifier([_public_jwk(weak_key, kid="weak-rsa", algorithm="RS256")])


@pytest.mark.parametrize(
    "headers",
    [
        {"typ": "JWT"},
        {"kid": "unknown-key"},
        {"kid": "invalid/key-id"},
        {"jku": "https://attacker.example/keys.json"},
        {"x5u": "https://attacker.example/cert.pem"},
        {"x5c": ["attacker-controlled-certificate"]},
        {"crit": ["attacker-extension"]},
    ],
)
def test_verification_rejects_untrusted_key_selection_and_headers(headers: dict[str, Any]) -> None:
    private_key = ed25519.Ed25519PrivateKey.generate()
    verifier = _verifier([_public_jwk(private_key, kid="current-key", algorithm="EdDSA")])

    with pytest.raises(AuthenticationError, match="invalid access token"):
        verifier.verify(_token(private_key, headers=headers))


def test_verification_rejects_algorithm_confusion_and_wrong_signature() -> None:
    private_key = ed25519.Ed25519PrivateKey.generate()
    other_key = ed25519.Ed25519PrivateKey.generate()
    verifier = _verifier([_public_jwk(private_key, kid="current-key", algorithm="EdDSA")])
    confused = jwt.encode(
        _payload(),
        "attacker-controlled-hmac-secret-that-is-long-enough",
        algorithm="HS256",
        headers={"typ": TOKEN_TYPE, "kid": "current-key"},
    )
    missing_kid = jwt.encode(_payload(), private_key, algorithm="EdDSA", headers={"typ": TOKEN_TYPE})

    with pytest.raises(AuthenticationError):
        verifier.verify(confused)
    with pytest.raises(AuthenticationError):
        verifier.verify(missing_kid)
    with pytest.raises(AuthenticationError):
        verifier.verify(_token(other_key))
    with pytest.raises(AuthenticationError):
        verifier.verify("x" * 8_193)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value.pop("alg"),
        lambda value: value.update(kid="bad/key"),
        lambda value: value.update(use="enc"),
        lambda value: value.update(key_ops=["sign"]),
        lambda value: value.update(d="private-material"),
        lambda value: value.update(kty="oct", k="symmetric-material"),
    ],
)
def test_jwks_rejects_keys_that_are_ambiguous_private_or_not_verification_only(mutate: Any) -> None:
    private_key = ed25519.Ed25519PrivateKey.generate()
    value = _public_jwk(private_key, kid="current-key", algorithm="EdDSA")
    mutate(value)

    with pytest.raises(TokenKeySetError, match="invalid public signing key"):
        _verifier([value])


def test_jwks_bounds_key_count_and_requires_unique_kids() -> None:
    private_key = ed25519.Ed25519PrivateKey.generate()
    value = _public_jwk(private_key, kid="same-key", algorithm="EdDSA")

    with pytest.raises(TokenKeySetError, match="between 1 and 32"):
        _verifier([])
    with pytest.raises(TokenKeySetError, match="between 1 and 32"):
        _verifier([value] * 33)
    with pytest.raises(TokenKeySetError, match="invalid public signing key"):
        _verifier([value, value.copy()])


@pytest.mark.parametrize(
    "contents",
    [
        pytest.param(b"", id="empty"),
        pytest.param(b"[]", id="not-object"),
        pytest.param(b"not-json", id="invalid-json"),
        pytest.param(b"\xff\xfe", id="invalid-utf8"),
        pytest.param(b"x" * 65_537, id="oversized"),
    ],
)
def test_jwks_files_fail_closed_without_exposing_path_or_contents(tmp_path: Path, contents: bytes) -> None:
    path = tmp_path / "sensitive-key-set.json"
    path.write_bytes(contents)

    with pytest.raises(TokenKeySetError) as error:
        JWKSAccessTokenVerifier.from_file(path)
    assert str(path) not in str(error.value)
    assert "not-json" not in str(error.value)


def test_jwks_file_must_be_readable_without_exposing_its_path(tmp_path: Path) -> None:
    path = tmp_path / "missing-sensitive-key-set.json"
    with pytest.raises(TokenKeySetError, match="must be readable") as error:
        JWKSAccessTokenVerifier.from_file(path)
    assert str(path) not in str(error.value)


def test_settings_load_static_jwks_and_reject_mixed_verification_rules(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    private_key = ed25519.Ed25519PrivateKey.generate()
    path = _write_jwks(
        tmp_path / "public.jwks.json",
        [_public_jwk(private_key, kid="current-key", algorithm="EdDSA")],
    )
    monkeypatch.setenv("SAMSARIX_CHAT_TOKEN_VERIFICATION_JWKS_FILE", str(path))
    monkeypatch.setenv("SAMSARIX_CHAT_TOKEN_ISSUER", ISSUER)
    monkeypatch.setenv("SAMSARIX_CHAT_TOKEN_AUDIENCE", AUDIENCE)

    settings = Settings.from_env()
    assert settings.token_verification_jwks_path == path

    with pytest.raises(ConfigurationError, match="set only one"):
        Settings(token_signing_secret="shared-secret-that-is-at-least-32-bytes", token_verification_jwks_path=path)

    monkeypatch.setenv("SAMSARIX_CHAT_TOKEN_SIGNING_SECRET", "shared-secret-that-is-at-least-32-bytes")
    with pytest.raises(ConfigurationError, match="set only one"):
        Settings.from_env()


def test_static_jwks_authenticates_http_and_websocket_clients(tmp_path: Path) -> None:
    private_key = ed25519.Ed25519PrivateKey.generate()
    path = _write_jwks(
        tmp_path / "public.jwks.json",
        [_public_jwk(private_key, kid="current-key", algorithm="EdDSA")],
    )
    settings = Settings(
        database_path=tmp_path / "asymmetric.db",
        api_key=OPERATOR_KEY,
        token_verification_jwks_path=path,
        token_issuer=ISSUER,
        token_audience=AUDIENCE,
        token_max_lifetime_seconds=3_600,
        token_clock_skew_seconds=0,
    )
    token = _token(private_key)

    with TestClient(create_app(settings)) as client:
        assert (
            client.post(
                "/v1/rooms",
                headers={"X-API-Key": OPERATOR_KEY},
                json={"id": "alpha", "name": "Alpha"},
            ).status_code
            == 201
        )
        response = client.post(
            "/v1/rooms/alpha/messages",
            headers={"Authorization": f"Bearer {token}"},
            json={"content": "signed outside the chat service"},
        )
        assert response.status_code == 201
        assert response.json()["sender"] == "asymmetric-user"

        with client.websocket_connect("/v1/rooms/alpha/ws") as websocket:
            assert websocket.receive_json()["type"] == "auth.required"
            websocket.send_json({"type": "auth", "token": token})
            assert websocket.receive_json()["username"] == "asymmetric-user"
            assert websocket.receive_json()["type"] == "history"
