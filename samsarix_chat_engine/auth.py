# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Short-lived access tokens and authorization principals."""

from __future__ import annotations

import json
import re
import secrets
import time
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, Literal, Protocol

import jwt
from jwt import InvalidTokenError, PyJWK, PyJWTError
from jwt.exceptions import MissingCryptographyError

from .models import ROOM_ID_PATTERN

Permission = Literal["room:read", "room:write", "admin"]
ALLOWED_PERMISSIONS: Final[frozenset[str]] = frozenset({"room:read", "room:write", "admin"})
TOKEN_TYPE: Final = "samsarix-access+jwt"  # noqa: S105 - JWT media type, not a credential
_ROOM_ID = re.compile(ROOM_ID_PATTERN)
_KEY_ID = re.compile(r"[A-Za-z0-9._~-]{1,128}")
_ASYMMETRIC_ALGORITHMS: Final[dict[str, tuple[str, str | None]]] = {
    "EdDSA": ("OKP", "Ed25519"),
    "RS256": ("RSA", None),
}
_PRIVATE_JWK_PARAMETERS: Final = frozenset({"d", "p", "q", "dp", "dq", "qi", "oth", "k"})
_FORBIDDEN_TOKEN_HEADERS: Final = frozenset({"b64", "crit", "jku", "x5c", "x5u"})


class AuthenticationError(ValueError):
    """Raised when a credential cannot be trusted."""


class TokenKeySetError(ValueError):
    """Raised when a configured verification key set cannot be trusted."""


@dataclass(frozen=True, slots=True)
class Principal:
    """Authenticated actor and the rooms/actions it may access."""

    subject: str | None
    permissions: frozenset[str]
    rooms: frozenset[str]
    authentication: Literal["none", "api_key", "token"]

    @property
    def is_admin(self) -> bool:
        return "admin" in self.permissions

    def allows(self, permission: Permission, room_id: str | None = None) -> bool:
        if self.is_admin:
            return True
        if permission not in self.permissions:
            return False
        return room_id is None or room_id in self.rooms

    @classmethod
    def local_operator(cls) -> Principal:
        """Represent the backwards-compatible unauthenticated loopback mode."""

        return cls(subject=None, permissions=frozenset({"admin"}), rooms=frozenset(), authentication="none")

    @classmethod
    def api_key_operator(cls) -> Principal:
        """Represent the deployment-wide administrative API key."""

        return cls(subject=None, permissions=frozenset({"admin"}), rooms=frozenset(), authentication="api_key")


class AccessTokenVerifier(Protocol):
    """Minimal verifier contract used by HTTP and WebSocket authentication."""

    def verify(self, token: str) -> Principal:
        """Verify one token and return its immutable authorization principal."""


def _validate_token_profile(
    issuer: str,
    audience: str,
    max_lifetime_seconds: int,
    clock_skew_seconds: int,
) -> None:
    for name, value in {"token issuer": issuer, "token audience": audience}.items():
        if not 1 <= len(value) <= 256 or value != value.strip():
            raise ValueError(f"{name} must be 1 to 256 non-whitespace-padded characters")
    if not 60 <= max_lifetime_seconds <= 604_800:
        raise ValueError("token maximum lifetime must be between 60 and 604800 seconds")
    if not 0 <= clock_skew_seconds <= 300:
        raise ValueError("token clock skew must be between 0 and 300 seconds")


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise ValueError("claim must be a string list")
    if len(value) != len(set(value)) or len(value) > 1_000:
        raise ValueError("claim contains duplicate or excessive values")
    return value


def _validate_rooms(rooms: Iterable[str]) -> frozenset[str]:
    normalized = frozenset(rooms)
    if len(normalized) > 1_000 or any(_ROOM_ID.fullmatch(room) is None for room in normalized):
        raise ValueError("token rooms must contain valid room IDs")
    return normalized


def _validate_permissions(permissions: Iterable[str]) -> frozenset[str]:
    normalized = frozenset(permissions)
    if not normalized <= ALLOWED_PERMISSIONS:
        raise ValueError("token contains an unknown permission")
    return normalized


def _principal_from_payload(payload: dict[str, Any], *, max_lifetime_seconds: int) -> Principal:
    subject = payload.get("sub")
    issued_at = payload.get("iat")
    not_before = payload.get("nbf")
    expires_at = payload.get("exp")
    token_id = payload.get("jti")
    if not isinstance(subject, str) or not 1 <= len(subject.strip()) <= 64 or subject != subject.strip():
        raise AuthenticationError("invalid access token")
    if (
        not isinstance(issued_at, int)
        or isinstance(issued_at, bool)
        or not isinstance(not_before, int)
        or isinstance(not_before, bool)
        or not isinstance(expires_at, int)
        or isinstance(expires_at, bool)
        or expires_at <= issued_at
        or expires_at - issued_at > max_lifetime_seconds
    ):
        raise AuthenticationError("invalid access token")
    if not isinstance(token_id, str) or not 1 <= len(token_id) <= 128:
        raise AuthenticationError("invalid access token")
    try:
        rooms = _validate_rooms(_string_list(payload.get("rooms")))
        permissions = _validate_permissions(_string_list(payload.get("permissions")))
    except ValueError as exc:
        raise AuthenticationError("invalid access token") from exc
    if not permissions or ("admin" not in permissions and not rooms):
        raise AuthenticationError("invalid access token")
    return Principal(subject=subject, permissions=permissions, rooms=rooms, authentication="token")


class AccessTokenService:
    """Issue and verify strictly profiled HS256 access tokens.

    The shared secret is an operator credential. Host applications should issue
    short-lived tokens to users after completing their own login flow.
    """

    def __init__(
        self,
        secret: str,
        *,
        issuer: str = "samsarix-chat-engine",
        audience: str = "samsarix-chat",
        max_lifetime_seconds: int = 86_400,
        clock_skew_seconds: int = 30,
    ) -> None:
        secret_bytes = len(secret.encode("utf-8"))
        if not 32 <= secret_bytes <= 4_096:
            raise ValueError("token signing secret must be between 32 and 4096 bytes")
        _validate_token_profile(issuer, audience, max_lifetime_seconds, clock_skew_seconds)
        self._secret = secret
        self.issuer = issuer
        self.audience = audience
        self.max_lifetime_seconds = max_lifetime_seconds
        self.clock_skew_seconds = clock_skew_seconds

    def issue(
        self,
        subject: str,
        *,
        rooms: Iterable[str],
        permissions: Iterable[Permission],
        expires_in_seconds: int = 3_600,
        now: int | None = None,
    ) -> str:
        """Create a compact token for one authenticated application user."""

        normalized_subject = subject.strip()
        if not 1 <= len(normalized_subject) <= 64:
            raise ValueError("token subject must be between 1 and 64 characters")
        normalized_rooms = _validate_rooms(rooms)
        normalized_permissions = _validate_permissions(permissions)
        if not normalized_permissions:
            raise ValueError("token must grant at least one permission")
        if "admin" not in normalized_permissions and not normalized_rooms:
            raise ValueError("non-admin token must grant at least one room")
        if not 60 <= expires_in_seconds <= self.max_lifetime_seconds:
            raise ValueError(f"token lifetime must be between 60 and {self.max_lifetime_seconds} seconds")

        issued_at = int(time.time()) if now is None else now
        payload: dict[str, Any] = {
            "iss": self.issuer,
            "aud": self.audience,
            "sub": normalized_subject,
            "iat": issued_at,
            "nbf": issued_at,
            "exp": issued_at + expires_in_seconds,
            "jti": uuid.uuid4().hex,
            "rooms": sorted(normalized_rooms),
            "permissions": sorted(normalized_permissions),
        }
        token = jwt.encode(payload, self._secret, algorithm="HS256", headers={"typ": TOKEN_TYPE})
        if len(token) > 8_192:
            raise ValueError("issued token exceeds the 8192-character transport limit")
        return token

    def verify(self, token: str) -> Principal:
        """Verify a token and return its immutable authorization principal."""

        if not token or len(token) > 8_192:
            raise AuthenticationError("invalid access token")
        try:
            header = jwt.get_unverified_header(token)
            if header.get("typ") != TOKEN_TYPE or header.get("alg") != "HS256":
                raise AuthenticationError("invalid access token")
            payload = jwt.decode(
                token,
                self._secret,
                algorithms=["HS256"],
                audience=self.audience,
                issuer=self.issuer,
                leeway=self.clock_skew_seconds,
                options={"require": ["iss", "aud", "sub", "iat", "nbf", "exp", "jti", "rooms", "permissions"]},
            )
        except (InvalidTokenError, ValueError, TypeError) as exc:
            raise AuthenticationError("invalid access token") from exc

        return _principal_from_payload(payload, max_lifetime_seconds=self.max_lifetime_seconds)


class JWKSAccessTokenVerifier:
    """Verify the Samsarix token profile with a bounded static public JWK set."""

    def __init__(
        self,
        jwks: dict[str, Any],
        *,
        issuer: str = "samsarix-chat-engine",
        audience: str = "samsarix-chat",
        max_lifetime_seconds: int = 86_400,
        clock_skew_seconds: int = 30,
    ) -> None:
        _validate_token_profile(issuer, audience, max_lifetime_seconds, clock_skew_seconds)
        keys = jwks.get("keys") if isinstance(jwks, dict) else None
        if not isinstance(keys, list) or not 1 <= len(keys) <= 32:
            raise TokenKeySetError("token JWKS must contain between 1 and 32 public signing keys")

        loaded: dict[str, PyJWK] = {}
        for value in keys:
            if not isinstance(value, dict):
                raise TokenKeySetError("token JWKS contains an invalid public signing key")
            kid = value.get("kid")
            algorithm = value.get("alg")
            expected = _ASYMMETRIC_ALGORITHMS.get(algorithm) if isinstance(algorithm, str) else None
            if (
                not isinstance(kid, str)
                or _KEY_ID.fullmatch(kid) is None
                or kid in loaded
                or expected is None
                or value.get("kty") != expected[0]
                or (expected[1] is not None and value.get("crv") != expected[1])
                or value.get("use") not in {None, "sig"}
                or bool(_PRIVATE_JWK_PARAMETERS.intersection(value))
            ):
                raise TokenKeySetError("token JWKS contains an invalid public signing key")
            key_operations = value.get("key_ops")
            if key_operations is not None and key_operations != ["verify"]:
                raise TokenKeySetError("token JWKS contains an invalid public signing key")
            try:
                key = PyJWK.from_dict(value, algorithm=algorithm)
            except MissingCryptographyError as exc:
                raise TokenKeySetError(
                    "asymmetric token verification requires samsarix-chat-engine[asymmetric-auth]"
                ) from exc
            except (PyJWTError, TypeError, ValueError) as exc:
                raise TokenKeySetError("token JWKS contains an invalid public signing key") from exc
            if algorithm == "RS256" and getattr(key.key, "key_size", 0) < 2_048:
                raise TokenKeySetError("token JWKS RSA keys must be at least 2048 bits")
            loaded[kid] = key

        self._keys = loaded
        self.issuer = issuer
        self.audience = audience
        self.max_lifetime_seconds = max_lifetime_seconds
        self.clock_skew_seconds = clock_skew_seconds

    @classmethod
    def from_file(
        cls,
        path: Path,
        *,
        issuer: str = "samsarix-chat-engine",
        audience: str = "samsarix-chat",
        max_lifetime_seconds: int = 86_400,
        clock_skew_seconds: int = 30,
    ) -> JWKSAccessTokenVerifier:
        """Load at most 64 KiB of UTF-8 JSON without exposing its path or contents."""

        try:
            with path.open("rb") as handle:
                encoded = handle.read(65_537)
        except OSError as exc:
            raise TokenKeySetError("token JWKS file must be readable") from exc
        if len(encoded) > 65_536:
            raise TokenKeySetError("token JWKS file must not exceed 65536 bytes")
        try:
            value = json.loads(encoded.decode("utf-8"))
        except (UnicodeDecodeError, ValueError, TypeError) as exc:
            raise TokenKeySetError("token JWKS file must contain valid UTF-8 JSON") from exc
        if not isinstance(value, dict):
            raise TokenKeySetError("token JWKS file must contain a JSON object")
        return cls(
            value,
            issuer=issuer,
            audience=audience,
            max_lifetime_seconds=max_lifetime_seconds,
            clock_skew_seconds=clock_skew_seconds,
        )

    def verify(self, token: str) -> Principal:
        """Select one local public key by bounded ``kid`` and verify the token."""

        if not token or len(token) > 8_192:
            raise AuthenticationError("invalid access token")
        try:
            header = jwt.get_unverified_header(token)
            kid = header.get("kid")
            algorithm = header.get("alg")
            if (
                header.get("typ") != TOKEN_TYPE
                or not isinstance(kid, str)
                or _KEY_ID.fullmatch(kid) is None
                or algorithm not in _ASYMMETRIC_ALGORITHMS
                or _FORBIDDEN_TOKEN_HEADERS.intersection(header)
            ):
                raise AuthenticationError("invalid access token")
            key = self._keys.get(kid)
            if key is None or key.algorithm_name != algorithm:
                raise AuthenticationError("invalid access token")
            payload = jwt.decode(
                token,
                key.key,
                algorithms=[algorithm],
                audience=self.audience,
                issuer=self.issuer,
                leeway=self.clock_skew_seconds,
                options={"require": ["iss", "aud", "sub", "iat", "nbf", "exp", "jti", "rooms", "permissions"]},
            )
        except (InvalidTokenError, ValueError, TypeError) as exc:
            raise AuthenticationError("invalid access token") from exc
        return _principal_from_payload(payload, max_lifetime_seconds=self.max_lifetime_seconds)


def credentials_match(provided: str | None, expected: str | None) -> bool:
    """Compare operator credentials without a data-dependent early return."""

    return (
        expected is not None
        and provided is not None
        and secrets.compare_digest(provided.encode("utf-8"), expected.encode("utf-8"))
    )
