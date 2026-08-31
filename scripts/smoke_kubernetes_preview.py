# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Exercise the checked Kubernetes preview through two local Pod forwards."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import httpx2 as httpx
import websockets
from websockets.asyncio.client import ClientConnection

from samsarix_chat_engine.auth import AccessTokenService

_API_KEY_ENV = "SAMSARIX_KUBERNETES_ACCEPTANCE_API_KEY"
_TOKEN_SECRET_ENV = "SAMSARIX_KUBERNETES_ACCEPTANCE_TOKEN_SECRET"  # noqa: S105 - variable name, not a secret
_ROOM_ID = "kubernetes-live"
_ROOM_NAME = "Kubernetes live acceptance"
_MESSAGE = "cross-replica acceptance"
_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "::1", "localhost"})


class AcceptanceError(RuntimeError):
    """Raised when the live preview violates an acceptance invariant."""


@dataclass(frozen=True, slots=True)
class Endpoint:
    """Validated HTTP and WebSocket origins for one local Pod forward."""

    http_origin: str
    websocket_origin: str


def parse_endpoint(value: str) -> Endpoint:
    """Accept only explicit loopback HTTP origins without URL extras."""

    parsed = urlsplit(value)
    if (
        parsed.scheme != "http"
        or parsed.hostname not in _LOOPBACK_HOSTS
        or parsed.port is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("endpoint must be an explicit loopback HTTP origin with a port")
    http_origin = urlunsplit(("http", parsed.netloc, "", "", ""))
    websocket_origin = urlunsplit(("ws", parsed.netloc, "", "", ""))
    return Endpoint(http_origin=http_origin, websocket_origin=websocket_origin)


def _required_environment(name: str) -> str:
    value = os.environ.get(name)
    if value is None or not value:
        raise AcceptanceError(f"required environment variable {name} is missing")
    return value


def _expect_status(response: httpx.Response, expected: int, operation: str) -> dict[str, Any] | None:
    if response.status_code != expected:
        raise AcceptanceError(f"{operation} returned HTTP {response.status_code}, expected {expected}")
    if not response.content:
        return None
    payload = response.json()
    if not isinstance(payload, dict):
        raise AcceptanceError(f"{operation} returned an unexpected response shape")
    return payload


async def _receive_type(connection: ClientConnection, expected: str) -> dict[str, Any]:
    for _ in range(12):
        raw = await asyncio.wait_for(connection.recv(), timeout=5)
        payload = json.loads(raw)
        if not isinstance(payload, dict) or not isinstance(payload.get("type"), str):
            raise AcceptanceError("WebSocket returned an invalid frame")
        if payload["type"] == expected:
            return payload
    raise AcceptanceError(f"WebSocket did not produce {expected}")


async def _check_replica(client: httpx.AsyncClient, endpoint: Endpoint) -> None:
    ready = await client.get(f"{endpoint.http_origin}/readyz")
    _expect_status(ready, 200, "readiness check")
    room = await client.get(f"{endpoint.http_origin}/v1/rooms/{_ROOM_ID}")
    payload = _expect_status(room, 200, "room lookup")
    if payload is None or payload.get("id") != _ROOM_ID or payload.get("name") != _ROOM_NAME:
        raise AcceptanceError("room lookup returned the wrong durable room")


async def run(first: Endpoint, second: Endpoint, *, verify_existing: bool) -> None:
    """Run the initial cross-replica journey or a post-restart durability check."""

    api_key = _required_environment(_API_KEY_ENV)
    token_secret = _required_environment(_TOKEN_SECRET_ENV)
    headers = {"X-API-Key": api_key}
    async with httpx.AsyncClient(headers=headers, timeout=5, trust_env=False) as client:
        if not verify_existing:
            created = await client.post(
                f"{first.http_origin}/v1/rooms",
                json={"id": _ROOM_ID, "name": _ROOM_NAME},
            )
            _expect_status(created, 201, "room creation")
        await asyncio.gather(_check_replica(client, first), _check_replica(client, second))

    if verify_existing:
        print("Kubernetes preview retained its room across Pod replacement.")
        return

    tokens = AccessTokenService(token_secret)
    alice_token = tokens.issue(
        "kubernetes-alice",
        rooms=[_ROOM_ID],
        permissions=["room:read", "room:write"],
        expires_in_seconds=300,
    )
    bob_token = tokens.issue(
        "kubernetes-bob",
        rooms=[_ROOM_ID],
        permissions=["room:read", "room:write"],
        expires_in_seconds=300,
    )
    socket_path = f"/v1/rooms/{_ROOM_ID}/ws"
    async with websockets.connect(
        f"{first.websocket_origin}{socket_path}",
        additional_headers={"Authorization": f"Bearer {alice_token}"},
        open_timeout=5,
        close_timeout=1,
    ) as alice:
        ready = await _receive_type(alice, "ready")
        await _receive_type(alice, "history")
        if ready.get("username") != "kubernetes-alice":
            raise AcceptanceError("first replica authenticated the wrong subject")
        async with websockets.connect(
            f"{second.websocket_origin}{socket_path}",
            additional_headers={"Authorization": f"Bearer {bob_token}"},
            open_timeout=5,
            close_timeout=1,
        ) as bob:
            ready = await _receive_type(bob, "ready")
            await _receive_type(bob, "history")
            if ready.get("username") != "kubernetes-bob":
                raise AcceptanceError("second replica authenticated the wrong subject")
            await alice.send(json.dumps({"type": "message", "content": _MESSAGE}))
            first_event, second_event = await asyncio.gather(
                _receive_type(alice, "message.created"),
                _receive_type(bob, "message.created"),
            )
            first_message = first_event.get("message")
            second_message = second_event.get("message")
            if (
                not isinstance(first_message, dict)
                or not isinstance(second_message, dict)
                or first_message.get("id") != second_message.get("id")
                or first_message.get("content") != _MESSAGE
                or second_message.get("content") != _MESSAGE
            ):
                raise AcceptanceError("replicas did not converge on the same message event")
    print("Kubernetes preview passed cross-replica HTTP and WebSocket acceptance.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--first-url", required=True, type=parse_endpoint)
    parser.add_argument("--second-url", required=True, type=parse_endpoint)
    parser.add_argument(
        "--verify-existing",
        action="store_true",
        help="verify readiness and the room created by an earlier acceptance run",
    )
    arguments = parser.parse_args()
    asyncio.run(run(arguments.first_url, arguments.second_url, verify_existing=arguments.verify_existing))


if __name__ == "__main__":
    main()
