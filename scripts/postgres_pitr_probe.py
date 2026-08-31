# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Exercise application state around a disposable PostgreSQL PITR target.

This is intentionally not a general recovery command. It accepts one exact
database on two loopback ports and requires an explicit CI-only confirmation,
so it cannot be pointed at an operator database by accident.
"""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Mapping
from typing import Any, Literal, NoReturn, Protocol, cast
from urllib.parse import urlparse

from fastapi.testclient import TestClient

from samsarix_chat_engine import AccessTokenService, Settings, create_app

Mode = Literal["seed", "target", "after-target", "verify"]

CONFIRMATION = "disposable-physical-pitr-cluster"
URL_ENVIRONMENT = "SAMSARIX_PITR_REHEARSAL_URL"
CONFIRM_ENVIRONMENT = "SAMSARIX_PITR_REHEARSAL_CONFIRM"
DATABASE = "samsarix_pitr_source"
PORT_BY_MODE: dict[Mode, int] = {
    "seed": 5432,
    "target": 5432,
    "after-target": 5432,
    "verify": 55432,
}
FORBIDDEN_LIBPQ_ENVIRONMENT = ("PGHOSTADDR", "PGSERVICE", "PGSERVICEFILE", "PGOPTIONS")

OPERATOR_KEY = "pitr-rehearsal-operator-key"
SIGNING_SECRET = "pitr-rehearsal-signing-secret-long-enough"  # noqa: S105 - disposable fixture
ISSUER = "samsarix-pitr-rehearsal"
AUDIENCE = "samsarix-pitr-client"
ROOM_ID = "pitr-support"
BASE_CLIENT_ID = "pitr-base"
TARGET_CLIENT_ID = "pitr-target"
AFTER_TARGET_CLIENT_ID = "pitr-after-target"
POST_RECOVERY_CLIENT_ID = "pitr-post-recovery"
BASE_CONTENT = "Baseline incident context retained in the base backup"
TARGET_CONTENT = "Resolution committed through archived WAL before the restore point"
AFTER_TARGET_CONTENT = "Divergent write that must not survive point-in-time recovery"


class ResponseLike(Protocol):
    status_code: int

    def json(self) -> Any: ...


def validated_rehearsal_url(value: str | None, mode: Mode, confirmation: str | None) -> str:
    """Return the exact disposable source or recovery URL, or fail closed."""

    if confirmation != CONFIRMATION:
        raise ValueError(f"{CONFIRM_ENVIRONMENT} must explicitly confirm the disposable rehearsal")
    if value is None or not value:
        raise ValueError(f"{URL_ENVIRONMENT} is required")
    parsed = urlparse(value)
    if parsed.scheme not in {"postgres", "postgresql"}:
        raise ValueError("the rehearsal requires a PostgreSQL URL")
    if parsed.hostname is None or parsed.hostname.lower() not in {"localhost", "127.0.0.1", "::1"}:
        raise ValueError("the rehearsal refuses non-loopback PostgreSQL hosts")
    try:
        port = parsed.port
    except ValueError as error:
        raise ValueError("the rehearsal requires the exact disposable port") from error
    expected_port = PORT_BY_MODE[mode]
    if parsed.path != f"/{DATABASE}" or port != expected_port or parsed.params or parsed.query or parsed.fragment:
        raise ValueError(f"{mode} mode requires {DATABASE} on loopback port {expected_port}")
    return value


def reject_libpq_routing_environment(environment: Mapping[str, str]) -> None:
    present = [name for name in FORBIDDEN_LIBPQ_ENVIRONMENT if name in environment]
    if present:
        raise ValueError(f"unset libpq routing overrides before the rehearsal: {', '.join(present)}")


def _settings(conninfo: str, mode: Mode) -> Settings:
    return Settings(
        storage_backend="postgres",
        postgres_url=conninfo,
        postgres_instance_id=f"pitr-{mode}",
        postgres_relay_poll_seconds=0.01,
        postgres_maintenance_interval_seconds=0.1,
        api_key=OPERATOR_KEY,
        token_signing_secret=SIGNING_SECRET,
        token_issuer=ISSUER,
        token_audience=AUDIENCE,
        token_clock_skew_seconds=0,
        max_rooms=5,
        max_stored_messages=50,
        max_stored_messages_per_room=50,
        max_connections=5,
        max_connections_per_room=5,
        messages_per_minute=50,
        searches_per_minute=50,
    )


def _operator() -> dict[str, str]:
    return {"X-API-Key": OPERATOR_KEY}


def _member() -> dict[str, str]:
    token = AccessTokenService(
        SIGNING_SECRET,
        issuer=ISSUER,
        audience=AUDIENCE,
        max_lifetime_seconds=86_400,
        clock_skew_seconds=0,
    ).issue(
        "pitr-member",
        rooms=[ROOM_ID],
        permissions=["room:read", "room:write"],
        expires_in_seconds=3_600,
    )
    return {"Authorization": f"Bearer {token}"}


def _failed(label: str, response: ResponseLike, expected: int) -> NoReturn:
    raise RuntimeError(f"{label} returned HTTP {response.status_code}; expected {expected}")


def _expect(label: str, response: ResponseLike, expected: int) -> Any:
    if response.status_code != expected:
        _failed(label, response, expected)
    if expected == 204:
        return None
    return response.json()


def _message_map(client: TestClient) -> dict[str, dict[str, Any]]:
    page = _expect("read PITR history", client.get(f"/v1/rooms/{ROOM_ID}/messages", headers=_member()), 200)
    return {item["client_message_id"]: item for item in page["items"]}


def _assert_state(
    client: TestClient,
    *,
    expected_client_ids: set[str],
    forbidden_client_ids: set[str] | None = None,
) -> Mapping[str, int]:
    room = _expect("read PITR room", client.get(f"/v1/rooms/{ROOM_ID}", headers=_operator()), 200)
    if room["name"] != "PITR Support" or room["description"] != "Physical recovery evidence":
        raise RuntimeError("PITR room metadata does not match the seed")

    messages = _message_map(client)
    if set(messages) != expected_client_ids:
        raise RuntimeError("PITR history does not contain the exact expected client IDs")
    if forbidden_client_ids and forbidden_client_ids.intersection(messages):
        raise RuntimeError("post-target divergent state survived point-in-time recovery")
    if messages[BASE_CLIENT_ID]["content"] != BASE_CONTENT or messages[BASE_CLIENT_ID]["edited_at"] is None:
        raise RuntimeError("base-backup message state is incorrect")
    if TARGET_CLIENT_ID in messages and messages[TARGET_CLIENT_ID]["content"] != TARGET_CONTENT:
        raise RuntimeError("WAL-replayed target message state is incorrect")
    if AFTER_TARGET_CLIENT_ID in messages and messages[AFTER_TARGET_CLIENT_ID]["content"] != AFTER_TARGET_CONTENT:
        raise RuntimeError("post-target source message state is incorrect")

    read_state = _expect("read PITR cursor", client.get(f"/v1/rooms/{ROOM_ID}/read-state", headers=_member()), 200)
    if read_state["last_read_message_id"] != messages[BASE_CLIENT_ID]["id"]:
        raise RuntimeError("PITR member read state is incorrect")

    if TARGET_CLIENT_ID in messages:
        search = _expect(
            "search WAL-replayed history",
            client.get(f"/v1/rooms/{ROOM_ID}/messages/search", headers=_member(), params={"q": "archived WAL"}),
            200,
        )
        if [item["id"] for item in search["items"]] != [messages[TARGET_CLIENT_ID]["id"]]:
            raise RuntimeError("WAL-replayed search state is incorrect")

    audits = _expect("read PITR audit", client.get("/v1/admin/audit-events", headers=_operator()), 200)["items"]
    actions = {item["action"] for item in audits}
    if not {"room.created", "message.updated"}.issubset(actions):
        raise RuntimeError("PITR audit trail is incomplete")
    audit_json = json.dumps(audits)
    for content in (BASE_CONTENT, TARGET_CONTENT, AFTER_TARGET_CONTENT):
        if content in audit_json:
            raise RuntimeError("PITR audit unexpectedly contains message content")

    return {"messages": len(messages), "audit_actions": len(actions)}


def seed(conninfo: str) -> Mapping[str, object]:
    with TestClient(create_app(_settings(conninfo, "seed"))) as client:
        _expect("source readiness", client.get("/readyz"), 200)
        if _expect("list empty PITR database", client.get("/v1/rooms", headers=_operator()), 200):
            raise RuntimeError("the source PITR database must be empty")
        _expect(
            "create PITR room",
            client.post(
                "/v1/rooms",
                headers=_operator(),
                json={"id": ROOM_ID, "name": "PITR Support", "description": "Physical recovery evidence"},
            ),
            201,
        )
        base = _expect(
            "create base-backup message",
            client.post(
                f"/v1/rooms/{ROOM_ID}/messages",
                headers=_member(),
                json={"content": "Initial incident context", "client_message_id": BASE_CLIENT_ID},
            ),
            201,
        )
        _expect(
            "edit base-backup message",
            client.patch(
                f"/v1/rooms/{ROOM_ID}/messages/{base['id']}",
                headers=_member(),
                json={"content": BASE_CONTENT},
            ),
            200,
        )
        _expect(
            "persist base-backup read cursor",
            client.put(
                f"/v1/rooms/{ROOM_ID}/read-state",
                headers=_member(),
                json={"message_id": base["id"]},
            ),
            200,
        )
        state = _assert_state(client, expected_client_ids={BASE_CLIENT_ID})
    return {"mode": "seed", "schema": 8, **state}


def add_target(conninfo: str) -> Mapping[str, object]:
    with TestClient(create_app(_settings(conninfo, "target"))) as client:
        _expect("target-write readiness", client.get("/readyz"), 200)
        _assert_state(client, expected_client_ids={BASE_CLIENT_ID})
        _expect(
            "create pre-target message",
            client.post(
                f"/v1/rooms/{ROOM_ID}/messages",
                headers=_member(),
                json={"content": TARGET_CONTENT, "client_message_id": TARGET_CLIENT_ID},
            ),
            201,
        )
        state = _assert_state(client, expected_client_ids={BASE_CLIENT_ID, TARGET_CLIENT_ID})
    return {"mode": "target", "schema": 8, **state}


def add_after_target(conninfo: str) -> Mapping[str, object]:
    with TestClient(create_app(_settings(conninfo, "after-target"))) as client:
        _expect("post-target readiness", client.get("/readyz"), 200)
        _assert_state(client, expected_client_ids={BASE_CLIENT_ID, TARGET_CLIENT_ID})
        _expect(
            "create divergent post-target message",
            client.post(
                f"/v1/rooms/{ROOM_ID}/messages",
                headers=_member(),
                json={"content": AFTER_TARGET_CONTENT, "client_message_id": AFTER_TARGET_CLIENT_ID},
            ),
            201,
        )
        state = _assert_state(
            client,
            expected_client_ids={BASE_CLIENT_ID, TARGET_CLIENT_ID, AFTER_TARGET_CLIENT_ID},
        )
    return {"mode": "after-target", "schema": 8, **state}


def verify(conninfo: str) -> Mapping[str, object]:
    with TestClient(create_app(_settings(conninfo, "verify"))) as client:
        _expect("recovered readiness", client.get("/readyz"), 200)
        state = _assert_state(
            client,
            expected_client_ids={BASE_CLIENT_ID, TARGET_CLIENT_ID},
            forbidden_client_ids={AFTER_TARGET_CLIENT_ID},
        )
        created = _expect(
            "write after PITR",
            client.post(
                f"/v1/rooms/{ROOM_ID}/messages",
                headers=_operator(),
                json={
                    "sender": "pitr-operator",
                    "content": "Recovered timeline remains writable",
                    "client_message_id": POST_RECOVERY_CLIENT_ID,
                },
            ),
            201,
        )
        _expect(
            "edit after PITR",
            client.patch(
                f"/v1/rooms/{ROOM_ID}/messages/{created['id']}",
                headers=_operator(),
                json={"content": "Recovered timeline write verified"},
            ),
            200,
        )
        _expect(
            "delete after PITR",
            client.delete(f"/v1/rooms/{ROOM_ID}/messages/{created['id']}", headers=_operator()),
            204,
        )
        _expect(
            "freeze after PITR", client.patch(f"/v1/rooms/{ROOM_ID}", headers=_operator(), json={"frozen": True}), 200
        )
        _expect(
            "unfreeze after PITR",
            client.patch(f"/v1/rooms/{ROOM_ID}", headers=_operator(), json={"frozen": False}),
            200,
        )
    return {"mode": "verify", "schema": 8, "post_recovery_writes": 5, **state}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=tuple(PORT_BY_MODE))
    args = parser.parse_args()
    mode = cast(Mode, args.mode)
    reject_libpq_routing_environment(os.environ)
    conninfo = validated_rehearsal_url(os.getenv(URL_ENVIRONMENT), mode, os.getenv(CONFIRM_ENVIRONMENT))
    operations = {
        "seed": seed,
        "target": add_target,
        "after-target": add_after_target,
        "verify": verify,
    }
    result = operations[mode](conninfo)
    print(json.dumps(result, separators=(",", ":"), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
