# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Seed and verify the disposable PostgreSQL logical-restore rehearsal.

This is intentionally not a general backup command. It refuses remote hosts and
database names outside the two fixed CI rehearsal databases so it cannot be
pointed at a production database by accident.
"""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Mapping
from typing import Any, Literal, NoReturn, Protocol
from urllib.parse import urlparse

from fastapi.testclient import TestClient

from samsarix_chat_engine import AccessTokenService, Settings, create_app

CONFIRMATION = "disposable-loopback-databases"
URL_ENVIRONMENT = "SAMSARIX_RESTORE_REHEARSAL_URL"
CONFIRM_ENVIRONMENT = "SAMSARIX_RESTORE_REHEARSAL_CONFIRM"
DATABASE_BY_MODE = {
    "seed": "samsarix_backup_source",
    "verify": "samsarix_backup_restore",
}
FORBIDDEN_LIBPQ_ENVIRONMENT = ("PGHOSTADDR", "PGSERVICE", "PGSERVICEFILE", "PGOPTIONS")
OPERATOR_KEY = "restore-rehearsal-operator-key"
SIGNING_SECRET = "restore-rehearsal-signing-secret-long-enough"  # noqa: S105 - disposable fixture
ISSUER = "samsarix-restore-rehearsal"
AUDIENCE = "samsarix-restore-client"
ROOM_ID = "restore-support"
ARCHIVED_ROOM_ID = "restore-archive"
FIRST_CLIENT_ID = "restore-first"
DELETED_CLIENT_ID = "restore-deleted"
RESTORED_CONTENT = "Case resolved after a searchable follow-up"


class ResponseLike(Protocol):
    status_code: int

    def json(self) -> Any: ...


def validated_rehearsal_url(value: str | None, mode: Literal["seed", "verify"], confirmation: str | None) -> str:
    """Return a narrowly scoped loopback URL or fail before touching storage."""

    if confirmation != CONFIRMATION:
        raise ValueError(f"{CONFIRM_ENVIRONMENT} must explicitly confirm the disposable rehearsal")
    if value is None or not value:
        raise ValueError(f"{URL_ENVIRONMENT} is required")
    parsed = urlparse(value)
    expected_database = DATABASE_BY_MODE[mode]
    if parsed.scheme not in {"postgres", "postgresql"}:
        raise ValueError("the rehearsal requires a PostgreSQL URL")
    if parsed.hostname is None or parsed.hostname.lower() not in {"localhost", "127.0.0.1", "::1"}:
        raise ValueError("the rehearsal refuses non-loopback PostgreSQL hosts")
    if parsed.path != f"/{expected_database}" or parsed.params or parsed.query or parsed.fragment:
        raise ValueError(f"{mode} mode requires the exact disposable database {expected_database}")
    return value


def reject_libpq_routing_environment(environment: Mapping[str, str]) -> None:
    present = [name for name in FORBIDDEN_LIBPQ_ENVIRONMENT if name in environment]
    if present:
        raise ValueError(f"unset libpq routing overrides before the rehearsal: {', '.join(present)}")


def _settings(conninfo: str, instance_id: str) -> Settings:
    return Settings(
        storage_backend="postgres",
        postgres_url=conninfo,
        postgres_instance_id=instance_id,
        postgres_relay_poll_seconds=0.01,
        postgres_maintenance_interval_seconds=0.1,
        api_key=OPERATOR_KEY,
        token_signing_secret=SIGNING_SECRET,
        token_issuer=ISSUER,
        token_audience=AUDIENCE,
        token_clock_skew_seconds=0,
        max_rooms=10,
        max_stored_messages=100,
        max_stored_messages_per_room=50,
        max_connections=10,
        max_connections_per_room=10,
        messages_per_minute=100,
        searches_per_minute=100,
    )


def _token() -> str:
    service = AccessTokenService(
        SIGNING_SECRET,
        issuer=ISSUER,
        audience=AUDIENCE,
        max_lifetime_seconds=86_400,
        clock_skew_seconds=0,
    )
    return service.issue(
        "restore-member",
        rooms=[ROOM_ID],
        permissions=["room:read", "room:write"],
        expires_in_seconds=3_600,
    )


def _operator() -> dict[str, str]:
    return {"X-API-Key": OPERATOR_KEY}


def _member() -> dict[str, str]:
    return {"Authorization": f"Bearer {_token()}"}


def _failed(label: str, response: ResponseLike, expected: int) -> NoReturn:
    raise RuntimeError(f"{label} returned HTTP {response.status_code}; expected {expected}")


def _expect(label: str, response: ResponseLike, expected: int) -> Any:
    if response.status_code != expected:
        _failed(label, response, expected)
    if expected == 204:
        return None
    return response.json()


def _assert_seeded_state(client: TestClient) -> dict[str, int]:
    operator = _operator()
    member = _member()
    room = _expect("read room", client.get(f"/v1/rooms/{ROOM_ID}", headers=operator), 200)
    if room["name"] != "Restore Support" or room["description"] != "Logical restore evidence":
        raise RuntimeError("restored room metadata does not match the seed")

    page = _expect("read history", client.get(f"/v1/rooms/{ROOM_ID}/messages", headers=member), 200)
    messages = page["items"]
    by_client_id = {message["client_message_id"]: message for message in messages}
    if len(messages) != 2 or set(by_client_id) != {FIRST_CLIENT_ID, DELETED_CLIENT_ID}:
        raise RuntimeError("restored history does not contain the exact seeded client IDs")
    first = by_client_id[FIRST_CLIENT_ID]
    deleted = by_client_id[DELETED_CLIENT_ID]
    if first["content"] != RESTORED_CONTENT or first["edited_at"] is None or first["deleted_at"] is not None:
        raise RuntimeError("restored edited message state is incorrect")
    if deleted["content"] != "" or deleted["deleted_at"] is None:
        raise RuntimeError("restored tombstone state is incorrect")

    search = _expect(
        "search restored history",
        client.get(f"/v1/rooms/{ROOM_ID}/messages/search", headers=member, params={"q": "searchable follow-up"}),
        200,
    )
    if [item["id"] for item in search["items"]] != [first["id"]]:
        raise RuntimeError("restored search result does not identify the edited message")

    read_state = _expect("read restored cursor", client.get(f"/v1/rooms/{ROOM_ID}/read-state", headers=member), 200)
    if read_state["last_read_message_id"] != first["id"] or read_state["subject"] != "restore-member":
        raise RuntimeError("restored member read state is incorrect")

    archived = _expect("read archived room", client.get(f"/v1/rooms/{ARCHIVED_ROOM_ID}", headers=operator), 200)
    if archived["archived_at"] is None:
        raise RuntimeError("restored archived-room state is incorrect")

    audit = _expect("read audit", client.get("/v1/admin/audit-events", headers=operator), 200)["items"]
    actions = {item["action"] for item in audit}
    required_actions = {
        "room.created",
        "room.archived",
        "room.frozen",
        "room.unfrozen",
        "message.updated",
        "message.deleted",
    }
    if not required_actions.issubset(actions):
        raise RuntimeError("restored audit trail is incomplete")
    if RESTORED_CONTENT in json.dumps(audit):
        raise RuntimeError("administrative audit unexpectedly contains message content")

    return {"rooms": 2, "messages": 2, "audit_actions": len(actions)}


def seed(conninfo: str) -> Mapping[str, object]:
    with TestClient(create_app(_settings(conninfo, "restore-seed"))) as client:
        _expect("readiness", client.get("/readyz"), 200)
        existing = _expect("list empty rehearsal database", client.get("/v1/rooms", headers=_operator()), 200)
        if existing:
            raise RuntimeError("the source rehearsal database must be empty")
        _expect(
            "create support room",
            client.post(
                "/v1/rooms",
                headers=_operator(),
                json={"id": ROOM_ID, "name": "Restore Support", "description": "Logical restore evidence"},
            ),
            201,
        )
        _expect(
            "create archived room",
            client.post(
                "/v1/rooms",
                headers=_operator(),
                json={"id": ARCHIVED_ROOM_ID, "name": "Archived Restore Fixture"},
            ),
            201,
        )
        first = _expect(
            "create member message",
            client.post(
                f"/v1/rooms/{ROOM_ID}/messages",
                headers=_member(),
                json={"content": "Initial case question", "client_message_id": FIRST_CLIENT_ID},
            ),
            201,
        )
        deleted = _expect(
            "create disposable message",
            client.post(
                f"/v1/rooms/{ROOM_ID}/messages",
                headers=_member(),
                json={"content": "Temporary note", "client_message_id": DELETED_CLIENT_ID},
            ),
            201,
        )
        _expect(
            "edit member message",
            client.patch(
                f"/v1/rooms/{ROOM_ID}/messages/{first['id']}",
                headers=_member(),
                json={"content": RESTORED_CONTENT},
            ),
            200,
        )
        _expect(
            "mark read",
            client.put(
                f"/v1/rooms/{ROOM_ID}/read-state",
                headers=_member(),
                json={"message_id": first["id"]},
            ),
            200,
        )
        _expect(
            "delete member message",
            client.delete(f"/v1/rooms/{ROOM_ID}/messages/{deleted['id']}", headers=_member()),
            204,
        )
        _expect("freeze room", client.patch(f"/v1/rooms/{ROOM_ID}", headers=_operator(), json={"frozen": True}), 200)
        _expect(
            "unfreeze room",
            client.patch(f"/v1/rooms/{ROOM_ID}", headers=_operator(), json={"frozen": False}),
            200,
        )
        _expect(
            "archive room",
            client.patch(f"/v1/rooms/{ARCHIVED_ROOM_ID}", headers=_operator(), json={"archived": True}),
            200,
        )
        state = _assert_seeded_state(client)
    return {"mode": "seed", "schema": 8, **state}


def verify(conninfo: str) -> Mapping[str, object]:
    with TestClient(create_app(_settings(conninfo, "restore-verify"))) as client:
        _expect("restored readiness", client.get("/readyz"), 200)
        state = _assert_seeded_state(client)
        created = _expect(
            "write after restore",
            client.post(
                f"/v1/rooms/{ROOM_ID}/messages",
                headers=_operator(),
                json={
                    "sender": "restore-operator",
                    "content": "Post-restore write acceptance",
                    "client_message_id": "restore-after",
                },
            ),
            201,
        )
        _expect(
            "edit after restore",
            client.patch(
                f"/v1/rooms/{ROOM_ID}/messages/{created['id']}",
                headers=_operator(),
                json={"content": "Post-restore write verified"},
            ),
            200,
        )
        _expect(
            "delete after restore",
            client.delete(f"/v1/rooms/{ROOM_ID}/messages/{created['id']}", headers=_operator()),
            204,
        )
        _expect(
            "unarchive after restore",
            client.patch(f"/v1/rooms/{ARCHIVED_ROOM_ID}", headers=_operator(), json={"archived": False}),
            200,
        )
        _expect(
            "rearchive after restore",
            client.patch(f"/v1/rooms/{ARCHIVED_ROOM_ID}", headers=_operator(), json={"archived": True}),
            200,
        )
    return {"mode": "verify", "schema": 8, "post_restore_writes": 5, **state}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=tuple(DATABASE_BY_MODE))
    args = parser.parse_args()
    mode: Literal["seed", "verify"] = args.mode
    reject_libpq_routing_environment(os.environ)
    conninfo = validated_rehearsal_url(
        os.getenv(URL_ENVIRONMENT),
        mode,
        os.getenv(CONFIRM_ENVIRONMENT),
    )
    result = seed(conninfo) if mode == "seed" else verify(conninfo)
    print(json.dumps(result, separators=(",", ":"), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
