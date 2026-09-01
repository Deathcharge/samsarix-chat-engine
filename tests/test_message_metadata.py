# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Bounded application metadata across validation, transports, persistence, and deletion."""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from samsarix_chat_engine import Settings, create_app
from samsarix_chat_engine.models import Message, MessageCreate


@pytest.mark.parametrize(
    "metadata,match",
    [
        ({"Bad": "key"}, "lowercase ASCII"),
        ({"nested": {"unsafe": True}}, "valid string"),
        ({"items": ["unsafe"]}, "valid string"),
        ({"number": float("nan")}, "finite"),
        ({"number": 9_007_199_254_740_992}, "JavaScript"),
        ({"number": 9_007_199_254_740_992.0}, "JavaScript"),
        ({f"key_{index}": index for index in range(21)}, "at most 20"),
        ({"context": "é" * 2_050}, "4096"),
    ],
)
def test_metadata_contract_rejects_unsafe_or_unbounded_values(metadata: object, match: str) -> None:
    with pytest.raises(ValidationError, match=match):
        MessageCreate(content="hello", metadata=metadata)  # type: ignore[arg-type]


def test_http_metadata_round_trip_update_clear_idempotency_and_export(
    client: TestClient,
    room: dict[str, str],
) -> None:
    created = client.post(
        "/v1/rooms/general/messages",
        headers={"Idempotency-Key": "ticket-42"},
        json={
            "sender": "Agent",
            "content": "Investigating",
            "metadata": {"ticket.id": "SUP-42", "priority": 2, "customer_visible": True},
        },
    )
    replay = client.post(
        "/v1/rooms/general/messages",
        headers={"Idempotency-Key": "ticket-42"},
        json={"sender": "Agent", "content": "ignored", "metadata": {"ticket.id": "OTHER"}},
    )
    preserved = client.patch(
        f"/v1/rooms/general/messages/{created.json()['id']}",
        json={"content": "Still investigating"},
    )
    cleared = client.patch(
        f"/v1/rooms/general/messages/{created.json()['id']}",
        json={"content": "Resolved", "metadata": {}},
    )
    history = client.get("/v1/rooms/general/messages").json()["items"]
    export = [json.loads(line) for line in client.get("/v1/rooms/general/export").text.splitlines()]

    expected = {"customer_visible": True, "priority": 2, "ticket.id": "SUP-42"}
    assert created.status_code == 201 and created.json()["metadata"] == expected
    assert replay.status_code == 200 and replay.json() == created.json()
    assert preserved.json()["metadata"] == expected
    assert cleared.json()["metadata"] == {}
    assert history[0]["metadata"] == {}
    assert export[0]["schema_version"] == 7
    assert export[1]["message"]["metadata"] == {}


def test_websocket_metadata_is_broadcast_and_recovered(client: TestClient, room: dict[str, str]) -> None:
    with client.websocket_connect("/v1/rooms/general/ws?username=Agent") as websocket:
        websocket.receive_json()
        websocket.receive_json()
        websocket.send_json(
            {
                "type": "message",
                "content": "Runbook applied",
                "metadata": {"incident.severity": "sev2", "runbook": "cache-failover"},
            }
        )
        event = websocket.receive_json()

    assert event["type"] == "message.created"
    assert event["message"]["metadata"] == {
        "incident.severity": "sev2",
        "runbook": "cache-failover",
    }
    assert client.get("/v1/rooms/general/messages").json()["items"][0] == event["message"]


def test_tombstone_clears_metadata_and_model_rejects_retention(settings: Settings) -> None:
    with TestClient(create_app(settings)) as client:
        client.post("/v1/rooms", json={"id": "general", "name": "General"})
        created = client.post(
            "/v1/rooms/general/messages",
            json={"sender": "Agent", "content": "Sensitive", "metadata": {"ticket.id": "SUP-9"}},
        ).json()
        assert client.delete(f"/v1/rooms/general/messages/{created['id']}").status_code == 204
        tombstone = client.get("/v1/rooms/general/messages").json()["items"][0]

    assert tombstone["content"] == "" and tombstone["metadata"] == {}
    with pytest.raises(ValidationError, match="cannot retain application metadata"):
        Message.model_validate({**tombstone, "metadata": {"ticket.id": "SUP-9"}})


def test_schema_v8_migration_adds_empty_metadata_without_rewriting_messages(tmp_path) -> None:
    database = tmp_path / "v8.db"
    created_at = datetime.now(timezone.utc).isoformat()
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.executescript(
            """
            CREATE TABLE rooms (
                id TEXT PRIMARY KEY, name TEXT NOT NULL, description TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL, archived_at TEXT, frozen_at TEXT
            );
            CREATE TABLE messages (
                id TEXT PRIMARY KEY, room_id TEXT NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
                sender TEXT NOT NULL, author_subject TEXT, content TEXT NOT NULL, created_at TEXT NOT NULL,
                client_message_id TEXT, parent_message_id TEXT, reactions_json TEXT NOT NULL DEFAULT '[]',
                pinned_at TEXT, pinned_by TEXT, edited_at TEXT, deleted_at TEXT,
                UNIQUE(room_id, client_message_id)
            );
            PRAGMA user_version = 8;
            """
        )
        connection.execute("INSERT INTO rooms VALUES (?, ?, ?, ?, NULL, NULL)", ("legacy", "Legacy", "", created_at))
        connection.execute(
            "INSERT INTO messages VALUES (?, ?, ?, ?, ?, ?, ?, NULL, '[]', NULL, NULL, NULL, NULL)",
            ("message-1", "legacy", "Agent", None, "preserved", created_at, None),
        )

    with TestClient(create_app(Settings(database_path=database))) as client:
        message = client.get("/v1/rooms/legacy/messages").json()["items"][0]

    with closing(sqlite3.connect(database)) as connection:
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        stored = connection.execute("SELECT metadata_json FROM messages WHERE id = 'message-1'").fetchone()[0]
    assert version == 10
    assert message["content"] == "preserved" and message["metadata"] == {}
    assert stored == "{}"
