# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Bounded host-owned attachment references across transports and lifecycle boundaries."""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from samsarix_chat_engine import AttachmentReference, Settings, create_app
from samsarix_chat_engine.models import Message, MessageCreate

ATTACHMENT = {
    "id": "upload:SUP-42:console-log",
    "name": "console-log.txt",
    "media_type": "text/plain",
    "size_bytes": 1842,
    "sha256": "a" * 64,
}


@pytest.mark.parametrize(
    "attachments,match",
    [
        ([{**ATTACHMENT, "id": "bad id"}], "string_pattern_mismatch"),
        ([{**ATTACHMENT, "media_type": "Text/Plain"}], "string_pattern_mismatch"),
        ([{**ATTACHMENT, "name": "bad\nname"}], "control characters"),
        ([{**ATTACHMENT, "size_bytes": 9_007_199_254_740_992}], "less_than_equal"),
        ([{**ATTACHMENT, "sha256": "A" * 64}], "string_pattern_mismatch"),
        ([ATTACHMENT, ATTACHMENT], "unique"),
        ([{**ATTACHMENT, "id": f"file-{index}"} for index in range(6)], "at most 5"),
    ],
)
def test_attachment_contract_rejects_unsafe_or_unbounded_values(attachments: object, match: str) -> None:
    with pytest.raises(ValidationError, match=match):
        MessageCreate(content="evidence", attachments=attachments)  # type: ignore[arg-type]


def test_message_requires_text_or_an_attachment() -> None:
    with pytest.raises(ValidationError, match="content or at least one attachment"):
        MessageCreate(content="")
    assert MessageCreate(attachments=[ATTACHMENT]).content == ""  # type: ignore[list-item]
    assert AttachmentReference.model_validate(ATTACHMENT).id == ATTACHMENT["id"]


def test_http_attachment_only_round_trip_idempotency_edit_and_export(
    client: TestClient,
    room: dict[str, str],
) -> None:
    created = client.post(
        "/v1/rooms/general/messages",
        headers={"Idempotency-Key": "attachment-42"},
        json={"sender": "Agent", "attachments": [ATTACHMENT]},
    )
    replay = client.post(
        "/v1/rooms/general/messages",
        headers={"Idempotency-Key": "attachment-42"},
        json={
            "sender": "Agent",
            "content": "ignored",
            "attachments": [{**ATTACHMENT, "id": "different-file"}],
        },
    )
    edited = client.patch(
        f"/v1/rooms/general/messages/{created.json()['id']}",
        json={"content": "Collected console evidence"},
    )
    history = client.get("/v1/rooms/general/messages").json()["items"]
    export = [json.loads(line) for line in client.get("/v1/rooms/general/export").text.splitlines()]

    assert created.status_code == 201 and created.json()["content"] == ""
    assert created.json()["attachments"] == [ATTACHMENT]
    assert replay.status_code == 200 and replay.json() == created.json()
    assert edited.json()["attachments"] == [ATTACHMENT]
    assert history[0]["attachments"] == [ATTACHMENT]
    assert export[0]["schema_version"] == 8
    assert export[1]["message"]["attachments"] == [ATTACHMENT]


def test_websocket_attachment_only_is_broadcast_and_recovered(client: TestClient, room: dict[str, str]) -> None:
    with client.websocket_connect("/v1/rooms/general/ws?username=Agent") as websocket:
        websocket.receive_json()
        websocket.receive_json()
        websocket.send_json({"type": "message", "attachments": [ATTACHMENT]})
        event = websocket.receive_json()

    assert event["type"] == "message.created"
    assert event["message"]["content"] == ""
    assert event["message"]["attachments"] == [ATTACHMENT]
    assert client.get("/v1/rooms/general/messages").json()["items"][0] == event["message"]


def test_tombstone_clears_attachment_references_and_model_rejects_retention(settings: Settings) -> None:
    with TestClient(create_app(settings)) as client:
        client.post("/v1/rooms", json={"id": "general", "name": "General"})
        created = client.post(
            "/v1/rooms/general/messages",
            json={"sender": "Agent", "content": "Sensitive evidence", "attachments": [ATTACHMENT]},
        ).json()
        assert client.delete(f"/v1/rooms/general/messages/{created['id']}").status_code == 204
        tombstone = client.get("/v1/rooms/general/messages").json()["items"][0]

    assert tombstone["content"] == "" and tombstone["attachments"] == []
    with pytest.raises(ValidationError, match="cannot retain attachment references"):
        Message.model_validate({**tombstone, "attachments": [ATTACHMENT]})


def test_schema_v9_migration_adds_empty_attachment_references_without_rewriting_messages(tmp_path) -> None:
    database = tmp_path / "v9.db"
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
                pinned_at TEXT, pinned_by TEXT, metadata_json TEXT NOT NULL DEFAULT '{}',
                edited_at TEXT, deleted_at TEXT, UNIQUE(room_id, client_message_id)
            );
            PRAGMA user_version = 9;
            """
        )
        connection.execute("INSERT INTO rooms VALUES (?, ?, ?, ?, NULL, NULL)", ("legacy", "Legacy", "", created_at))
        connection.execute(
            "INSERT INTO messages VALUES (?, ?, ?, ?, ?, ?, ?, NULL, '[]', NULL, NULL, '{}', NULL, NULL)",
            ("message-1", "legacy", "Agent", None, "preserved", created_at, None),
        )

    with TestClient(create_app(Settings(database_path=database))) as client:
        message = client.get("/v1/rooms/legacy/messages").json()["items"][0]

    with closing(sqlite3.connect(database)) as connection:
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        stored = connection.execute("SELECT attachments_json FROM messages WHERE id = 'message-1'").fetchone()[0]
    assert version == 11
    assert message["content"] == "preserved" and message["attachments"] == []
    assert stored == "[]"
