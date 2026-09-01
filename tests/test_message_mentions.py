# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Bounded host-resolved message mentions across transports and privacy boundaries."""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from samsarix_chat_engine import Settings, create_app
from samsarix_chat_engine.models import Message, MessageCreate, MessageUpdate, WebSocketMessage


@pytest.mark.parametrize(
    "mentioned_subjects,match",
    [
        ([f"user-{index}" for index in range(11)], "at most 10"),
        (["oncall", "oncall"], "duplicates"),
        ([" oncall"], "surrounding whitespace"),
        ([""], "between 1 and 64"),
        (["x" * 65], "between 1 and 64"),
        ([7], "contain strings"),
        (("oncall",), "must be an array"),
    ],
)
def test_mention_contract_rejects_ambiguous_or_unbounded_subjects(
    mentioned_subjects: object,
    match: str,
) -> None:
    with pytest.raises(ValidationError, match=match):
        MessageCreate(content="Escalating", mentioned_subjects=mentioned_subjects)  # type: ignore[arg-type]


def test_mentions_do_not_replace_required_message_content() -> None:
    with pytest.raises(ValidationError, match="content or at least one attachment"):
        MessageCreate(content="", mentioned_subjects=["oncall"])


def test_http_mentions_round_trip_idempotency_edit_clear_and_export(
    client: TestClient,
    room: dict[str, str],
) -> None:
    created = client.post(
        "/v1/rooms/general/messages",
        headers={"Idempotency-Key": "mention-42"},
        json={"sender": "Agent", "content": "Escalating", "mentioned_subjects": ["oncall", "lead"]},
    )
    replay = client.post(
        "/v1/rooms/general/messages",
        headers={"Idempotency-Key": "mention-42"},
        json={"sender": "Agent", "content": "ignored", "mentioned_subjects": ["other"]},
    )
    preserved = client.patch(
        f"/v1/rooms/general/messages/{created.json()['id']}",
        json={"content": "Still escalating"},
    )
    replaced = client.patch(
        f"/v1/rooms/general/messages/{created.json()['id']}",
        json={"content": "Manager engaged", "mentioned_subjects": ["manager"]},
    )
    cleared = client.patch(
        f"/v1/rooms/general/messages/{created.json()['id']}",
        json={"content": "Resolved", "mentioned_subjects": []},
    )
    history = client.get("/v1/rooms/general/messages").json()["items"]
    export = [json.loads(line) for line in client.get("/v1/rooms/general/export").text.splitlines()]

    assert created.status_code == 201 and created.json()["mentioned_subjects"] == ["oncall", "lead"]
    assert replay.status_code == 200 and replay.json() == created.json()
    assert preserved.json()["mentioned_subjects"] == ["oncall", "lead"]
    assert replaced.json()["mentioned_subjects"] == ["manager"]
    assert cleared.json()["mentioned_subjects"] == []
    assert history[0]["mentioned_subjects"] == []
    assert export[0]["schema_version"] == 8
    assert export[1]["message"]["mentioned_subjects"] == []


def test_websocket_mentions_are_broadcast_and_recovered(client: TestClient, room: dict[str, str]) -> None:
    with client.websocket_connect("/v1/rooms/general/ws?username=Agent") as websocket:
        websocket.receive_json()
        websocket.receive_json()
        websocket.send_json({"type": "message", "content": "Please inspect", "mentioned_subjects": ["oncall", "lead"]})
        event = websocket.receive_json()

    assert event["type"] == "message.created"
    assert event["message"]["mentioned_subjects"] == ["oncall", "lead"]
    assert client.get("/v1/rooms/general/messages").json()["items"][0] == event["message"]


def test_tombstone_clears_mentions_and_models_reject_retention(settings: Settings) -> None:
    with TestClient(create_app(settings)) as client:
        client.post("/v1/rooms", json={"id": "general", "name": "General"})
        created = client.post(
            "/v1/rooms/general/messages",
            json={"sender": "Agent", "content": "Escalating", "mentioned_subjects": ["oncall"]},
        ).json()
        assert client.delete(f"/v1/rooms/general/messages/{created['id']}").status_code == 204
        tombstone = client.get("/v1/rooms/general/messages").json()["items"][0]

    assert tombstone["content"] == "" and tombstone["mentioned_subjects"] == []
    with pytest.raises(ValidationError, match="cannot retain mentioned subjects"):
        Message.model_validate({**tombstone, "mentioned_subjects": ["oncall"]})
    with pytest.raises(ValidationError, match="surrounding whitespace"):
        MessageUpdate(content="edited", mentioned_subjects=[" oncall"])
    with pytest.raises(ValidationError, match="duplicates"):
        WebSocketMessage(type="message", content="hello", mentioned_subjects=["lead", "lead"])


def test_schema_v10_migration_adds_empty_mentions_without_rewriting_messages(tmp_path) -> None:
    database = tmp_path / "v10.db"
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
                attachments_json TEXT NOT NULL DEFAULT '[]', edited_at TEXT, deleted_at TEXT,
                UNIQUE(room_id, client_message_id)
            );
            PRAGMA user_version = 10;
            """
        )
        connection.execute("INSERT INTO rooms VALUES (?, ?, ?, ?, NULL, NULL)", ("legacy", "Legacy", "", created_at))
        connection.execute(
            """
            INSERT INTO messages (
                id, room_id, sender, author_subject, content, created_at, client_message_id,
                parent_message_id, reactions_json, pinned_at, pinned_by, metadata_json,
                attachments_json, edited_at, deleted_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, '[]', NULL, NULL, '{}', '[]', NULL, NULL)
            """,
            ("message-1", "legacy", "Agent", None, "preserved", created_at, None),
        )

    with TestClient(create_app(Settings(database_path=database))) as client:
        message = client.get("/v1/rooms/legacy/messages").json()["items"][0]

    with closing(sqlite3.connect(database)) as connection:
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        stored = connection.execute("SELECT mentioned_subjects_json FROM messages WHERE id = 'message-1'").fetchone()[0]
    assert version == 11
    assert message["content"] == "preserved" and message["mentioned_subjects"] == []
    assert stored == "[]"
