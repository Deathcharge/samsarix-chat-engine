"""Data-lifecycle, audit, retention, and schema-migration integration tests."""

import json
import logging
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from samsarix_chat_engine import AccessTokenService, ChatStore, RoomCreate, Settings, create_app
from samsarix_chat_engine.store import UnsupportedSchemaVersionError


def test_archive_export_delete_and_metadata_only_audit(client: TestClient, room: dict[str, str]) -> None:
    private_content = "private-message-that-must-not-enter-the-audit-log"
    created = client.post(
        "/v1/rooms/general/messages",
        json={"sender": "Andrew", "content": private_content},
    )
    assert created.status_code == 201

    active_delete = client.delete(
        "/v1/rooms/general",
        headers={"X-Confirm-Room-Delete": "general"},
    )
    missing_confirmation = client.delete("/v1/rooms/general")
    exported = client.get("/v1/rooms/general/export")
    lines = [json.loads(line) for line in exported.text.splitlines()]

    assert active_delete.status_code == 409
    assert active_delete.json()["error"]["code"] == "room_not_archived"
    assert missing_confirmation.status_code == 400
    assert missing_confirmation.json()["error"]["code"] == "deletion_confirmation_required"
    assert exported.status_code == 200
    assert exported.headers["content-type"].startswith("application/x-ndjson")
    assert exported.headers["content-disposition"] == 'attachment; filename="general-messages.ndjson"'
    assert lines[0]["type"] == "samsarix.room_export"
    assert lines[0]["schema_version"] == 3
    assert lines[0]["room"]["id"] == "general"
    assert lines[1] == {"type": "message", "message": created.json()}

    archived = client.patch("/v1/rooms/general", json={"archived": True})
    unchanged_archive = client.patch("/v1/rooms/general", json={"archived": True})
    blocked_write = client.post(
        "/v1/rooms/general/messages",
        json={"sender": "Andrew", "content": "blocked"},
    )
    deleted = client.delete(
        "/v1/rooms/general",
        headers={"X-Confirm-Room-Delete": "general"},
    )
    audit = client.get("/v1/admin/audit-events").json()

    assert archived.status_code == 200
    assert archived.json()["archived_at"] is not None
    assert unchanged_archive.json() == archived.json()
    assert blocked_write.status_code == 409
    assert blocked_write.json()["error"]["code"] == "room_archived"
    assert deleted.status_code == 204
    assert deleted.content == b""
    assert client.get("/v1/rooms/general").status_code == 404
    assert [event["action"] for event in audit["items"]] == [
        "room.created",
        "room.export_requested",
        "room.archived",
        "room.deleted",
    ]
    assert audit["items"][-1]["details"] == {"deleted_messages": 1}
    assert private_content not in json.dumps(audit)


def test_export_streams_across_storage_batches(tmp_path) -> None:
    database = tmp_path / "large-export.db"
    with TestClient(create_app(Settings(database_path=database))) as client:
        client.post("/v1/rooms", json={"id": "general", "name": "General"})
        timestamp = datetime.now(timezone.utc).isoformat()
        rows = [
            (f"message-{index:04d}", "general", "writer", f"content-{index:04d}", timestamp, None)
            for index in range(1_005)
        ]
        with closing(sqlite3.connect(database)) as connection, connection:
            connection.executemany(
                """
                INSERT INTO messages (id, room_id, sender, content, created_at, client_message_id)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

        exported = client.get("/v1/rooms/general/export")

    lines = exported.text.splitlines()
    assert len(lines) == 1_006
    assert json.loads(lines[1])["message"]["content"] == "content-0000"
    assert json.loads(lines[-1])["message"]["content"] == "content-1004"
    # Windows refuses to rename a file with an open handle, so this verifies
    # that the completed export released its SQLite snapshot connection.
    moved = database.with_suffix(".moved")
    database.rename(moved)
    moved.rename(database)


async def test_export_snapshot_survives_concurrent_room_deletion(tmp_path) -> None:
    store = ChatStore(
        tmp_path / "snapshot.db",
        max_rooms=10,
        max_stored_messages=100,
        max_stored_messages_per_room=100,
    )
    await store.initialize()
    await store.create_room(RoomCreate(id="general", name="General"))
    await store.create_message(
        room_id="general",
        sender="Andrew",
        content="snapshot-content",
        client_message_id=None,
        allow_frozen=True,
    )
    await store.set_room_archived("general", archived=True, actor="operator")

    snapshot = await store.prepare_export("general", actor="operator")
    await store.delete_room("general", actor="operator")

    assert [message.content for message in snapshot] == ["snapshot-content"]


def test_archive_closes_connected_clients_and_unarchive_reopens(client: TestClient, room: dict[str, str]) -> None:
    with pytest.raises(WebSocketDisconnect) as archived_close:
        with client.websocket_connect("/v1/rooms/general/ws?username=Andrew") as websocket:
            websocket.receive_json()
            websocket.receive_json()
            archived = client.patch("/v1/rooms/general", json={"archived": True})
            assert archived.status_code == 200
            event = websocket.receive_json()
            assert event["type"] == "room.archived"
            websocket.receive_json()
    assert archived_close.value.code == 4409
    assert client.get("/v1/stats").json() == {"active_connections": 0}

    with pytest.raises(WebSocketDisconnect) as rejected:
        with client.websocket_connect("/v1/rooms/general/ws?username=Andrew") as websocket:
            assert websocket.receive_json()["code"] == "room_archived"
            websocket.receive_json()
    assert rejected.value.code == 4409

    reopened = client.patch("/v1/rooms/general", json={"archived": False})
    assert reopened.status_code == 200
    assert reopened.json()["archived_at"] is None
    with client.websocket_connect("/v1/rooms/general/ws?username=Andrew") as websocket:
        assert websocket.receive_json()["type"] == "ready"
        assert websocket.receive_json()["type"] == "history"


def test_lifecycle_endpoints_require_admin_token(tmp_path) -> None:
    secret = "test-only-signing-secret-that-is-long-enough"
    operator_key = "test-only-operator-api-key"
    settings = Settings(
        database_path=tmp_path / "admin.db",
        api_key=operator_key,
        token_signing_secret=secret,
        token_issuer="test-issuer",
        token_audience="test-audience",
        token_clock_skew_seconds=0,
    )
    service = AccessTokenService(secret, issuer="test-issuer", audience="test-audience", clock_skew_seconds=0)
    token = service.issue(
        "room-user",
        rooms=["general"],
        permissions=["room:read", "room:write"],
        expires_in_seconds=300,
    )
    user_headers = {"Authorization": f"Bearer {token}"}
    operator_headers = {"X-API-Key": operator_key}

    with TestClient(create_app(settings)) as client:
        assert (
            client.post("/v1/rooms", headers=operator_headers, json={"id": "general", "name": "General"}).status_code
            == 201
        )
        assert client.get("/v1/rooms/general/export", headers=user_headers).status_code == 403
        assert client.patch("/v1/rooms/general", headers=user_headers, json={"archived": True}).status_code == 403
        assert client.get("/v1/admin/audit-events", headers=user_headers).status_code == 403
        assert client.post("/v1/admin/retention/run", headers=user_headers).status_code == 403
        assert client.get("/v1/rooms/general", headers=user_headers).status_code == 200


def test_retention_deletes_old_messages_and_records_count(tmp_path) -> None:
    database = tmp_path / "retention.db"
    settings = Settings(database_path=database, message_retention_days=1)
    with TestClient(create_app(settings)) as client:
        client.post("/v1/rooms", json={"id": "general", "name": "General"})
        old = client.post(
            "/v1/rooms/general/messages",
            json={"sender": "Andrew", "content": "old"},
        ).json()
        client.post(
            "/v1/rooms/general/messages",
            json={"sender": "Andrew", "content": "current"},
        )
        old_timestamp = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
        with closing(sqlite3.connect(database)) as connection, connection:
            connection.execute("UPDATE messages SET created_at = ? WHERE id = ?", (old_timestamp, old["id"]))

        result = client.post("/v1/admin/retention/run")
        history = client.get("/v1/rooms/general/messages").json()["items"]
        audit = client.get("/v1/admin/audit-events").json()["items"]

    assert result.status_code == 200
    assert result.json()["deleted_messages"] == 1
    assert [message["content"] for message in history] == ["current"]
    assert audit[-1]["action"] == "retention.executed"
    assert audit[-1]["details"]["deleted_messages"] == 1


def test_message_commit_audits_automatic_cross_room_retention(tmp_path) -> None:
    database = tmp_path / "automatic-retention.db"
    settings = Settings(database_path=database, message_retention_days=1)
    with TestClient(create_app(settings)) as client:
        client.post("/v1/rooms", json={"id": "first", "name": "First"})
        expired = client.post(
            "/v1/rooms/first/messages",
            json={"sender": "Andrew", "content": "expired"},
        ).json()
        client.post(
            "/v1/rooms/first/messages",
            json={"sender": "Andrew", "content": "current"},
        )
        old_timestamp = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
        with closing(sqlite3.connect(database)) as connection, connection:
            connection.execute("UPDATE messages SET created_at = ? WHERE id = ?", (old_timestamp, expired["id"]))
        client.post("/v1/rooms", json={"id": "second", "name": "Second"})

        trigger = client.post(
            "/v1/rooms/second/messages",
            json={"sender": "Andrew", "content": "trigger"},
        )
        first_history = client.get("/v1/rooms/first/messages").json()["items"]
        second_history = client.get("/v1/rooms/second/messages").json()["items"]
        audit = client.get("/v1/admin/audit-events").json()["items"]

    assert trigger.status_code == 201
    assert [message["content"] for message in first_history] == ["current"]
    assert [message["content"] for message in second_history] == ["trigger"]
    automatic = audit[-1]
    assert automatic["action"] == "retention.automatic"
    assert automatic["actor"] == "system:retention"
    assert {key: value for key, value in automatic["details"].items() if key != "cutoff"} == {
        "age_deleted": 1,
        "global_cap_deleted": 0,
        "room_cap_deleted": 0,
        "trigger_room_id": "second",
    }
    assert datetime.fromisoformat(automatic["details"]["cutoff"]).tzinfo is not None


def test_retention_must_be_configured_and_audit_cursor_is_validated(client: TestClient, room: dict[str, str]) -> None:
    retention = client.post("/v1/admin/retention/run")
    invalid_cursor = client.get("/v1/admin/audit-events", params={"before": "missing"})

    assert retention.status_code == 409
    assert retention.json()["error"]["code"] == "retention_not_configured"
    assert invalid_cursor.status_code == 400
    assert invalid_cursor.json()["error"]["code"] == "invalid_cursor"


def test_v1_database_migrates_in_place(tmp_path, caplog: pytest.LogCaptureFixture) -> None:
    database = tmp_path / "v1.db"
    created_at = datetime.now(timezone.utc).isoformat()
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.executescript(
            """
            CREATE TABLE rooms (
                id TEXT PRIMARY KEY, name TEXT NOT NULL, description TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL
            );
            CREATE TABLE messages (
                id TEXT PRIMARY KEY,
                room_id TEXT NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
                sender TEXT NOT NULL,
                content TEXT NOT NULL,
                created_at TEXT NOT NULL,
                client_message_id TEXT,
                UNIQUE(room_id, client_message_id)
            );
            PRAGMA user_version = 1;
            """
        )
        connection.execute("INSERT INTO rooms VALUES (?, ?, ?, ?)", ("legacy", "Legacy", "", created_at))
        connection.execute(
            "INSERT INTO messages VALUES (?, ?, ?, ?, ?, ?)",
            ("message-1", "legacy", "Andrew", "preserved", created_at, None),
        )

    caplog.set_level(logging.INFO, logger="samsarix_chat_engine.store")
    with TestClient(create_app(Settings(database_path=database))) as client:
        room = client.get("/v1/rooms/legacy")
        history = client.get("/v1/rooms/legacy/messages")

    with closing(sqlite3.connect(database)) as connection:
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        columns = {row[1] for row in connection.execute("PRAGMA table_info(rooms)")}
        message_columns = {row[1] for row in connection.execute("PRAGMA table_info(messages)")}
        read_state_table = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'room_read_states'"
        ).fetchone()
        webhook_table = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'webhook_deliveries'"
        ).fetchone()

    assert room.status_code == 200
    assert room.json()["archived_at"] is None
    assert history.json()["items"][0]["content"] == "preserved"
    assert version == 6
    assert {"archived_at", "frozen_at"} <= columns
    assert {"edited_at", "deleted_at", "author_subject", "parent_message_id"} <= message_columns
    assert read_state_table is not None
    assert webhook_table is not None
    assert "Migrating database schema from version 1 to 6" in caplog.text


def test_v2_database_migrates_conversation_controls_in_place(tmp_path) -> None:
    database = tmp_path / "v2.db"
    created_at = datetime.now(timezone.utc).isoformat()
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.executescript(
            """
            CREATE TABLE rooms (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                archived_at TEXT
            );
            CREATE TABLE messages (
                id TEXT PRIMARY KEY,
                room_id TEXT NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
                sender TEXT NOT NULL,
                content TEXT NOT NULL,
                created_at TEXT NOT NULL,
                client_message_id TEXT,
                UNIQUE(room_id, client_message_id)
            );
            CREATE TABLE audit_events (
                id TEXT PRIMARY KEY,
                action TEXT NOT NULL,
                actor TEXT NOT NULL,
                room_id TEXT,
                created_at TEXT NOT NULL,
                details_json TEXT NOT NULL DEFAULT '{}'
            );
            PRAGMA user_version = 2;
            """
        )
        connection.execute(
            "INSERT INTO rooms VALUES (?, ?, ?, ?, ?)",
            ("legacy", "Legacy", "", created_at, None),
        )
        connection.execute(
            "INSERT INTO messages VALUES (?, ?, ?, ?, ?, ?)",
            ("message-1", "legacy", "author", "preserved", created_at, None),
        )

    with TestClient(create_app(Settings(database_path=database))) as client:
        room = client.get("/v1/rooms/legacy").json()
        message = client.get("/v1/rooms/legacy/messages").json()["items"][0]
        frozen = client.patch("/v1/rooms/legacy", json={"frozen": True})
        moderated = client.patch(
            "/v1/rooms/legacy/members/author/moderation",
            json={"muted_for_seconds": 60},
        )

    with closing(sqlite3.connect(database)) as connection:
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        controls_table = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'room_member_controls'"
        ).fetchone()
        read_state_table = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'room_read_states'"
        ).fetchone()
        webhook_table = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'webhook_deliveries'"
        ).fetchone()
        message_columns = {row[1] for row in connection.execute("PRAGMA table_info(messages)")}

    assert version == 6
    assert controls_table is not None
    assert read_state_table is not None
    assert webhook_table is not None
    assert "author_subject" in message_columns
    assert "parent_message_id" in message_columns
    assert room["frozen_at"] is None
    assert message["content"] == "preserved"
    assert message["edited_at"] is None
    assert message["deleted_at"] is None
    assert frozen.status_code == 200
    assert frozen.json()["frozen_at"] is not None
    assert moderated.status_code == 200
    assert moderated.json()["muted_until"] is not None


def test_newer_database_schema_is_refused_without_mutation(tmp_path) -> None:
    database = tmp_path / "future.db"
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.execute("PRAGMA user_version = 7")

    with pytest.raises(UnsupportedSchemaVersionError, match="newer than supported"):
        with TestClient(create_app(Settings(database_path=database))):
            pass

    with closing(sqlite3.connect(database)) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 7


@pytest.mark.asyncio
async def test_sqlite_store_close_is_idempotent(tmp_path: Path) -> None:
    store = ChatStore(
        tmp_path / "close.db",
        max_rooms=10,
        max_stored_messages=100,
        max_stored_messages_per_room=50,
    )
    await store.initialize()

    await store.close()
    await store.close()

    assert await store.check_ready()
