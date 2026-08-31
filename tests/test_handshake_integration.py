# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Committed HTTP mutations during a paused real WebSocket history snapshot."""

import asyncio
import threading
from dataclasses import replace

import pytest
from fastapi.testclient import TestClient

from samsarix_chat_engine import Settings, create_app


def _exercise_snapshot_gap(settings, monkeypatch):
    application = create_app(settings)
    captured = threading.Event()
    dispatched = threading.Event()
    resume = asyncio.Event()
    headers = {"X-API-Key": settings.api_key}
    with TestClient(application) as client:
        assert client.post("/v1/rooms", headers=headers, json={"id": "room", "name": "Room"}).status_code == 201
        original_message = client.post(
            "/v1/rooms/room/messages", headers=headers, json={"sender": "Operator", "content": "original"}
        )
        assert original_message.status_code == 201
        message_id = original_message.json()["id"]
        store = application.state.store
        connections = application.state.connections
        history = store.list_messages
        broadcast = connections.broadcast

        async def paused_history(*args, **kwargs):
            snapshot = await history(*args, **kwargs)
            captured.set()
            await resume.wait()
            return snapshot

        async def observed_broadcast(room_id, event, **kwargs):
            await broadcast(room_id, event, **kwargs)
            if event["type"] == "message.deleted":
                dispatched.set()

        monkeypatch.setattr(store, "list_messages", paused_history)
        monkeypatch.setattr(connections, "broadcast", observed_broadcast)
        try:
            with client.websocket_connect("/v1/rooms/room/ws?username=Reader", headers=headers) as websocket:
                assert captured.wait(5), "history snapshot was not captured"
                created = client.post(
                    "/v1/rooms/room/messages", headers=headers, json={"sender": "Operator", "content": "during history"}
                )
                assert created.status_code == 201
                edited = client.patch(
                    f"/v1/rooms/room/messages/{message_id}", headers=headers, json={"content": "edited during history"}
                )
                assert edited.status_code == 200
                assert client.delete(f"/v1/rooms/room/messages/{message_id}", headers=headers).status_code == 204
                assert dispatched.wait(5), "committed delete did not reach the pending socket"
                client.portal.call(resume.set)
                assert websocket.receive_json()["type"] == "ready"
                initial = websocket.receive_json()
                assert initial["type"] == "history"
                assert initial["items"] == [original_message.json()]
                # The PostgreSQL relay can also replay the original create after
                # registration. Merge by ID; never assume history/live are disjoint.
                merged = {item["id"]: item for item in initial["items"]}
                seen = []
                while "message.deleted" not in seen:
                    event = websocket.receive_json()
                    assert event["type"] in {"message.created", "message.updated", "message.deleted"}
                    merged[event["message"]["id"]] = event["message"]
                    seen.append(event["type"])
                assert seen[-3:] == ["message.created", "message.updated", "message.deleted"]
                monkeypatch.setattr(store, "list_messages", history)
                durable = client.get("/v1/rooms/room/messages", headers=headers).json()["items"]
                assert merged == {item["id"]: item for item in durable}
                assert merged[message_id]["content"] == ""
                assert merged[message_id]["deleted_at"] is not None
                assert merged[created.json()["id"]]["content"] == "during history"
                websocket.send_json({"type": "ping"})
                assert websocket.receive_json() == {"type": "pong"}
            assert connections._pending_bytes == 0
            assert connections.active_connections == 0
            if application.state.postgres_runtime is not None:

                async def lease_rows():
                    async with store.foundation.transaction() as connection:
                        cursor = await connection.execute("SELECT COUNT(*) FROM public.samsarix_connection_leases")
                        return (await cursor.fetchone())[0]

                assert client.portal.call(lease_rows) == 0
        finally:
            client.portal.call(resume.set)


def test_sqlite_history_snapshot_and_queued_live_events_converge(settings, monkeypatch):
    _exercise_snapshot_gap(replace(settings, api_key="handshake-integration-operator-key"), monkeypatch)


@pytest.mark.postgres
def test_postgres_history_snapshot_and_queued_live_events_converge(clean_postgres_database, monkeypatch):
    _exercise_snapshot_gap(
        Settings(
            storage_backend="postgres",
            postgres_url=clean_postgres_database,
            postgres_instance_id="handshake-buffer",
            postgres_relay_poll_seconds=0.01,
            postgres_maintenance_interval_seconds=0.1,
            api_key="handshake-integration-operator-key",
        ),
        monkeypatch,
    )
