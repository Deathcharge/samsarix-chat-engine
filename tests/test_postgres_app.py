# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Real-process application tests for the guarded PostgreSQL runtime."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

pytest.importorskip("psycopg")

from samsarix_chat_engine import Settings, create_app  # noqa: E402

pytestmark = pytest.mark.postgres


def _settings(conninfo: str, instance_id: str) -> Settings:
    return Settings(
        storage_backend="postgres",
        postgres_url=conninfo,
        postgres_instance_id=instance_id,
        postgres_relay_poll_seconds=0.01,
        postgres_maintenance_interval_seconds=0.1,
        api_key="postgres-operator-key-1234",
        max_rooms=10,
        max_stored_messages=20,
        max_stored_messages_per_room=10,
        max_connections=4,
        max_connections_per_room=4,
    )


@pytest.mark.asyncio
async def test_two_app_instances_share_http_websocket_presence_typing_and_messages(
    clean_postgres_database: str,
) -> None:
    first_app = create_app(_settings(clean_postgres_database, "app-first"))
    second_app = create_app(_settings(clean_postgres_database, "app-second"))
    headers = {"X-API-Key": "postgres-operator-key-1234"}

    with TestClient(first_app) as first_client, TestClient(second_app) as second_client:
        created = first_client.post(
            "/v1/rooms",
            headers=headers,
            json={"id": "general", "name": "General"},
        )
        assert created.status_code == 201
        assert second_client.get("/v1/rooms/general", headers=headers).status_code == 200

        with first_client.websocket_connect(
            "/v1/rooms/general/ws?username=Alice",
            headers=headers,
        ) as alice:
            assert alice.receive_json()["type"] == "ready"
            assert alice.receive_json()["type"] == "history"
            with second_client.websocket_connect(
                "/v1/rooms/general/ws?username=Bob",
                headers=headers,
            ) as bob:
                assert bob.receive_json()["type"] == "ready"
                assert bob.receive_json()["type"] == "history"
                joined = alice.receive_json()
                assert joined["type"] == "presence.joined"
                assert joined["username"] == "Bob"
                assert joined["active_connections"] == 2
                assert first_client.get("/v1/stats", headers=headers).json() == {"active_connections": 2}

                bob.send_json({"type": "typing", "active": True})
                typing_started = alice.receive_json()
                assert typing_started == {
                    "type": "typing.started",
                    "username": "Bob",
                    "expires_in": 8.0,
                }
                bob.send_json({"type": "typing", "active": False})
                assert alice.receive_json() == {"type": "typing.stopped", "username": "Bob"}

                bob.send_json({"type": "message", "content": "Across processes", "client_message_id": "cross-1"})
                alice_message = alice.receive_json()
                bob_message = bob.receive_json()
                assert alice_message["type"] == "message.created"
                assert alice_message == bob_message
                assert alice_message["message"]["content"] == "Across processes"
                assert alice_message["idempotent_replay"] is False

            left = alice.receive_json()
            assert left["type"] == "presence.left"
            assert left["username"] == "Bob"
            assert left["active_connections"] == 1

        assert second_client.get("/v1/stats", headers=headers).json() == {"active_connections": 0}
