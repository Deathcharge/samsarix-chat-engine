"""Real application fixtures for HTTP, WebSocket, and persistence tests."""

from collections.abc import Iterator

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from helix_chat_engine import Settings, create_app


@pytest.fixture
def settings(tmp_path) -> Settings:
    return Settings(
        database_path=tmp_path / "chat.db",
        max_message_chars=64,
        max_connections=10,
        max_connections_per_room=5,
        messages_per_minute=20,
        max_rooms=10,
        max_stored_messages=20,
        max_stored_messages_per_room=10,
    )


@pytest.fixture
def app(settings: Settings) -> FastAPI:
    return create_app(settings)


@pytest.fixture
def client(app: FastAPI) -> Iterator[TestClient]:
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def room(client: TestClient) -> dict[str, str]:
    response = client.post(
        "/v1/rooms",
        json={"id": "general", "name": "General", "description": "Release chat"},
    )
    assert response.status_code == 201
    return response.json()
