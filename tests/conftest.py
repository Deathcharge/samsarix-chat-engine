"""Real application fixtures for HTTP, WebSocket, and persistence tests."""

import os
from collections.abc import AsyncIterator, Iterator

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from samsarix_chat_engine import Settings, create_app


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


@pytest.fixture
async def clean_postgres_database() -> AsyncIterator[str]:
    """Yield only the dedicated live-test database after a guarded reset."""

    conninfo = os.getenv("SAMSARIX_TEST_POSTGRES_URL")
    if conninfo is None:
        pytest.skip("SAMSARIX_TEST_POSTGRES_URL is not configured")
    await _reset_postgres_test_database(conninfo)
    try:
        yield conninfo
    finally:
        await _reset_postgres_test_database(conninfo)


async def _reset_postgres_test_database(conninfo: str) -> None:
    import psycopg

    async with await psycopg.AsyncConnection.connect(conninfo, autocommit=True) as connection:
        cursor = await connection.execute("SELECT current_database()")
        row = await cursor.fetchone()
        if row is None or row[0] != "samsarix_test":
            raise RuntimeError("live PostgreSQL tests require the dedicated samsarix_test database")
        await connection.execute("DROP TABLE IF EXISTS public.samsarix_instance_cursors")
        await connection.execute("DROP TABLE IF EXISTS public.samsarix_realtime_events")
        await connection.execute("DROP TABLE IF EXISTS public.samsarix_room_read_states")
        await connection.execute("DROP TABLE IF EXISTS public.samsarix_room_member_controls")
        await connection.execute("DROP TABLE IF EXISTS public.samsarix_messages")
        await connection.execute("DROP TABLE IF EXISTS public.samsarix_rooms")
        await connection.execute("DROP TABLE IF EXISTS public.samsarix_audit_events")
        await connection.execute("DROP TABLE IF EXISTS public.samsarix_webhook_deliveries")
        await connection.execute("DROP TABLE IF EXISTS public.samsarix_schema_metadata")
