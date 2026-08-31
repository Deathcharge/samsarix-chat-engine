# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Live startup/migration locks, transaction boundaries and replica continuity."""

import asyncio

import pytest
from fastapi.testclient import TestClient

psycopg = pytest.importorskip("psycopg")

from samsarix_chat_engine import Settings, create_app  # noqa: E402
from samsarix_chat_engine.postgres import (  # noqa: E402
    POSTGRES_MIGRATION_LOCK_ID,
    POSTGRES_SCHEMA_VERSION,
    PostgresFoundation,
    PostgresUnavailableError,
    UnsupportedPostgresSchemaError,
)

pytestmark = pytest.mark.postgres


async def _initialize(conninfo):
    async with PostgresFoundation(conninfo):
        pass


async def _wait_for_migration_waiter(conninfo):
    async with await psycopg.AsyncConnection.connect(conninfo) as observer:
        while True:
            cursor = await observer.execute(
                """
                SELECT EXISTS (
                    SELECT 1 FROM pg_locks
                    WHERE locktype = 'advisory' AND NOT granted
                      AND classid = %s AND objid = %s
                )
                """,
                (POSTGRES_MIGRATION_LOCK_ID >> 32, POSTGRES_MIGRATION_LOCK_ID & 0xFFFFFFFF),
            )
            if (await cursor.fetchone())[0]:
                return
            await asyncio.sleep(0.01)


@pytest.mark.parametrize("lock_mode", ["ACCESS SHARE", "ROW EXCLUSIVE"])
@pytest.mark.asyncio
async def test_current_schema_startup_does_not_wait_for_active_table_access(clean_postgres_database, lock_mode):
    await _initialize(clean_postgres_database)
    service = PostgresFoundation(clean_postgres_database, operation_timeout_seconds=1)
    try:
        async with await psycopg.AsyncConnection.connect(clean_postgres_database) as active:
            # These locks model ordinary SELECT and write/maintenance access.
            # Neither conflicts with inspection, but both block ALTER TABLE.
            await active.execute(
                psycopg.sql.SQL(
                    "LOCK TABLE public.samsarix_instance_cursors, public.samsarix_connection_leases IN {} MODE"
                ).format(psycopg.sql.SQL(lock_mode))
            )
            await asyncio.wait_for(service.open(), 3)
            assert await service.schema_version() == POSTGRES_SCHEMA_VERSION
            cursor = await active.execute("SELECT 1")
            assert await cursor.fetchone() == (1,)
    finally:
        await service.close()


@pytest.mark.parametrize("version", [POSTGRES_SCHEMA_VERSION, POSTGRES_SCHEMA_VERSION + 1])
@pytest.mark.asyncio
async def test_existing_schema_inspection_needs_no_writable_transaction(clean_postgres_database, version):
    await _initialize(clean_postgres_database)
    async with await psycopg.AsyncConnection.connect(clean_postgres_database) as connection:
        await connection.execute("UPDATE public.samsarix_schema_metadata SET version = %s", (version,))
    readonly = psycopg.conninfo.make_conninfo(clean_postgres_database, options="-c default_transaction_read_only=on")
    service = PostgresFoundation(readonly)
    try:
        if version > POSTGRES_SCHEMA_VERSION:
            with pytest.raises(UnsupportedPostgresSchemaError, match="newer"):
                await service.open()
            assert not service._opened
        else:
            await service.open()
            assert await service.schema_version() == version
            async with service.transaction() as connection:
                cursor = await connection.execute("SHOW transaction_read_only")
                assert await cursor.fetchone() == ("on",)
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_current_schema_restarts_preserve_metadata_and_retention_rows(clean_postgres_database):
    await _initialize(clean_postgres_database)

    async def snapshot():
        async with await psycopg.AsyncConnection.connect(clean_postgres_database) as connection:
            cursor = await connection.execute(
                """
                SELECT metadata.version, metadata.updated_at, metadata.xmin::text,
                       retention.pruned_through_sequence, retention.updated_at, retention.xmin::text
                FROM public.samsarix_schema_metadata AS metadata
                CROSS JOIN public.samsarix_realtime_retention AS retention
                """
            )
            return await cursor.fetchone()

    before = await snapshot()
    await asyncio.gather(_initialize(clean_postgres_database), _initialize(clean_postgres_database))
    assert await snapshot() == before


@pytest.mark.asyncio
async def test_blocked_older_schema_migration_rolls_back_then_can_be_retried(clean_postgres_database):
    await _initialize(clean_postgres_database)
    async with await psycopg.AsyncConnection.connect(clean_postgres_database) as connection:
        await connection.execute("UPDATE public.samsarix_schema_metadata SET version = 7")
        # The migration seeds this row before reaching the locked lease table.
        await connection.execute("DELETE FROM public.samsarix_realtime_retention")
    blocked = PostgresFoundation(clean_postgres_database, operation_timeout_seconds=0.3)
    try:
        async with await psycopg.AsyncConnection.connect(clean_postgres_database) as active:
            await active.execute("LOCK TABLE public.samsarix_connection_leases IN ACCESS SHARE MODE")
            with pytest.raises(PostgresUnavailableError):
                await asyncio.wait_for(blocked.open(), 3)
            assert not blocked._opened
            cursor = await active.execute("SELECT version FROM public.samsarix_schema_metadata")
            assert await cursor.fetchone() == (7,)
            cursor = await active.execute("SELECT COUNT(*) FROM public.samsarix_realtime_retention")
            assert await cursor.fetchone() == (0,)
    finally:
        await blocked.close()
    async with PostgresFoundation(clean_postgres_database) as recovered:
        assert await recovered.schema_version() == POSTGRES_SCHEMA_VERSION
        assert await recovered.event_retention_floor() == 0


@pytest.mark.asyncio
async def test_current_schema_inspection_waits_for_migration_lock_and_reads_committed_version(clean_postgres_database):
    await _initialize(clean_postgres_database)
    service = PostgresFoundation(clean_postgres_database)
    opening = None
    try:
        async with await psycopg.AsyncConnection.connect(clean_postgres_database) as migrator:
            await migrator.execute("SELECT pg_advisory_xact_lock(%s)", (POSTGRES_MIGRATION_LOCK_ID,))
            await migrator.execute(
                "UPDATE public.samsarix_schema_metadata SET version = %s", (POSTGRES_SCHEMA_VERSION + 1,)
            )
            opening = asyncio.create_task(service.open())

            # Observe a real waiter instead of relying on a scheduling delay.
            await asyncio.wait_for(_wait_for_migration_waiter(clean_postgres_database), 3)
            assert not opening.done()
        with pytest.raises(UnsupportedPostgresSchemaError, match="newer"):
            await asyncio.wait_for(opening, 3)
        assert not service._opened
    finally:
        if opening is not None:
            opening.cancel()
            await asyncio.gather(opening, return_exceptions=True)
        await service.close()


@pytest.mark.asyncio
async def test_cancelled_migration_lock_wait_closes_the_real_pool(clean_postgres_database):
    await _initialize(clean_postgres_database)
    service = PostgresFoundation(clean_postgres_database)
    opening = None
    try:
        async with await psycopg.AsyncConnection.connect(clean_postgres_database) as migrator:
            await migrator.execute("SELECT pg_advisory_xact_lock(%s)", (POSTGRES_MIGRATION_LOCK_ID,))
            opening = asyncio.create_task(service.open())
            await asyncio.wait_for(_wait_for_migration_waiter(clean_postgres_database), 3)
            opening.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(opening, 3)
            assert service._pool.closed
            assert not service._opened
    finally:
        if opening is not None:
            opening.cancel()
            await asyncio.gather(opening, return_exceptions=True)
        await service.close()


def test_starting_replica_does_not_interrupt_existing_room_session(clean_postgres_database, caplog):
    def app(instance_id):
        return create_app(
            Settings(
                storage_backend="postgres",
                postgres_url=clean_postgres_database,
                postgres_instance_id=instance_id,
                postgres_relay_poll_seconds=0.01,
                postgres_maintenance_interval_seconds=0.01,
                api_key="startup-inspection-operator-key",
            )
        )

    first = app("already-serving")
    headers = {"X-API-Key": "startup-inspection-operator-key"}
    with TestClient(first) as serving:
        assert serving.post("/v1/rooms", headers=headers, json={"id": "room", "name": "Room"}).status_code == 201
        with serving.websocket_connect("/v1/rooms/room/ws?username=Alice", headers=headers) as websocket:
            assert websocket.receive_json()["type"] == "ready"
            assert websocket.receive_json()["type"] == "history"
            for index in range(3):
                with TestClient(app(f"starting-{index}")) as joining:
                    assert joining.get("/readyz").status_code == serving.get("/readyz").status_code == 200
                    response = joining.post(
                        "/v1/rooms/room/messages", headers=headers, json={"sender": "Operator", "content": str(index)}
                    )
                    assert response.status_code == 201
                    assert websocket.receive_json()["message"] == response.json()
                    websocket.send_json({"type": "ping"})
                    assert websocket.receive_json() == {"type": "pong"}
            assert first.state.connections.active_connections == 1
        assert first.state.connections.active_connections == 0
    assert "maintenance step" not in caplog.text
    assert "DeadlockDetected" not in caplog.text
