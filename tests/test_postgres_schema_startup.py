# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Schema startup decision tests without a PostgreSQL server."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

psycopg = pytest.importorskip("psycopg")

from samsarix_chat_engine.postgres import (  # noqa: E402
    POSTGRES_SCHEMA_VERSION,
    PostgresFoundation,
    PostgresUnavailableError,
    UnsupportedPostgresSchemaError,
)
from samsarix_chat_engine.postgres_store import PostgresChatStore  # noqa: E402


def _service(version, *, commit_error=False):
    service = PostgresFoundation("postgresql://localhost/samsarix_test")
    statements = []

    async def execute(statement, parameters=None):
        sql = " ".join(statement.split())
        statements.append((sql, parameters))
        row = None
        if "to_regclass" in sql:
            row = ("samsarix_schema_metadata" if version is not None else None,)
        elif sql.startswith("SELECT version FROM public.samsarix_schema_metadata"):
            row = (version,) if version else None
        return SimpleNamespace(fetchone=AsyncMock(return_value=row))

    @asynccontextmanager
    async def transaction():
        yield
        if commit_error:
            raise psycopg.OperationalError("commit failed")

    connection = SimpleNamespace(execute=execute, transaction=transaction, pgconn=SimpleNamespace(finish=Mock()))

    @asynccontextmanager
    async def pool_connection():
        yield connection

    service._pool = SimpleNamespace(open=AsyncMock(), close=AsyncMock(), connection=pool_connection)
    return service, statements


@pytest.mark.asyncio
async def test_current_schema_startup_only_inspects_without_ddl_or_row_mutation():
    service, statements = _service(POSTGRES_SCHEMA_VERSION)
    await service.open()
    assert service._opened
    assert statements
    assert all(sql.startswith(("SELECT ", "SET LOCAL ")) for sql, _parameters in statements)
    assert not any("FOR UPDATE" in sql for sql, _parameters in statements)
    service._pool.close.assert_not_awaited()


@pytest.mark.asyncio
async def test_future_schema_rejection_does_not_attempt_ddl():
    service, statements = _service(POSTGRES_SCHEMA_VERSION + 1)
    with pytest.raises(UnsupportedPostgresSchemaError, match="newer"):
        await service.open()
    assert not service._opened
    assert all(sql.startswith(("SELECT ", "SET LOCAL ")) for sql, _parameters in statements)
    service._pool.close.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("version", [None, 0, 2, 6, 7])
async def test_missing_or_older_schema_still_runs_migration_before_recording_version(version):
    service, statements = _service(version)
    await service.open()
    assert service._opened
    assert any(sql.startswith("ALTER TABLE ") for sql, _parameters in statements)
    sql, parameters = statements[-1]
    assert sql.startswith("INSERT INTO public.samsarix_schema_metadata ")
    assert parameters == (POSTGRES_SCHEMA_VERSION,)


@pytest.mark.asyncio
@pytest.mark.parametrize("version", [None, POSTGRES_SCHEMA_VERSION])
async def test_startup_is_not_published_until_transaction_exit_succeeds(version):
    service, _statements = _service(version, commit_error=True)
    with pytest.raises(PostgresUnavailableError):
        await service.open()
    assert not service._opened
    service._pool.close.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("version", [POSTGRES_SCHEMA_VERSION - 1, POSTGRES_SCHEMA_VERSION, POSTGRES_SCHEMA_VERSION + 1])
async def test_readiness_requires_the_exact_supported_schema(version):
    store = object.__new__(PostgresChatStore)
    store.foundation = SimpleNamespace(schema_version=AsyncMock(return_value=version))
    assert await store.check_ready() is (version == POSTGRES_SCHEMA_VERSION)


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["pool", "schema"])
async def test_cancelled_startup_closes_pool_without_publishing_open_state(phase):
    service, _statements = _service(POSTGRES_SCHEMA_VERSION)
    if phase == "pool":
        service._pool.open.side_effect = asyncio.CancelledError
    else:
        service._initialize_schema = AsyncMock(side_effect=asyncio.CancelledError)
    with pytest.raises(asyncio.CancelledError):
        await service.open()
    assert not service._opened
    service._pool.close.assert_awaited_once()
