# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Safety checks for the disposable PostgreSQL restore-rehearsal probe."""

from __future__ import annotations

import pytest
from httpx import Response

from scripts.postgres_restore_probe import (
    CONFIRMATION,
    _expect,
    reject_libpq_routing_environment,
    validated_rehearsal_url,
)


@pytest.mark.parametrize(
    "mode,database",
    [
        ("seed", "samsarix_backup_source"),
        ("verify", "samsarix_backup_restore"),
    ],
)
def test_rehearsal_url_accepts_only_the_mode_specific_disposable_database(mode: str, database: str) -> None:
    value = f"postgresql://rehearsal:secret@127.0.0.1:5432/{database}"
    assert validated_rehearsal_url(value, mode, CONFIRMATION) == value


@pytest.mark.parametrize(
    "value,mode,confirmation,error",
    [
        (None, "seed", CONFIRMATION, "is required"),
        ("postgresql://user@127.0.0.1/samsarix_backup_source", "seed", None, "explicitly confirm"),
        ("https://127.0.0.1/samsarix_backup_source", "seed", CONFIRMATION, "PostgreSQL URL"),
        (
            "postgresql://user@database.example/samsarix_backup_source?sslmode=verify-full",
            "seed",
            CONFIRMATION,
            "non-loopback",
        ),
        (
            "postgresql://user@127.0.0.1/production",
            "seed",
            CONFIRMATION,
            "exact disposable database",
        ),
        (
            "postgresql://user@127.0.0.1/samsarix_backup_restore",
            "seed",
            CONFIRMATION,
            "samsarix_backup_source",
        ),
        (
            "postgresql://user@127.0.0.1/samsarix_backup_source#fragment",
            "seed",
            CONFIRMATION,
            "exact disposable database",
        ),
        (
            "postgresql://user@127.0.0.1/samsarix_backup_source?hostaddr=203.0.113.1",
            "seed",
            CONFIRMATION,
            "exact disposable database",
        ),
    ],
)
def test_rehearsal_url_rejects_unsafe_or_ambiguous_targets(
    value: str | None,
    mode: str,
    confirmation: str | None,
    error: str,
) -> None:
    with pytest.raises(ValueError, match=error):
        validated_rehearsal_url(value, mode, confirmation)


def test_rehearsal_rejects_libpq_environment_routing_without_echoing_values() -> None:
    environment = {
        "PGHOSTADDR": "private-host-value",
        "PGOPTIONS": "private-options-value",
        "UNRELATED": "allowed",
    }
    with pytest.raises(ValueError) as caught:
        reject_libpq_routing_environment(environment)
    assert "PGHOSTADDR, PGOPTIONS" in str(caught.value)
    assert "private-host-value" not in str(caught.value)
    assert "private-options-value" not in str(caught.value)


def test_request_failure_never_echoes_response_content() -> None:
    secret = "private message and credential"
    response = Response(500, text=secret)
    with pytest.raises(RuntimeError) as caught:
        _expect("restore read", response, 200)
    assert "HTTP 500" in str(caught.value)
    assert secret not in str(caught.value)
