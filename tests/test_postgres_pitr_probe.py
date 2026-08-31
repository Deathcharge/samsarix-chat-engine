# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Safety checks for the disposable PostgreSQL physical-PITR probe."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from scripts.postgres_pitr_probe import (
    CONFIRMATION,
    _expect,
    reject_libpq_routing_environment,
    validated_rehearsal_url,
)


@pytest.mark.parametrize(
    "mode,port",
    [
        ("seed", 5432),
        ("target", 5432),
        ("after-target", 5432),
        ("verify", 55432),
    ],
)
def test_pitr_url_accepts_only_the_mode_specific_loopback_port(mode: str, port: int) -> None:
    value = f"postgresql://pitr:secret@127.0.0.1:{port}/samsarix_pitr_source"
    assert validated_rehearsal_url(value, mode, CONFIRMATION) == value


@pytest.mark.parametrize(
    "value,mode,confirmation,error",
    [
        (None, "seed", CONFIRMATION, "is required"),
        (
            "postgresql://pitr@127.0.0.1:5432/samsarix_pitr_source",
            "seed",
            None,
            "explicitly confirm",
        ),
        ("https://127.0.0.1:5432/samsarix_pitr_source", "seed", CONFIRMATION, "PostgreSQL URL"),
        (
            "postgresql://pitr@database.example:5432/samsarix_pitr_source",
            "seed",
            CONFIRMATION,
            "non-loopback",
        ),
        ("postgresql://pitr@127.0.0.1:5432/production", "seed", CONFIRMATION, "requires"),
        ("postgresql://pitr@127.0.0.1:55432/samsarix_pitr_source", "seed", CONFIRMATION, "port 5432"),
        ("postgresql://pitr@127.0.0.1:5432/samsarix_pitr_source", "verify", CONFIRMATION, "port 55432"),
        (
            "postgresql://pitr@127.0.0.1:5432/samsarix_pitr_source?hostaddr=203.0.113.1",
            "seed",
            CONFIRMATION,
            "requires",
        ),
        (
            "postgresql://pitr@127.0.0.1:invalid/samsarix_pitr_source",
            "seed",
            CONFIRMATION,
            "exact disposable port",
        ),
    ],
)
def test_pitr_url_rejects_unsafe_or_ambiguous_targets(
    value: str | None,
    mode: str,
    confirmation: str | None,
    error: str,
) -> None:
    with pytest.raises(ValueError, match=error):
        validated_rehearsal_url(value, mode, confirmation)


def test_pitr_probe_rejects_libpq_routing_without_echoing_values() -> None:
    environment = {
        "PGHOSTADDR": "private-host-value",
        "PGSERVICEFILE": "private-service-value",
        "UNRELATED": "allowed",
    }
    with pytest.raises(ValueError) as caught:
        reject_libpq_routing_environment(environment)
    assert "PGHOSTADDR, PGSERVICEFILE" in str(caught.value)
    assert "private-host-value" not in str(caught.value)
    assert "private-service-value" not in str(caught.value)


def test_pitr_request_failure_never_echoes_response_content() -> None:
    secret = "private recovered message and credential"
    response = SimpleNamespace(status_code=500, json=lambda: {"private": secret})
    with pytest.raises(RuntimeError) as caught:
        _expect("PITR read", response, 200)
    assert "HTTP 500" in str(caught.value)
    assert secret not in str(caught.value)
