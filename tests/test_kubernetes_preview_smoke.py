# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Unit checks for the live Kubernetes preview smoke boundary."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from scripts.smoke_kubernetes_preview import parse_endpoint

_ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize(
    ("raw", "http_origin", "websocket_origin"),
    [
        ("http://127.0.0.1:18000", "http://127.0.0.1:18000", "ws://127.0.0.1:18000"),
        ("http://localhost:18001/", "http://localhost:18001", "ws://localhost:18001"),
        ("http://[::1]:18002", "http://[::1]:18002", "ws://[::1]:18002"),
    ],
)
def test_parse_endpoint_accepts_explicit_loopback_origins(raw: str, http_origin: str, websocket_origin: str) -> None:
    endpoint = parse_endpoint(raw)
    assert endpoint.http_origin == http_origin
    assert endpoint.websocket_origin == websocket_origin


@pytest.mark.parametrize(
    "raw",
    [
        "https://127.0.0.1:18000",
        "http://10.0.0.2:18000",
        "http://example.com:18000",
        "http://127.0.0.1",
        "http://user:secret@127.0.0.1:18000",
        "http://127.0.0.1:18000/path",
        "http://127.0.0.1:18000?token=secret",
        "http://127.0.0.1:18000#fragment",
    ],
)
def test_parse_endpoint_rejects_nonlocal_or_ambiguous_targets(raw: str) -> None:
    with pytest.raises(ValueError, match="loopback"):
        parse_endpoint(raw)


def test_live_acceptance_assets_remain_pinned_and_ci_required() -> None:
    workflow = (_ROOT / ".github" / "workflows" / "kubernetes-preview.yml").read_text(encoding="utf-8")
    ci = (_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    assert "KIND_VERSION: v0.32.0" in workflow
    assert "KUBECTL_VERSION: v1.36.1" in workflow
    assert "kindest/node:v1.36.1@sha256:" in workflow
    assert "uses: ./.github/workflows/kubernetes-preview.yml" in ci

    documents = list(
        yaml.safe_load_all(
            (_ROOT / "deploy" / "kubernetes" / "acceptance" / "postgres.yaml").read_text(encoding="utf-8")
        )
    )
    deployment = next(document for document in documents if document["kind"] == "Deployment")
    container = deployment["spec"]["template"]["spec"]["containers"][0]
    assert container["image"].startswith("postgres:18.6-bookworm@sha256:")
    assert "ssl=on" in container["command"][-1]
