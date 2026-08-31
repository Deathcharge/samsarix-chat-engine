# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Structural safety tests for the guarded Kubernetes preview."""

from __future__ import annotations

from copy import deepcopy

import pytest

from scripts.verify_kubernetes_preview import (
    ManifestValidationError,
    load_documents,
    validate_documents,
)


def _resource(documents: list[dict], kind: str, name: str) -> dict:
    return next(document for document in documents if document["kind"] == kind and document["metadata"]["name"] == name)


def _chat_container(documents: list[dict]) -> dict:
    stateful_set = _resource(documents, "StatefulSet", "samsarix-chat")
    return next(
        container for container in stateful_set["spec"]["template"]["spec"]["containers"] if container["name"] == "chat"
    )


def test_checked_manifest_has_two_stable_hardened_replicas() -> None:
    assert validate_documents(load_documents()) == 2


def test_manifest_rejects_a_shared_literal_instance_identity() -> None:
    documents = deepcopy(load_documents())
    environment = _chat_container(documents)["env"]
    identity = next(item for item in environment if item["name"] == "SAMSARIX_CHAT_POSTGRES_INSTANCE_ID")
    identity.pop("valueFrom")
    identity["value"] = "shared-replica"

    with pytest.raises(ManifestValidationError, match="shared literal"):
        validate_documents(documents)


def test_manifest_rejects_inline_database_credentials() -> None:
    documents = deepcopy(load_documents())
    _chat_container(documents)["env"].append(
        {
            "name": "SAMSARIX_CHAT_POSTGRES_URL",
            "value": "postgresql://user:secret@database.example/samsarix?sslmode=verify-full",
        }
    )

    with pytest.raises(ManifestValidationError, match="never be embedded"):
        validate_documents(documents)


def test_manifest_rejects_inline_application_secrets() -> None:
    documents = deepcopy(load_documents())
    _chat_container(documents)["env"].append(
        {
            "name": "SAMSARIX_CHAT_API_KEY",
            "value": "inline-operator-key-must-not-be-accepted",
        }
    )

    with pytest.raises(ManifestValidationError, match="never be embedded"):
        validate_documents(documents)


def test_manifest_rejects_selector_drift() -> None:
    documents = deepcopy(load_documents())
    stateful_set = _resource(documents, "StatefulSet", "samsarix-chat")
    stateful_set["spec"]["template"]["metadata"]["labels"]["app.kubernetes.io/component"] = "other"

    with pytest.raises(ManifestValidationError, match="pod labels"):
        validate_documents(documents)


def test_manifest_rejects_automatic_mixed_version_rollouts() -> None:
    documents = deepcopy(load_documents())
    stateful_set = _resource(documents, "StatefulSet", "samsarix-chat")
    stateful_set["spec"]["updateStrategy"]["type"] = "RollingUpdate"

    with pytest.raises(ManifestValidationError, match="manual Pod replacement"):
        validate_documents(documents)


def test_manifest_rejects_weakened_container_security() -> None:
    documents = deepcopy(load_documents())
    _chat_container(documents)["securityContext"]["readOnlyRootFilesystem"] = False

    with pytest.raises(ManifestValidationError, match="read-only"):
        validate_documents(documents)
