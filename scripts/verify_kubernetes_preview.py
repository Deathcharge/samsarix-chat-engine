# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Fail-closed structural verification for the Kubernetes PostgreSQL preview."""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import yaml

DEFAULT_MANIFEST = Path("deploy/kubernetes/postgres-preview.yaml")
_LABELS = {
    "app.kubernetes.io/name": "samsarix-chat",
    "app.kubernetes.io/component": "engine",
}
_SECRET_FILES = {
    "SAMSARIX_CHAT_POSTGRES_URL_FILE": "/run/secrets/postgres-url",
    "SAMSARIX_CHAT_API_KEY_FILE": "/run/secrets/operator-api-key",
    "SAMSARIX_CHAT_TOKEN_SIGNING_SECRET_FILE": "/run/secrets/token-signing-secret",
}
_DIRECT_SECRET_ENV = {
    "SAMSARIX_CHAT_POSTGRES_URL",
    "SAMSARIX_CHAT_API_KEY",
    "SAMSARIX_CHAT_TOKEN_SIGNING_SECRET",
    "SAMSARIX_CHAT_WEBHOOK_SIGNING_SECRET",
    "SAMSARIX_CHAT_WEBHOOK_PREVIOUS_SIGNING_SECRET",
}


class ManifestValidationError(ValueError):
    """Raised when the preview manifest weakens a required deployment invariant."""


def _mapping(value: Any, description: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ManifestValidationError(f"{description} must be a mapping")
    return value


def _sequence(value: Any, description: str) -> Sequence[Any]:
    if not isinstance(value, list):
        raise ManifestValidationError(f"{description} must be a list")
    return value


def load_documents(path: Path = DEFAULT_MANIFEST) -> list[dict[str, Any]]:
    """Load non-empty YAML resources without constructing arbitrary Python objects."""

    try:
        with path.open(encoding="utf-8") as handle:
            loaded = list(yaml.safe_load_all(handle))
    except (OSError, yaml.YAMLError) as exc:
        raise ManifestValidationError("preview manifest must be readable valid YAML") from exc
    documents: list[dict[str, Any]] = []
    for index, document in enumerate(loaded, start=1):
        if document is None:
            continue
        if not isinstance(document, dict):
            raise ManifestValidationError(f"manifest document {index} must be a mapping")
        documents.append(document)
    if not documents:
        raise ManifestValidationError("preview manifest must contain resources")
    return documents


def _resource(documents: Sequence[Mapping[str, Any]], kind: str, name: str) -> Mapping[str, Any]:
    matches = []
    for document in documents:
        metadata = document.get("metadata")
        if document.get("kind") == kind and isinstance(metadata, Mapping) and metadata.get("name") == name:
            matches.append(document)
    if len(matches) != 1:
        raise ManifestValidationError(f"manifest must contain exactly one {kind}/{name}")
    return matches[0]


def _env_by_name(container: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    variables: dict[str, Mapping[str, Any]] = {}
    for raw in _sequence(container.get("env"), "chat environment"):
        variable = _mapping(raw, "chat environment entry")
        name = variable.get("name")
        if not isinstance(name, str) or not name:
            raise ManifestValidationError("every chat environment entry must have a name")
        if name in variables:
            raise ManifestValidationError(f"chat environment contains duplicate {name}")
        variables[name] = variable
    return variables


def _require_probe(container: Mapping[str, Any], probe_name: str, path: str) -> None:
    probe = _mapping(container.get(probe_name), f"chat {probe_name}")
    http_get = _mapping(probe.get("httpGet"), f"chat {probe_name}.httpGet")
    if http_get.get("path") != path or http_get.get("port") != "http":
        raise ManifestValidationError(f"chat {probe_name} must check {path} on the named http port")


def _require_service_port(service_spec: Mapping[str, Any], port: int) -> None:
    ports = _sequence(service_spec.get("ports"), "Service ports")
    expected = [
        item
        for item in ports
        if isinstance(item, Mapping)
        and item.get("name") == "http"
        and item.get("port") == port
        and item.get("targetPort") == "http"
    ]
    if len(expected) != 1:
        raise ManifestValidationError(f"Service must route port {port} to the named http container port")


def validate_documents(documents: Sequence[Mapping[str, Any]]) -> int:
    """Validate stable identity, secret isolation, routing, and pod hardening."""

    stateful_set = _resource(documents, "StatefulSet", "samsarix-chat")
    headless = _resource(documents, "Service", "samsarix-chat-headless")
    service = _resource(documents, "Service", "samsarix-chat")
    disruption_budget = _resource(documents, "PodDisruptionBudget", "samsarix-chat")

    spec = _mapping(stateful_set.get("spec"), "StatefulSet spec")
    replicas = spec.get("replicas")
    if not isinstance(replicas, int) or isinstance(replicas, bool) or replicas < 2:
        raise ManifestValidationError("StatefulSet must exercise at least two replicas")
    if spec.get("serviceName") != "samsarix-chat-headless":
        raise ManifestValidationError("StatefulSet must use the checked headless service")
    if _mapping(spec.get("updateStrategy"), "StatefulSet update strategy").get("type") != "OnDelete":
        raise ManifestValidationError("StatefulSet must require manual Pod replacement for version changes")
    selector = _mapping(spec.get("selector"), "StatefulSet selector")
    if selector.get("matchLabels") != _LABELS:
        raise ManifestValidationError("StatefulSet selector must exactly match the Samsarix engine labels")

    template = _mapping(spec.get("template"), "StatefulSet pod template")
    template_metadata = _mapping(template.get("metadata"), "StatefulSet pod metadata")
    if template_metadata.get("labels") != _LABELS:
        raise ManifestValidationError("pod labels must exactly match the StatefulSet selector")
    pod = _mapping(template.get("spec"), "StatefulSet pod spec")
    if pod.get("automountServiceAccountToken") is not False:
        raise ManifestValidationError("pod must disable service-account token automount")
    if pod.get("enableServiceLinks") is not False:
        raise ManifestValidationError("pod must disable ambient service-link environment variables")
    if pod.get("terminationGracePeriodSeconds") != 30:
        raise ManifestValidationError("pod must retain the 30-second graceful termination window")
    pod_security = _mapping(pod.get("securityContext"), "pod security context")
    if (
        pod_security.get("runAsNonRoot") is not True
        or pod_security.get("runAsUser") != 10001
        or pod_security.get("runAsGroup") != 10001
        or pod_security.get("fsGroup") != 10001
        or _mapping(pod_security.get("seccompProfile"), "pod seccomp profile").get("type") != "RuntimeDefault"
    ):
        raise ManifestValidationError("pod must use the non-root 10001 identity and RuntimeDefault seccomp")

    containers = _sequence(pod.get("containers"), "pod containers")
    chat_containers = [item for item in containers if isinstance(item, Mapping) and item.get("name") == "chat"]
    if len(chat_containers) != 1:
        raise ManifestValidationError("pod must contain exactly one chat container")
    chat = chat_containers[0]
    image = chat.get("image")
    if not isinstance(image, str) or not image or image.endswith(":latest"):
        raise ManifestValidationError("chat image must use an explicit non-latest reference")
    args = _sequence(chat.get("args"), "chat args")
    if "serve" not in args or "--database" in args:
        raise ManifestValidationError("chat args must start the service without a SQLite database override")

    environment = _env_by_name(chat)
    if environment.get("SAMSARIX_CHAT_STORAGE", {}).get("value") != "postgres":
        raise ManifestValidationError("chat storage must be explicitly set to postgres")
    embedded_secrets = sorted(_DIRECT_SECRET_ENV.intersection(environment))
    if embedded_secrets:
        raise ManifestValidationError("credentials must never be embedded directly in the manifest")
    identity = environment.get("SAMSARIX_CHAT_POSTGRES_INSTANCE_ID")
    if identity is None or "value" in identity:
        raise ManifestValidationError("PostgreSQL instance identity must not be a shared literal")
    value_from = _mapping(identity.get("valueFrom"), "PostgreSQL instance identity valueFrom")
    field_ref = _mapping(value_from.get("fieldRef"), "PostgreSQL instance identity fieldRef")
    if field_ref.get("fieldPath") != "metadata.name":
        raise ManifestValidationError("PostgreSQL instance identity must come from the stable Pod name")
    for name, expected_path in _SECRET_FILES.items():
        if environment.get(name, {}).get("value") != expected_path:
            raise ManifestValidationError(f"{name} must use the checked mounted-secret path")

    mounts = _sequence(chat.get("volumeMounts"), "chat volume mounts")
    secret_mounts = [
        item
        for item in mounts
        if isinstance(item, Mapping)
        and item.get("name") == "runtime-secrets"
        and item.get("mountPath") == "/run/secrets"
        and item.get("readOnly") is True
    ]
    if len(secret_mounts) != 1:
        raise ManifestValidationError("runtime secrets must have one read-only /run/secrets mount")
    volumes = _sequence(pod.get("volumes"), "pod volumes")
    secret_volumes = [item for item in volumes if isinstance(item, Mapping) and item.get("name") == "runtime-secrets"]
    if len(secret_volumes) != 1:
        raise ManifestValidationError("pod must contain exactly one runtime-secrets volume")
    secret = _mapping(secret_volumes[0].get("secret"), "runtime-secrets source")
    if secret.get("secretName") != "samsarix-chat-runtime":
        raise ManifestValidationError("runtime-secrets must reference samsarix-chat-runtime")
    secret_items = _sequence(secret.get("items"), "runtime secret items")
    projected = {(item.get("key"), item.get("path")) for item in secret_items if isinstance(item, Mapping)}
    required_items = {
        ("postgres-url", "postgres-url"),
        ("postgres-ca.pem", "postgres-ca.pem"),
        ("operator-api-key", "operator-api-key"),
        ("token-signing-secret", "token-signing-secret"),
    }
    if projected != required_items:
        raise ManifestValidationError("runtime secret must project exactly the four documented keys")

    container_security = _mapping(chat.get("securityContext"), "chat security context")
    capabilities = _mapping(container_security.get("capabilities"), "chat capabilities")
    if (
        container_security.get("allowPrivilegeEscalation") is not False
        or container_security.get("readOnlyRootFilesystem") is not True
        or capabilities.get("drop") != ["ALL"]
    ):
        raise ManifestValidationError("chat container must be read-only, non-escalating, and capability-free")
    resources = _mapping(chat.get("resources"), "chat resources")
    if not isinstance(resources.get("requests"), Mapping) or not isinstance(resources.get("limits"), Mapping):
        raise ManifestValidationError("chat container must declare resource requests and limits")
    _require_probe(chat, "startupProbe", "/readyz")
    _require_probe(chat, "readinessProbe", "/readyz")
    _require_probe(chat, "livenessProbe", "/healthz")
    ports = _sequence(chat.get("ports"), "chat ports")
    http_ports = [
        item
        for item in ports
        if isinstance(item, Mapping)
        and item.get("name") == "http"
        and item.get("containerPort") == 8000
        and item.get("protocol") == "TCP"
    ]
    if len(http_ports) != 1:
        raise ManifestValidationError("chat container must expose one named TCP http port at 8000")

    headless_spec = _mapping(headless.get("spec"), "headless Service spec")
    if headless_spec.get("clusterIP") != "None" or headless_spec.get("selector") != _LABELS:
        raise ManifestValidationError("headless Service must select only the stable Samsarix Pods")
    _require_service_port(headless_spec, 8000)
    service_spec = _mapping(service.get("spec"), "client Service spec")
    if service_spec.get("type") != "ClusterIP" or service_spec.get("selector") != _LABELS:
        raise ManifestValidationError("client Service must be an internal ClusterIP for ready Samsarix Pods")
    _require_service_port(service_spec, 80)
    budget_spec = _mapping(disruption_budget.get("spec"), "PodDisruptionBudget spec")
    if (
        budget_spec.get("minAvailable") != 1
        or _mapping(budget_spec.get("selector"), "PodDisruptionBudget selector").get("matchLabels") != _LABELS
    ):
        raise ManifestValidationError("PodDisruptionBudget must retain at least one selected replica")
    return replicas


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", nargs="?", type=Path, default=DEFAULT_MANIFEST)
    arguments = parser.parse_args(argv)
    try:
        replicas = validate_documents(load_documents(arguments.manifest))
    except ManifestValidationError as exc:
        parser.error(str(exc))
    print(f"validated {arguments.manifest}: {replicas} stable StatefulSet replicas")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
