# Kubernetes PostgreSQL preview

This manifest is a guarded evaluation topology for the unreleased PostgreSQL backend. It runs two application Pods against an operator-provided PostgreSQL service; it does not install or operate the database, an ingress controller, certificates, backups, or monitoring.

The application uses a `StatefulSet` because every Pod needs a stable, unique identity. Kubernetes assigns names such as `samsarix-chat-0` and `samsarix-chat-1`; the Downward API passes that Pod name to `SAMSARIX_CHAT_POSTGRES_INSTANCE_ID`. The database lease rejects a second live process that presents the same identity. Do not replace the StatefulSet with a Deployment, hard-code one identity, or override the environment variable.

The `OnDelete` update strategy is deliberate. It prevents Kubernetes from automatically mixing application versions against one PostgreSQL schema, but it does not make arbitrary one-by-one replacement safe. For a version upgrade, follow the stop-all-old procedure in the PostgreSQL preview guide; use manual Pod replacement only for same-version recovery after confirming the old process is gone or fenced.

Kubernetes documents the StatefulSet ordinal, stable network identity, and one-Pod-per-identity contract in [StatefulSets](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/). Forced deletion can violate the assumption that the old process is gone, so follow the [force-deletion safety guidance](https://kubernetes.io/docs/tasks/run-application/force-delete-stateful-set-pod/) and externally fence an uncertain old Pod or node before replacement.

## Prerequisites

- a reviewed Samsarix container image that includes the PostgreSQL extra; the repository Dockerfile now does;
- a PostgreSQL 18 service reachable with TLS hostname verification;
- a dedicated database/application role and a complete backup/restore plan;
- `kubectl` access to an evaluation namespace.

The checked-in image tag is a local placeholder because the project does not publish a registry image yet. Import the reviewed image into the cluster or replace the image with an immutable digest before applying the manifest.

## Runtime secret

Create four protected files outside the repository:

- `postgres-url`: one PostgreSQL URL using `sslmode=verify-full` and, for a private CA, `sslrootcert=/run/secrets/postgres-ca.pem`;
- `postgres-ca.pem`: the trusted database CA certificate chain;
- `operator-api-key`: at least 16 random characters;
- `token-signing-secret`: at least 32 random bytes/characters.

Create the Secret without placing values directly in the manifest:

```bash
kubectl create secret generic samsarix-chat-runtime \
  --from-file=postgres-url=/protected/postgres-url \
  --from-file=postgres-ca.pem=/protected/postgres-ca.pem \
  --from-file=operator-api-key=/protected/operator-api-key \
  --from-file=token-signing-secret=/protected/token-signing-secret
```

For production-oriented identity separation, replace the token-signing secret with a mounted public JWKS and `SAMSARIX_CHAT_TOKEN_VERIFICATION_JWKS_FILE`, leaving signing authority in the host application.

## Validate and apply

Install the development dependencies and run the repository-specific structural check:

```bash
python -m pip install -e ".[dev]"
python scripts/verify_kubernetes_preview.py
kubectl apply --dry-run=client -f deploy/kubernetes/postgres-preview.yaml
kubectl apply -f deploy/kubernetes/postgres-preview.yaml
```

The verifier requires the StatefulSet identity to come from `metadata.name`, requires the manual `OnDelete` update gate, checks the selectors and headless-service relationship, forbids inline credentials, validates secret-file mounts, and checks the non-root/read-only/probe/resource baseline. A successful structural check does not prove that an external database, network policy, ingress, or certificate is correct.

Wait for both Pods and exercise the service from inside the cluster:

```bash
kubectl wait --for=condition=Ready \
  --selector=app.kubernetes.io/name=samsarix-chat,app.kubernetes.io/component=engine \
  pod \
  --timeout=180s
kubectl get pods -l app.kubernetes.io/name=samsarix-chat
kubectl port-forward service/samsarix-chat 8000:80
curl http://127.0.0.1:8000/readyz
```

The ClusterIP Service sends new connections only to ready Pods. WebSocket clients must reconnect and reload authoritative history after any disconnect; individual socket delivery remains best effort.

## Live repository acceptance

The reusable `.github/workflows/kubernetes-preview.yml` workflow executes this manifest in a disposable pinned kind cluster on every pull request and main update. It builds and imports the current repository image, creates a short-lived CA and hostname certificate, provisions the CI-only digest-pinned PostgreSQL manifest in `acceptance/postgres.yaml`, and creates all credentials as files before constructing Kubernetes Secrets.

Acceptance requires both StatefulSet Pods to become ready, PostgreSQL to report the exact live identities `samsarix-chat-0` and `samsarix-chat-1`, an HTTP room created through one Pod to be visible through the other, and one signed member's WebSocket message to reach a signed member on the other Pod with the same durable ID. The workflow then deletes ordinal zero normally, waits for its same-version replacement, and proves the stable identity and room survived. `scripts/smoke_kubernetes_preview.py` permits only explicit loopback origins so the checkout-only probe cannot be redirected to a remote service.

This closes the repository's live manifest-execution gate. The acceptance database is ephemeral and single-node; it is not an example for operating PostgreSQL and does not exercise an ingress, NetworkPolicy, certificate rotation, node loss, persistent volumes, external fencing, database failover/failback, registry publication, capacity, or an owner's environment.

## Boundaries

The manifest deliberately omits public ingress and database provisioning. Operators still own TLS termination, NetworkPolicy, external PostgreSQL failover/fencing, availability zones, immutable image provenance, secret rotation, migrations, backup retention, capacity, alerting, and measured RPO/RTO. The PodDisruptionBudget only influences voluntary Kubernetes disruptions and is not a database or node-fencing mechanism.
