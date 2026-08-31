# Container deployment

Version 0.12 provides a production-oriented **single-instance** Docker image and Compose profile. It is a repeatable packaging and hardening path, not a horizontal-scale claim. Run exactly one `chat` service against one SQLite volume.

## Security and process model

The image is built from the official Python 3.14.6 slim Bookworm image in two stages and runs one Uvicorn process as numeric user/group `10001:10001`. The default command uses exec form so termination signals reach the CLI and FastAPI lifespan shutdown. Compose adds a read-only root filesystem, drops Linux capabilities, prevents privilege escalation, bounds PIDs and temporary storage, uses the local rotating log driver, and mounts only `/data` as durable writable state.

The host Docker daemon remains a privileged deployment boundary; a non-root process inside the container does not make an unsafe daemon or socket safe. Terminate TLS at a trusted reverse proxy, do not mount the Docker socket, keep the host and base image patched, and retain the default loopback-only published port unless a protected proxy network requires otherwise.

Official rationale: [Docker build guidance](https://docs.docker.com/build/building/best-practices/) recommends multi-stage builds, a small trusted base, `.dockerignore`, CI builds, and a non-root `USER`; [FastAPI container guidance](https://fastapi.tiangolo.com/deployment/docker/) recommends exec-form commands for lifespan shutdown and one process per container; [Docker Compose secret guidance](https://docs.docker.com/compose/how-tos/use-secrets/) recommends mounted secret files over ordinary environment values.

## Create local secrets

Generate the two ignored files described in [`secrets/README.md`](../secrets/README.md). Each file must be UTF-8, contain exactly one non-empty line, and be no larger than 4097 bytes including one optional trailing line ending.

```text
secrets/operator-api-key.txt
secrets/token-signing-secret.txt
```

Compose mounts them read-only under `/run/secrets` and configures:

```text
SAMSARIX_CHAT_API_KEY_FILE=/run/secrets/operator_api_key
SAMSARIX_CHAT_TOKEN_SIGNING_SECRET_FILE=/run/secrets/token_signing_secret
```

The same `_FILE` convention is supported for the current and previous webhook signing secrets. Setting both a direct secret variable and its `_FILE` counterpart is a startup error; the engine never logs file contents. File paths are operator configuration, not secret values, but should still avoid user-controlled directories.

The image includes the optional asymmetric-auth and PostgreSQL dependencies. The bundled Compose profile still selects SQLite and remains strictly single-replica. PostgreSQL images are used only by the guarded [Kubernetes evaluation topology](../deploy/kubernetes/README.md) or an operator's separately reviewed deployment.

To keep signing authority outside the engine, mount a public JWKS and override the Compose environment (for example in `compose.override.yaml`):

```yaml
services:
  chat:
    environment:
      SAMSARIX_CHAT_TOKEN_SIGNING_SECRET_FILE: null
      SAMSARIX_CHAT_TOKEN_VERIFICATION_JWKS_FILE: /run/config/token-verification.jwks.json
    volumes:
      - ./config/token-verification.jwks.json:/run/config/token-verification.jwks.json:ro
```

The public-key file is validated at startup and need not be stored as a secret, but its integrity controls who can mint accepted tokens. Restrict changes to deployment administrators and follow the overlapping-key rotation sequence in [Identity and room authorization](AUTHORIZATION.md).

## Build and start

```bash
docker compose config --quiet
docker compose build --pull
docker compose up --detach
docker compose ps
```

The default port mapping is `127.0.0.1:8000:8000`. Override the host port without changing the container port:

```bash
SAMSARIX_CHAT_PORT=8080 docker compose up --detach
```

Set exact browser origins through the host environment before `up` when needed:

```bash
SAMSARIX_CHAT_ALLOWED_ORIGINS=https://chat.example.com docker compose up --detach
```

Check readiness and authenticated data access:

```bash
curl http://127.0.0.1:8000/readyz
curl -H "X-API-Key: $(cat secrets/operator-api-key.txt)" http://127.0.0.1:8000/v1/rooms
```

On PowerShell, read the key with `Get-Content -Raw secrets/operator-api-key.txt` and pass it as the header value. Avoid commands that place secrets in shared shell history on multi-user machines.

## Persistence, backup, and upgrades

The named volume `samsarix-data` owns `/data/samsarix-chat.db` and its WAL, shared-memory, and lifecycle-lock files. `docker compose restart chat` preserves it. `docker compose down` preserves it; `docker compose down --volumes` **irreversibly removes the deployment database** and is not an ordinary stop command.

Create an integrity-checked backup into the volume while the service is running:

```bash
docker compose exec chat samsarix-chat database backup /data/backups/chat.db
```

Copy backups to separately protected storage and test restore using the [operations runbook](OPERATIONS.md). The container runs as UID/GID 10001; bind-mounted host directories must grant that identity write access. Prefer the named volume unless host filesystem ownership is intentionally managed.

For an upgrade:

1. create and copy out a verified backup;
2. record the current image ID with `docker image inspect samsarix-chat-engine:0.12.0`;
3. rebuild from the reviewed revision with `docker compose build --pull`;
4. run `docker compose up --detach` and wait for healthy/readiness state;
5. exercise an authenticated room read and WebSocket reconnect.

Version 0.12 still uses SQLite schema 5, so rollback to 0.11 requires no database downgrade. Stop the 0.12 container, restore the prior authentication configuration and reviewed 0.11 image, then start it against the preserved volume. A deployment using asymmetric JWKS mode must return to HS256 or another v0.11-supported credential before rollback. Restore the pre-upgrade backup for older incompatible schemas.

## Operational limits

- Run one replica and one process with this SQLite Compose profile. Do not use `docker compose up --scale chat=...`, Uvicorn workers, Kubernetes replicas, or multiple containers against the volume. The separate PostgreSQL preview uses no SQLite volume and has its own identity and acceptance contract.
- Readiness proves the process can query SQLite; it does not test a reverse proxy, webhook receiver, backup freshness, disk capacity, or client path.
- The health check is intentionally unauthenticated and returns no chat data.
- Container logs are operational metadata but may include request paths and search query strings from access logging. Govern them as potentially sensitive data.
- Compose does not configure TLS, a firewall, backups, monitoring, host patching, or a support SLA.

Multi-instance support requires a shared authoritative database plus fan-out, presence, rate-limit, migration, restore, and leader-election decisions. Redis Pub/Sub alone is at-most-once and does not solve those storage/lifecycle concerns. The accepted [v0.13 architecture](MULTI_INSTANCE_ARCHITECTURE.md) uses PostgreSQL plus a transactional event log and keeps this container single-replica until every cross-process failure gate passes.
