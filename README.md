# Samsarix Chat Engine

Samsarix Chat Engine is a small, local-first room chat service from Samsarix LLC for developers who need persisted messages and live WebSocket delivery without adopting a full collaboration platform. It runs as a standalone FastAPI service or as an embeddable ASGI application, stores data in SQLite, and has no dependency on Redis, an LLM provider, or any private package.

Version 0.12.0 is an alpha release candidate. Its core single-instance journey, tenant-safe access boundary, verification-only asymmetric authentication, accountable data lifecycle, practical conversation controls, typed TypeScript client, support workflow and retrieval, durable application webhooks, and hardened container deployment are implemented and tested. The development branch additionally includes unreleased one-depth threaded replies and a guarded PostgreSQL multi-instance preview. The project is licensed under the standard Mozilla Public License 2.0.

## What works

- Create and inspect rooms over HTTP.
- Post validated messages over HTTP or WebSocket.
- Persist room history in SQLite and recover it after reconnect or restart.
- Search the current retained content of one authorized room with Unicode-aware cursor pagination.
- Keep contextual follow-ups in one-depth threads with authorized reply pagination.
- Broadcast messages and lightweight join/leave presence within one process.
- Retry message submission safely with `Idempotency-Key` or `client_message_id`.
- Protect operator actions with an optional shared API key.
- Give application users signed, expiring, per-room read/write access tokens using HS256 or a static public Ed25519/RSA JWKS.
- Track signed users' monotonic room read cursors and current unread counts without counting their own messages.
- Exchange separately rate-limited, auto-expiring typing signals without persisting activity history.
- Let authors edit or delete their own messages while administrators can moderate any message.
- Freeze rooms for administrator-only announcements, mute disruptive members, and ban room access by token subject.
- Integrate from browser or Node applications with a typed, zero-runtime-dependency TypeScript client.
- Stream versioned room exports, archive/reopen rooms, and require two-step confirmed deletion.
- Apply optional age-based retention and inspect a bounded metadata-only administrative audit trail.
- Create integrity-checked SQLite backups and restore them through the CLI.
- Deliver selected committed message/moderation events through a signed, durable, retrying webhook outbox.
- Deploy one non-root process with a hardened Compose profile, mounted secret files, persistent SQLite volume, and readiness health check.
- Bound message size, send rate, connections, room count, and retained history.
- Check liveness at `/healthz`, storage readiness at `/readyz`, and OpenAPI docs at `/docs`.

It deliberately does not provide user registration, password storage, attachments, end-to-end encryption, federation, multi-instance fan-out, or AI agents.

## Quick start

Prerequisites: Python 3.10 or newer and Git.

```bash
git clone https://github.com/Deathcharge/samsarix-chat-engine.git
cd samsarix-chat-engine
python -m venv .venv
```

Activate the environment with `.venv\Scripts\Activate.ps1` on PowerShell or `source .venv/bin/activate` on POSIX, then:

```bash
python -m pip install --upgrade pip setuptools wheel
python -m pip install .
samsarix-chat serve
```

The service binds to `127.0.0.1:8000` and creates `data/samsarix-chat.db`. In another terminal, create a room and send a message:

```bash
curl -X POST http://127.0.0.1:8000/v1/rooms \
  -H "Content-Type: application/json" \
  -d '{"id":"general","name":"General"}'

curl -X POST http://127.0.0.1:8000/v1/rooms/general/messages \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: getting-started-1" \
  -d '{"sender":"Andrew","content":"Hello, room"}'

curl http://127.0.0.1:8000/v1/rooms/general/messages

curl --get http://127.0.0.1:8000/v1/rooms/general/messages/search \
  --data-urlencode "q=hello"
```

For a complete WebSocket client, install the test extra and run the examples after creating `general`:

```bash
python -m pip install ".[test]"
python examples/01_rest_chat.py
python examples/02_websocket_chat.py
```

See [Getting started](docs/GETTING_STARTED.md) for authentication and browser examples, [Conversation controls](docs/CONVERSATION_CONTROLS.md) for moderation workflows, and [Data lifecycle operations](docs/OPERATIONS.md) for export, deletion, retention, backup, and restore.

The development branch also contains a guarded, unreleased PostgreSQL multi-instance mode. It is fully wired through the application, and CI rehearses both a native logical dump into a fresh database and physical base-backup/WAL recovery to a named point with application-level verification. A pinned disposable kind gate executes the structurally verified [Kubernetes evaluation manifest](deploy/kubernetes/README.md): it proves two distinct StatefulSet identities, cross-replica HTTP/WebSocket delivery over TLS-verified PostgreSQL, and same-version Pod replacement with retained state. Five independent [measured workload profiles](docs/POSTGRES_LOAD.md) cover steady delivery, count/age/retained-gap fencing, and a bounded all-client reconnect storm during continued writes and lifecycle changes. Provider failover, external old-primary fencing/cutover, controlled-host capacity, sustained soak, and owner-environment acceptance remain open gates. See [PostgreSQL multi-instance preview](docs/POSTGRES_PREVIEW.md) and [PostgreSQL recovery contract](docs/POSTGRES_BACKUP.md); SQLite remains the default and the supported v0.12 deployment.

PostgreSQL same-version startup inspects schema metadata without replaying DDL against live replicas. Actual schema upgrades still require drained/stopped old replicas and a PostgreSQL-native backup/rollback plan; mixed-version rolling upgrades are not supported.

The preview also checks each replica's unread relay backlog before each batch. Exceeding its configurable event-count or event-age limit closes that replica's sockets with 1012 before a fresh cursor is established. Clients reconnect and reload history; these checks are not a delivery-latency SLA or a hard event-log disk limit. See [live-lag recovery](docs/POSTGRES_PREVIEW.md#live-relay-lag-and-resynchronization).

The [application-workflow guide](docs/APPLICATION_WORKFLOWS.md) and runnable `examples/03_support_workflow.py` show a two-party support case with separate customer and agent identities. [Reliable application webhooks](docs/WEBHOOKS.md) covers receiver verification, retries, replay, rotation, and failure recovery.

## Container quick start

Docker Compose packages the supported one-process topology. Create the two ignored files described in [`secrets/README.md`](secrets/README.md), then run:

```bash
docker compose config --quiet
docker compose build --pull
docker compose up --detach
docker compose ps
curl http://127.0.0.1:8000/readyz
```

The profile publishes only to host loopback, runs as UID/GID 10001, mounts `/data` as the sole durable writable volume, and reads operator/token secrets from `/run/secrets`. It is intentionally single-replica: do not add Uvicorn workers or scale the Compose service. See [Container deployment](docs/CONTAINER_DEPLOYMENT.md) before exposing it through a TLS reverse proxy or relying on its volume.

## TypeScript client

The framework-neutral [`@samsarix/chat-client`](clients/typescript/README.md) source ships in `clients/typescript`. It wraps authenticated HTTP operations and browser-safe first-message WebSocket authentication, emits generated declarations, refreshes credentials on reconnect, and applies bounded exponential backoff without runtime dependencies. The package is verified and packable but is not yet published to npm.

Unpublished SDK 0.5.0 adds `listReplies()` and `sendReply()` while retaining the 0.4 connection contract: it waits for initial history and a post-history activation reply before `connect()` resolves. Attempts have a configurable deadline; retry budgets reset only after a stable activated connection, not merely `ready`. See the [migration and recovery contract](clients/typescript/README.md#reconnect-behavior), including browser-legal close codes and caller-owned history reconciliation.

## WebSocket protocol

Connect to:

```text
ws://127.0.0.1:8000/v1/rooms/{room_id}/ws
```

The server sends `ready` and `history`, then accepts these JSON commands:

```json
{"type":"message","content":"Hello","client_message_id":"browser-42"}
```

To reply to a top-level message, add its ID:

```json
{"type":"message","content":"Contextual follow-up","parent_message_id":"message-id","client_message_id":"browser-43"}
```

```json
{"type":"ping"}
```

```json
{"type":"typing","active":true}
```

Clients receive `message.created`, `message.updated`, `message.deleted`, `typing.started`, `typing.stopped`, room-state, moderation, presence, `pong`, and structured `error` events. Browser clients first receive `auth.required` and reply with `{"type":"auth","token":"..."}`. Token identity supplies the username; legacy local/operator connections still use `?username=`. API keys and tokens are never accepted in query strings.

The exact HTTP and event contracts are in [API reference](docs/API_REFERENCE.md). See [Identity and room authorization](docs/AUTHORIZATION.md) for issuance and permission examples, and [Application workflows](docs/APPLICATION_WORKFLOWS.md) for the end-to-end support-room integration.

## Configuration

All settings are optional for loopback development. Copy [.env.example](.env.example) as a reference; the service reads process environment variables directly and does not automatically load `.env` files. Sensitive settings also accept a mutually exclusive `_FILE` form containing one UTF-8 line, which is preferred for container/orchestrator secrets.

| Variable | Default | Purpose |
| --- | ---: | --- |
| `SAMSARIX_CHAT_DATABASE` | `data/samsarix-chat.db` | SQLite database path |
| `SAMSARIX_CHAT_STORAGE` | `sqlite` | Storage backend; `postgres` selects the guarded v0.13 preview |
| `SAMSARIX_CHAT_POSTGRES_URL_FILE` | unset | Preferred protected PostgreSQL URL file; required with the preview unless direct URL form is used |
| `SAMSARIX_CHAT_POSTGRES_INSTANCE_ID` | unset | Required unique stable replica identity in PostgreSQL mode |
| `SAMSARIX_CHAT_API_KEY` | unset | Shared secret protecting all `/v1` data; minimum 16 characters |
| `SAMSARIX_CHAT_API_KEY_FILE` | unset | File alternative to `API_KEY`; never set both |
| `SAMSARIX_CHAT_TOKEN_SIGNING_SECRET` | unset | Enables signed application-user tokens; minimum 32 bytes |
| `SAMSARIX_CHAT_TOKEN_SIGNING_SECRET_FILE` | unset | File alternative to `TOKEN_SIGNING_SECRET`; never set both |
| `SAMSARIX_CHAT_TOKEN_VERIFICATION_JWKS_FILE` | unset | Static public JWKS for verification-only EdDSA/RS256 auth; mutually exclusive with the signing secret |
| `SAMSARIX_CHAT_TOKEN_ISSUER` | `samsarix-chat-engine` | Required JWT issuer |
| `SAMSARIX_CHAT_TOKEN_AUDIENCE` | `samsarix-chat` | Required JWT audience |
| `SAMSARIX_CHAT_TOKEN_MAX_LIFETIME` | `86400` | Maximum issued/accepted token lifetime in seconds |
| `SAMSARIX_CHAT_TOKEN_CLOCK_SKEW` | `30` | JWT time-claim leeway in seconds |
| `SAMSARIX_CHAT_ALLOWED_ORIGINS` | unset | Comma-separated exact browser origins for CORS/WebSockets |
| `SAMSARIX_CHAT_MAX_MESSAGE_CHARS` | `4000` | Per-message character limit |
| `SAMSARIX_CHAT_MESSAGES_PER_MINUTE` | `60` | Per-client HTTP and per-connection WebSocket message rate |
| `SAMSARIX_CHAT_SEARCHES_PER_MINUTE` | `30` | Per-subject or client-address room-search rate |
| `SAMSARIX_CHAT_MAX_CONNECTIONS` | `200` | Process-wide WebSocket cap |
| `SAMSARIX_CHAT_MAX_CONNECTIONS_PER_ROOM` | `100` | Per-room WebSocket cap |
| `SAMSARIX_CHAT_MAX_ROOMS` | `1000` | Persisted room cap |
| `SAMSARIX_CHAT_MAX_STORED_MESSAGES` | `100000` | Global retained-message cap |
| `SAMSARIX_CHAT_MAX_STORED_MESSAGES_PER_ROOM` | `10000` | Per-room retained-message cap |
| `SAMSARIX_CHAT_MAX_READ_STATES_PER_ROOM` | `10000` | Persisted signed-user read cursors per room |
| `SAMSARIX_CHAT_TYPING_EVENTS_PER_MINUTE` | `60` | Typing commands allowed per signed subject or unauthenticated/operator client address |
| `SAMSARIX_CHAT_TYPING_TIMEOUT` | `8` | Seconds before an active typing signal automatically expires |
| `SAMSARIX_CHAT_MESSAGE_RETENTION_DAYS` | unset | Optional maximum message age, 1–3650 days |
| `SAMSARIX_CHAT_MAX_AUDIT_EVENTS` | `100000` | Retained administrative audit-event cap |
| `SAMSARIX_CHAT_WS_AUTH_TIMEOUT` | `5` | Browser authentication deadline in seconds |
| `SAMSARIX_CHAT_WS_SEND_TIMEOUT` | `2` | Slow-client send timeout in seconds |
| `SAMSARIX_CHAT_WS_MAX_BYTES` | `16384` | WebSocket command frame cap used by the CLI server |
| `SAMSARIX_CHAT_WEBHOOK_URL` | unset | Opt-in host-application callback; HTTPS except literal loopback development |
| `SAMSARIX_CHAT_WEBHOOK_SIGNING_SECRET` | unset | Standard Webhooks `whsec_` current HMAC secret; required with a URL |
| `SAMSARIX_CHAT_WEBHOOK_SIGNING_SECRET_FILE` | unset | File alternative to the current webhook secret |
| `SAMSARIX_CHAT_WEBHOOK_PREVIOUS_SIGNING_SECRET` | unset | Temporary old `whsec_` secret for zero-downtime rotation |
| `SAMSARIX_CHAT_WEBHOOK_PREVIOUS_SIGNING_SECRET_FILE` | unset | File alternative to the previous webhook secret |
| `SAMSARIX_CHAT_WEBHOOK_EVENTS` | all four when URL set | Comma-separated selected committed message/moderation event types |
| `SAMSARIX_CHAT_WEBHOOK_TIMEOUT` | `10` | Total network-attempt budget in seconds, 0.1–30, including the caller's DNS wait; storage operations and whole-process shutdown have separate budgets |
| `SAMSARIX_CHAT_WEBHOOK_MAX_ATTEMPTS` | `9` | Total automatic delivery attempts, 1–20 |
| `SAMSARIX_CHAT_MAX_WEBHOOK_DELIVERIES` | `100000` | Bounded pending/completed outbox rows |
| `SAMSARIX_CHAT_WEBHOOK_ALLOW_PRIVATE_TARGETS` | `false` | Explicitly allow trusted private-network destinations |

The CLI refuses `--host 0.0.0.0` or another non-loopback bind unless an API key or token verifier is configured. `--allow-insecure-public` is an explicit development escape hatch, not a production recommendation. Install `.[asymmetric-auth]` when using a JWKS outside the container image or `.[postgres]` for the PostgreSQL preview; the image includes both extras.

## Embed it

```python
from pathlib import Path

from samsarix_chat_engine import Settings, create_app

app = create_app(Settings(database_path=Path("data/chat.db")))
```

`create_app()` returns a complete FastAPI application. Mounting it under another application is supported, but the parent deployment owns proxy limits, TLS, process topology, and graceful-shutdown behavior.

## Architecture and guarantees

```text
HTTP / WebSocket clients
          |
       FastAPI  -- validation, identity/room auth, error contracts, rate limits
          |
    ConnectionManager -- bounded, in-process room broadcast
          |
       ChatStore -- SQLite transactions, lifecycle audit, bounded retention
          |
  durable outbox -- signed, bounded webhook worker (optional)
```

- SQLite writes are serialized within one process and use `BEGIN IMMEDIATE`; foreign keys, WAL mode, and a five-second busy timeout are enabled.
- A message is persisted before `message.created` is broadcast. An HTTP success therefore means the local database committed it.
- Selected webhook rows commit in the same transaction as their message/moderation change, then deliver at least once in the background. Receivers deduplicate the stable delivery ID.
- Signed users can persist a monotonic per-room read cursor and retrieve a current unread count that excludes their own and deleted messages.
- Search is room-authorized, current-state Unicode-normalized substring matching over at most the configured retained messages for that room; it is not global, fuzzy, or externally indexed.
- Typing signals are transition-only, separately rate-limited, automatically expired, and never persisted or audited.
- WebSocket delivery and presence events are best-effort/at-most-once. Reconnecting clients recover the last 50 messages and can page older history over HTTP.
- Embedded `ConnectionManager` callers register accepted sockets before sending or closing through the manager. Unknown/detached sockets reject sends and ignore duplicate closes; teardown drops queued sends, while a send already in progress may finish before close. Pre-admission frames use the socket directly.
- During initialization, room broadcasts are buffered after registration and flushed after `ready`/`history`, before live delivery starts. Defaults are 64 events / 256 KiB per pending socket and 8 MiB shared serialized payload budget, including in-flight flushes. Overflow or a flush exceeding one send-timeout interval closes 1013 for reconnect/history recovery. History and live events can overlap; merge messages by ID instead of blindly appending. See [the initialization contract](docs/API_REFERENCE.md#server-events).
- PostgreSQL admission records the committed join-event sequence. A delayed relay skips earlier events for that new socket, preventing an old archive or ban from closing a later authorized reconnect. Events after admission still apply, including to older sockets; this is not durable per-client replay or a catch-up-complete marker.
- Authenticated connection setup and receive-loop failures share cancellation-protected cleanup. Storage failures produce `storage_unavailable`/1012 without database details; clients reconnect with backoff and reload history. PostgreSQL reservation release is best effort during outages, with lease expiry as the backstop.
- SQLite remains one-process only. The guarded PostgreSQL preview uses database-owned cursors, connection leases, rate buckets, typing state, presence, and an ordered event log across replicas; it is not a supported scale claim until the published acceptance gates pass.
- Retention always applies configured count caps and can additionally apply an operator-selected maximum age.

## Security, privacy, and operating cost

The default loopback bind avoids accidental network exposure. The API key is an all-room operator credential; do not ship it to browsers. Host applications authenticate users and issue short-lived room tokens whose subject becomes the server-enforced sender identity. Production deployments can give the engine only public verification keys so private signing authority remains in the host application. Configure TLS at a reverse proxy and exact allowed browser origins for any network deployment.

Messages and display names are stored as plaintext in the configured SQLite file. The engine does not collect telemetry or put message bodies or API keys in its administrative audit trail. When explicitly configured, webhook payloads send selected message content and identifiers to the operator's endpoint and retain a payload copy in the bounded outbox. Backups, exports, webhook receivers, filesystem permissions, retention policy, user consent, and legal obligations remain the deployment owner's responsibility. Default operation has no metered API cost; its operating costs are compute, disk, backup, webhook requests, and network transfer only.

## Development and release checks

```bash
python -m pip install --upgrade pip setuptools wheel
python -m pip install -e ".[dev]"
python -m ruff check .
python -m ruff format --check .
python -m mypy samsarix_chat_engine
python -m pip_audit
python -m pytest --cov=samsarix_chat_engine --cov-report=term-missing
python -m build
python -m twine check dist/*
python scripts/smoke_installed_wheel.py  # run with an installed wheel's Python
```

CI runs the tests on CPython 3.10–3.14 on Linux and CPython 3.12 on Windows, verifies the TypeScript package, and builds/smokes the hardened Linux container. See [Contributing](CONTRIBUTING.md) and the living [productization record](docs/PRODUCTIZATION.md).

Tagged releases use a tag-only, least-privilege workflow that builds the wheel and source archive once, verifies a fresh wheel install, publishes SHA-256 checksums, and creates GitHub/Sigstore provenance attestations before attaching the same files to a versioned GitHub prerelease. It does not publish to PyPI or npm. Maintainers and consumers can follow the [release integrity and verification guide](docs/RELEASING.md).

## Limitations and project status

This is a coherent single-instance MVP, not a hosted chat platform. The Compose profile supports exactly one SQLite process and replica. The container also includes the guarded PostgreSQL extra so the checked [Kubernetes evaluation manifest](deploy/kubernetes/README.md) can run reviewed development images. The [PostgreSQL preview](docs/POSTGRES_PREVIEW.md) wires the accepted [multi-instance architecture](docs/MULTI_INSTANCE_ARCHITECTURE.md) through real application instances, but remains explicitly unreleased until its remaining process-failure, failover, and deployment-acceptance gates pass. Attachments with explicit storage policy follow. Those are intentionally not presented as current supported capabilities.

## License

Copyright (c) 2026 Samsarix LLC. The source is licensed under the [Mozilla Public License 2.0](LICENSE). MPL-2.0 keeps distributed modifications to covered source files open and preserves license notices, while allowing those files to be combined with separate proprietary files in a larger work.

The canonical Python package, command, and environment prefix are `samsarix_chat_engine`, `samsarix-chat`, and `SAMSARIX_CHAT_*`. Version 0.12 keeps `helix_chat_engine`, `helix-chat`, and `HELIX_CHAT_*` as deprecated compatibility aliases. If only `data/helix-chat.db` exists, the CLI reuses it so the rename does not hide existing data.

For general inquiries, email contact@samsarix.com. For product support and private security reports, email support@samsarix.com or read [SECURITY.md](SECURITY.md).
