# PostgreSQL multi-instance preview

Status: **guarded, unreleased v0.13 preview**.

The repository's development branch can run the complete HTTP and WebSocket application against PostgreSQL. SQLite remains the default and the v0.12 supported topology remains one process. CI now exercises two real Uvicorn processes and crash-lease recovery, but do not describe the preview as production-supported until the remaining interruption, reconnect, load/soak, backup, and rollback gates in the [multi-instance architecture](MULTI_INSTANCE_ARCHITECTURE.md) are published.

## What the preview wires

Every replica uses PostgreSQL for rooms, messages, search, moderation, read state, audit events, retention, and the webhook outbox. The same database coordinates:

- an ordered transactional event log and one exclusively generation-fenced cursor per replica;
- cross-replica message, room-state, moderation, presence, and typing delivery;
- deployment-wide and per-room WebSocket capacity;
- database-time message, search, and typing rate buckets;
- connection heartbeats, crash reclamation, and bounded typing/connection cleanup;
- bounded event retention that never crosses a live cursor and fences stale replicas after a retained gap.

The relay polls the durable log for correctness. PostgreSQL notifications are emitted by transactions but are not yet used as the wake-up optimization, so `SAMSARIX_CHAT_POSTGRES_RELAY_POLL` bounds normal cross-replica latency.

## Configure one replica

Install the optional dependency:

```bash
python -m pip install ".[postgres]"
```

Put one PostgreSQL URL in a protected, single-line UTF-8 file. A loopback development database may omit TLS parameters:

```text
postgresql://samsarix:replace-me@127.0.0.1:5432/samsarix
```

A non-loopback URL must use certificate and hostname verification:

```text
postgresql://samsarix:replace-me@db.example.com:5432/samsarix?sslmode=verify-full
```

Start one process with a unique, stable instance ID:

```bash
export SAMSARIX_CHAT_STORAGE=postgres
export SAMSARIX_CHAT_POSTGRES_URL_FILE=/run/secrets/samsarix-postgres-url
export SAMSARIX_CHAT_POSTGRES_INSTANCE_ID=chat-a
export SAMSARIX_CHAT_API_KEY_FILE=/run/secrets/samsarix-operator-key
samsarix-chat serve --host 127.0.0.1 --port 8000
```

Use a separate ID such as `chat-b` for a second replica. A concurrently active duplicate ID fails closed instead of sharing an event cursor. Run one Uvicorn worker per configured process; `--workers` would copy the same instance ID into multiple workers and is intentionally rejected by the database claim. A graceful shutdown releases the claim, while a crashed owner remains fenced until its lease expires.

All replicas must run the exact same Samsarix version and use identical authentication, limits, webhook, retention, and timing settings. Put a health-aware load balancer in front of `/healthz` and `/readyz`; liveness does not depend on PostgreSQL, while readiness requires the schema, pool, relay, and maintenance loops.

## PostgreSQL settings

| Variable | Default | Contract |
| --- | ---: | --- |
| `SAMSARIX_CHAT_STORAGE` | `sqlite` | Set exactly `postgres` to select this backend |
| `SAMSARIX_CHAT_POSTGRES_URL` | unset | Direct PostgreSQL URL; prefer the file form |
| `SAMSARIX_CHAT_POSTGRES_URL_FILE` | unset | Mutually exclusive protected URL file |
| `SAMSARIX_CHAT_POSTGRES_INSTANCE_ID` | unset | Required 1–128 character unique stable replica ID |
| `SAMSARIX_CHAT_POSTGRES_MIN_POOL_SIZE` | `1` | Per-process idle pool floor, 0–100 |
| `SAMSARIX_CHAT_POSTGRES_MAX_POOL_SIZE` | `10` | Per-process pool ceiling, 1–100 |
| `SAMSARIX_CHAT_POSTGRES_POOL_TIMEOUT` | `10` | Pool acquisition/startup timeout seconds, 0.1–60 |
| `SAMSARIX_CHAT_POSTGRES_LEASE_SECONDS` | `30` | Process and socket lease, 3–300 seconds |
| `SAMSARIX_CHAT_POSTGRES_RELAY_POLL` | `0.25` | Durable-log poll interval, 0.01–5 seconds |
| `SAMSARIX_CHAT_POSTGRES_MAINTENANCE_INTERVAL` | `1` | Bounded cleanup cadence, 0.1–60 seconds |
| `SAMSARIX_CHAT_POSTGRES_MAX_RATE_BUCKETS` | `100000` | Deployment-wide active bucket cardinality cap |
| `SAMSARIX_CHAT_POSTGRES_MAX_REALTIME_EVENTS` | `100000` | Retained coordination-event count target |
| `SAMSARIX_CHAT_POSTGRES_REALTIME_EVENT_MAX_AGE` | `604800` | Retained coordination-event age in seconds |

`SAMSARIX_CHAT_DATABASE` and the CLI `--database` option cannot be combined with PostgreSQL mode. The SQLite `database backup` and `database restore` commands refuse PostgreSQL rather than pretending a copied local file protects a remote database.

## Migration, backup, and rollback

Startup takes a transaction-scoped advisory migration lock and advances the internal PostgreSQL schema to version 8. Newer unknown schemas fail closed. Before the first preview startup, take a provider-native physical/PITR backup or a tested logical backup that includes all `public.samsarix_*` tables and identity sequences.

Rolling back to a binary that supports an older schema requires restoring its matching pre-upgrade database backup. Dropping the retention table or editing schema metadata is not a supported downgrade. PostgreSQL availability, TLS roots, least-privilege roles, patching, PITR, replication, failover, vacuuming, and capacity remain deployment-owner responsibilities.

## Known preview boundaries

- Two-process normal delivery and kill/lease-expiry/restart recovery run in CI; forced database/listener interruption, explicit reconnect recovery, reconnect storms, and sustained load/soak evidence remain pending.
- A live but extremely slow replica can currently hold the event-retention floor; the configurable live-lag fence is not implemented yet.
- Polling, rather than `LISTEN`/`NOTIFY`, currently determines normal fan-out latency.
- The bundled Compose profile is still the supported SQLite single-replica example and does not provision PostgreSQL.
- Presence and WebSocket delivery remain best effort at the individual socket boundary. Reconnecting clients reload authoritative history over HTTP.

Report preview issues without credentials or message content to support@samsarix.com. Private security reports follow [SECURITY.md](../SECURITY.md).
