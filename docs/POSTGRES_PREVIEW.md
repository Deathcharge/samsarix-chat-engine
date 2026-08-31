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

All replicas must run the exact same Samsarix version and use identical authentication, limits, webhook, retention, and timing settings. Use `/readyz` to decide whether a replica should receive traffic and `/healthz` for process liveness. Liveness does not depend on PostgreSQL. Readiness requires the schema, pool, running coordination loops, and a locally unexpired relay claim with no pending socket fence or recovery. A live task alone is not evidence that the relay can serve traffic. WebSocket admission captures both the database generation and a local admission epoch before reserving capacity, then rechecks them under the same local lock used to detach fenced sockets. A reservation that crosses a fence is released and rejected even if the relay recovered with the same database generation in the meantime.

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
| `SAMSARIX_CHAT_POSTGRES_OPERATION_TIMEOUT` | `10` | Checked-out operation deadline seconds, 0.1–300; also sets per-session statement and idle-in-transaction limits |
| `SAMSARIX_CHAT_POSTGRES_LEASE_SECONDS` | `30` | Process and socket lease, 3–300 seconds |
| `SAMSARIX_CHAT_POSTGRES_RELAY_POLL` | `0.25` | Durable-log poll interval, 0.01–5 seconds |
| `SAMSARIX_CHAT_POSTGRES_MAINTENANCE_INTERVAL` | `1` | Bounded cleanup cadence, 0.1–60 seconds |
| `SAMSARIX_CHAT_POSTGRES_MAX_RATE_BUCKETS` | `100000` | Deployment-wide active bucket cardinality cap |
| `SAMSARIX_CHAT_POSTGRES_MAX_REALTIME_EVENTS` | `100000` | Retained coordination-event count target |
| `SAMSARIX_CHAT_POSTGRES_REALTIME_EVENT_MAX_AGE` | `604800` | Retained coordination-event age in seconds |

`SAMSARIX_CHAT_DATABASE` and the CLI `--database` option cannot be combined with PostgreSQL mode. The SQLite `database backup` and `database restore` commands refuse PostgreSQL rather than pretending a copied local file protects a remote database.

## Database connection interruption and recovery

On a detected database connection failure, the relay fences its existing local sockets with close code `1012`. The process remains live, but readiness returns `503` while storage or the relay claim is unavailable. A failed explicit socket/instance release is logged by exception type only and left for lease expiry; shutdown still closes the pool. Once connectivity returns, the process reacquires its cursor and resumes polling without requiring an application restart. Clients must reconnect with backoff and reload authoritative history; they must not expect missed events to reappear on their closed socket.

`tests/test_postgres_processes.py::test_database_network_cut_fences_clients_and_recovers_without_process_restart` cuts one replica's real database TCP connections using a test-only loopback proxy and refuses replacement connections. A second Uvicorn process connects directly to the dedicated `samsarix_test` database. The test checks that the healthy peer keeps publishing, rejected writes do not appear in recovered history, readiness returns after reconnection, and fresh sockets again receive cross-replica messages. It does not stop PostgreSQL or modify its networking configuration.

The test now runs both connection reset/refusal and silent bidirectional traffic-stall cases. In the stall case, the loopback proxy keeps TCP connections open but withholds application traffic in both directions, including replacement connection handshakes. Existing sockets close with `1012`, readiness fails, the healthy peer continues publishing, and restoring forwarding allows fresh connections and history recovery. This models stalled database traffic, not kernel-level packet loss or a PostgreSQL failover.

The test uses a three-second lease, one-second pool timeout, and two-second operation deadline for the interrupted replica. Other replicas retain the default ten-second pool and operation limits. Child diagnostics are drained continuously into a bounded in-memory tail. These are accelerated CI settings, **not** a production latency guarantee.

## Operation deadlines and ambiguous outcomes

Pool acquisition and checked-out operation time are separate limits. After checkout, the operation timer covers transaction setup, queries, and commit/rollback, including schema initialization. On expiry, the client closes the PostgreSQL session before interrupting its waiter, avoiding cancellation cleanup that would itself wait on the stalled transport. The unusable connection is discarded by the pool. The libpq connection timeout is at least two seconds (its minimum) and otherwise the pool timeout rounded up to whole seconds, preventing replacement connection handshakes from waiting indefinitely.

Every connection also receives per-session `statement_timeout` and `idle_in_transaction_session_timeout` values equal to the operation limit in milliseconds. Existing unrelated connection options are preserved. These server-side limits bound individual statements and idle open transactions when a client cannot communicate cancellation; they do not make network transport reliable or replace a PostgreSQL failover strategy. Administrators should size the limit for their workload, including startup migrations and large exports. A request or shutdown may perform several sequential database operations; the setting is not a total HTTP-request or application-shutdown deadline.

A timed-out write has an **unknown outcome** if PostgreSQL committed before its reply was lost. A `503 storage_unavailable` response does not prove rollback. Reconcile authoritative state and reuse the same `Idempotency-Key` or `client_message_id` when retrying message creation; do not blindly replay other mutations. Clients reconnect with backoff and reload history after socket loss. The process fault test withholds traffic before its rejected write starts and proves absence only for that controlled case.

The design follows Psycopg's [pool timeout contract](https://www.psycopg.org/psycopg3/docs/api/pool.html) and [async cancellation caveats](https://www.psycopg.org/psycopg3/docs/advanced/async.html#interrupting-async-operations), plus PostgreSQL's [statement and idle-transaction timeout guidance](https://www.postgresql.org/docs/current/runtime-config-client.html). Python 3.11+ can preserve an overlapping caller cancellation separately from the deadline; Python 3.10 lacks that cancellation-count API, so the overlapping-cancellation discrimination test is explicitly skipped there.

## Moderation and socket teardown

Authenticated handshake cleanup covers failures and cancellation before and after local registration, including room/moderation rechecks, history loading, counts, and initial frame delivery. The handler owns a successful reservation through finalization and stops its background tasks before release. If admission loses its database result or is cancelled before returning, it attempts idempotent release; known duplicate-ID rejection does not release another existing lease. A database outage or ambiguously late commit can still require lease expiry, so this is not an instantaneous capacity-recovery guarantee under arbitrary network failure.

Database-independent tests exercise ASGI cancellation scopes and repeated `Task.cancel()` calls; live PostgreSQL tests inject failure after a committed reservation, verify the physical lease table is empty after teardown, and reconnect successfully. Individual close, pool, and operation deadlines remain in force while cleanup is shielded. This is not a total request/shutdown deadline or protection from process kill; crashed-process recovery still relies on database leases.

CI runs two independent Uvicorn processes with short-lived, signed, room-scoped member identities and a separate operator key. The acceptance journey proves freeze/unfreeze and mute/unmute enforcement over HTTP and WebSocket, member rejection at administrative endpoints, ban eviction on both replicas with code 4403, reconnect denial until unban, and archive eviction with code 4409 followed by reopen/history recovery. A same-subject socket in an unrelated room stays connected and writable; unaffected peers remain connected.

Authoritative database checks govern reads and mutations; remote socket notifications arrive asynchronously through the polling relay. A successful administrative HTTP response is not an acknowledgement that every remote socket has physically closed. On local teardown, the manager first detaches matching sockets so late direct sends and queued broadcast snapshots cannot bypass the close. A send already in progress may finish before the close operation. Unavailable sends trigger a bounded close attempt with code 1013; a broken transport may prevent the peer observing that code.

Additional live PostgreSQL application tests hold the archive relay behind an explicit barrier so the connection heartbeat observes archive, reservation reaping, or room deletion first; an unrelated room remains connected. Admission tests change room state after the first validation but before database reservation. Failed renewal checks the authenticated room even if its reservation row was removed: verified archive produces `room.archived`/4409, verified deletion produces `room_not_found`/4404, and actual storage/lease uncertainty remains 1012. Healthy renewal stays a single query; the diagnostic query runs only after renewal fails and shares the transaction's operation deadline. The final room snapshot is observed state, not a promise it cannot change again.

These controlled barriers run through real application handlers and PostgreSQL, not separate network subprocesses; the two-process normal moderation journey is separate evidence. They do not cover every combination of outage, failover, rapid archive/reopen, or delayed events arriving during a fresh handshake. Individual-socket delivery remains best effort; after any disconnect, clients reauthorize and reload current history rather than treating an error frame as durable state.

## Initialization handoff

Registered sockets now buffer room broadcasts while reading/sending initial history. Activation drains those snapshots before enabling live broadcasts; overflow or a flush exceeding one send-timeout interval closes 1013 for reconnect. The shared buffer budget also counts in-flight activation sends, preventing detachment from making their retained payload invisible to accounting. See the [protocol limits](API_REFERENCE.md#server-events). This queue is local and ephemeral, and is discarded on cancellation, moderation teardown, fencing, or disconnect.

Controlled SQLite and PostgreSQL application tests pause a captured history snapshot, commit create/edit/delete operations through HTTP, wait for dispatch, and then verify that initial history plus queued mutations converges to current database rows, including tombstones. This is real-storage/ASGI evidence, not a network-process stall, load, or failover benchmark. Old relay events can overlap history and duplicates remain possible; no durable per-client cursor or end-of-catch-up marker is added. Rapid archive/reopen ordering and delayed lifecycle events remain unverified release gates.

## Migration, backup, and rollback

Startup takes a transaction-scoped advisory migration lock and advances the internal PostgreSQL schema to version 8. Newer unknown schemas fail closed. Before the first preview startup, take a provider-native physical/PITR backup or a tested logical backup that includes all `public.samsarix_*` tables and identity sequences.

Rolling back to a binary that supports an older schema requires restoring its matching pre-upgrade database backup. Dropping the retention table or editing schema metadata is not a supported downgrade. PostgreSQL availability, TLS roots, least-privilege roles, patching, PITR, replication, failover, vacuuming, and capacity remain deployment-owner responsibilities.

## Known preview boundaries

- Two-process normal delivery, kill/lease-expiry/restart, database TCP reset/refusal, and silent bidirectional database-traffic stalls with explicit reconnect/history recovery run in CI. Kernel-level packet blackholes, database failover, notification-listener interruption, reconnect storms, and sustained load/soak evidence remain pending.
- A live but extremely slow replica can currently hold the event-retention floor; the configurable live-lag fence is not implemented yet.
- Polling, rather than `LISTEN`/`NOTIFY`, currently determines normal fan-out latency.
- The bundled Compose profile is still the supported SQLite single-replica example and does not provision PostgreSQL.
- Presence and WebSocket delivery remain best effort at the individual socket boundary. Reconnecting clients reload authoritative history over HTTP.

Report preview issues without credentials or message content to support@samsarix.com. Private security reports follow [SECURITY.md](../SECURITY.md).
