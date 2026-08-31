# Changelog

This project follows semantic versioning while it is in alpha: minor versions may add or revise public contracts, and those changes are called out here.

## Unreleased

### Maintenance

- Keep SQLite-only test collection working without the optional PostgreSQL driver; the load-harness module explicitly skips in that environment. Add a clean `.[test]` CI job that asserts driver absence and runs the applicable suite, alongside the existing full-driver/live PostgreSQL jobs.
- Update disposable PostgreSQL CI services from 18.4 to 18.6 after reviewing the official security/bug-fix release. Historical load measurements retain their actual server version; production database upgrades and release-note-specific cleanup remain operator responsibilities.

### Added

- A guarded two-replica Kubernetes PostgreSQL evaluation manifest uses StatefulSet Pod names as stable unique database-lease identities, a manual `OnDelete` version-update gate, mounted secret files, non-root/read-only containers, readiness/liveness/startup probes, resource bounds, anti-affinity, an internal Service and a PodDisruptionBudget. A fail-closed structural verifier rejects automatic rollout, identity, selector, credential, secret-mount and hardening regressions. A real-process PostgreSQL test requires a duplicate live identity to exit without disrupting the owner, then proves graceful same-identity replacement and retained state. The container image now includes the PostgreSQL extra; the supported Compose topology remains SQLite-only and single-replica. External database failover/fencing, ingress, NetworkPolicy, capacity and owner acceptance remain open gates.
- Disposable PostgreSQL 18.6 physical point-in-time recovery rehearsal. CI enables WAL archiving, verifies a SHA-256-manifest `pg_basebackup`, commits application state before and after a named restore point, confirms the required WAL segment is archived, fences the old primary from its application role, recovers and promotes a second cluster onto a new timeline, excludes the divergent post-target write, and proves search/read-state/audit invariants plus post-recovery writes. A content-free evidence artifact records exact revision and hashes. Provider failover, external process/network fencing, durable off-host archives and measured RPO/RTO remain deployment gates.
- Disposable PostgreSQL 18.6 logical-backup restore rehearsal. CI seeds representative schema-8 application state, creates and inspects a native custom-format whole-database archive, restores it transactionally into a fresh `template0` database, and verifies readiness, edited/tombstoned history, search, read state, lifecycle/audit data and post-restore writes. The probe fails closed outside two exact loopback scratch database names and rejects URL or libpq-environment routing overrides. The accompanying recovery contract separates portable logical rollback from physical recovery and deployment-owned failover, credential/role provisioning, privacy, RPO/RTO and webhook deduplication responsibilities.
- Version 2 load reports add ten-second completion-time latency windows plus bounded application/driver CPU, I/O and RSS/PSS counters, host pressure, database activity/wait categories, cumulative database statistics and privacy-safe static fence-signal counts. Missing pressure is null, statistics limitations are explicit, and no query text, hostname, address, username, token or raw server log enters the report.
- Checkout-only PostgreSQL load/recovery tooling with bounded open arrivals, create/edit/delete fan-out, acknowledged-event coverage, final history convergence, room isolation, resource/latency reporting and live count/age/natural-gap fault profiles. Disposable CI jobs preserve failed as well as successful measurements; a manual workflow supports longer probes. It rejects unsafe targets and environment routing overrides, never publishes telemetry, and does not imply production capacity or change the supported SQLite alpha / PostgreSQL preview boundary. See [the workload contract and evidence](docs/POSTGRES_LOAD.md).
- Real two-process Linux pause/recovery acceptance for live count/age lag and natural lease-expiry/retained-gap recovery. Tests preserve kernel-stop and database-idleness barriers, exercise edits/deletion and freeze/archive/reopen during the pause, reject obsolete frames, verify generation/cursor and physical lease cleanup, and reconnect four signed members to authoritative history and fresh fan-out. The healthy unrelated-room connection remains usable. These are bounded correctness scenarios, not load/soak or reconnect-storm capacity claims.
- Real two-process webhook crash acceptance with raw-body signature verification, withheld acknowledgements, killed-worker claim persistence, natural 60-second database lease expiry, stable-ID/payload recovery, deletion-before-reclaim suppression and continued survivor delivery. Recorded attempt counts exclude requests whose outcomes were lost in a crash; receivers still require deduplication.
- Real two-process contention acceptance for idempotent creates, ordered edits/deletes and reconnect history, shared connection/room caps, and signed-subject HTTP/search/WebSocket/typing rate budgets. Database lock barriers require both replicas to wait before release. This adds bounded correctness evidence, not a throughput benchmark or promotion of the PostgreSQL preview.
- Unpublished TypeScript SDK 0.4.0 bounds each connection attempt with `handshakeTimeoutMs` and resets automatic retry budgets only after `stableConnectionMs` of activated connection time (both default 10000 ms). `connect()` now waits for ready/history plus a post-history ping reply; callers must await it before publishing. Browser-legal cleanup codes, stale-generation/timer fencing, reentrant-listener cancellation, capped jitter and strict timer/close validation protect recovery. The package README documents these contract changes; a native-WebSocket smoke verifies credential-refresh reconnect, offline edits/history and resumed delivery.
- PostgreSQL live-relay backlog limits: `SAMSARIX_CHAT_POSTGRES_RELAY_MAX_PENDING_EVENTS` (default 10000) and `SAMSARIX_CHAT_POSTGRES_RELAY_MAX_EVENT_AGE` (default 30 seconds). A pre-batch count/age violation fails readiness and fences local sockets with 1012 before rotating the claim to the committed head; reconnecting clients reload history. Recovery retries preserve one UUID/cursor after an ambiguous reply and cannot overwrite another owner. The application uses this retry-safe recovery for retained gaps too. Limits do not promise a maximum delivery delay or hard disk bound.
- Accepted PostgreSQL multi-instance architecture and explicit cross-process correctness, recovery, capacity, moderation, webhook, migration, and failure-test gates for v0.13.
- Storage-neutral `ChatStorage` protocol between application/webhook orchestration and the existing SQLite backend.
- Optional `postgres` installation extra and an internal PostgreSQL foundation with advisory-lock schema initialization, a transaction-coupled ordered realtime event log, and durable per-instance cursors.
- Internal PostgreSQL schema v3 and a complete `ChatStorage` implementation for rooms, bounded messages, Unicode-normalized search, moderation, audit history, monotonic read state, stable spooled exports, explicit retention, and cross-instance idempotency/capacity enforcement.
- Transactional PostgreSQL webhook outbox with stable delivery IDs, database-time scheduling, expiring worker-owned claims, `SKIP LOCKED` work selection, crash recovery, bounded terminal-history pruning, and operator replay.
- Internal per-process PostgreSQL realtime relay with durable cursors, ordered polling/replay, post-dispatch acknowledgement, archive/ban socket teardown, and lease-loss fencing. Polling is the correctness path; a future `LISTEN`/`NOTIFY` listener may only reduce latency.
- PostgreSQL schema v4 and an internal connection registry with database-time socket leases, atomic deployment-wide and per-room capacity, owner-bound renewal/release, archived-room rejection, and crashed-process reclamation.
- PostgreSQL schema v5 and internal deployment-wide message, search, and typing rate buckets with atomic per-identity consumption, database-time boundaries, bounded active cardinality, and raw-key minimization.
- PostgreSQL schema v6 and internal connection-bound typing state with transition-only starts, refresh without event storms, explicit stops, database-time expiry, and bounded concurrent sweeping into durable coordination events.
- PostgreSQL schema v7 and lease-derived presence transitions with exact join/explicit-leave counts, bounded crashed-process sweeping, typing-before-leave ordering, and process-generation fencing for safe stable-ID restarts.
- PostgreSQL schema v8 realtime-retention metadata, bounded count/age pruning behind every live cursor, explicit retained-gap detection, and relay recovery that fences sockets, rotates the stale process generation, and resumes from the authoritative event head.
- Guarded PostgreSQL application configuration and lifecycle orchestration for shared HTTP storage, cross-instance WebSocket messages and room state, global socket capacity/rate controls/stats, leased connection renewal, sender-excluded presence/typing, bounded maintenance, and readiness.
- A PostgreSQL preview deployment guide covering protected URL files, mandatory remote `verify-full` TLS, unique replica identities, pool/lease/retention bounds, migration, backup/rollback ownership, and remaining release gates.
- A real-network acceptance test that launches two independent Uvicorn processes, verifies cross-process HTTP/WebSocket delivery, kills one replica, observes lease-derived presence/capacity convergence, restarts its stable identity after expiry, and reloads durable history.
- A real two-process database connection-reset/refusal test with independent healthy-peer progress, unavailable-write rejection, explicit client reconnect/history recovery, and resumed fan-out without an application restart.
- Real two-process moderation acceptance with signed room-scoped members: freeze/unfreeze, mute/unmute, ban/unban and reconnect denial, archive/reopen with retained history, and unrelated-room isolation.

### Changed

- Webhook delivery now uses one monotonic network-attempt deadline across the caller's DNS wait, connect, TLS handshake, writes and response headers. Timeout/cancellation interrupts the owned socket; late results cannot overwrite a timeout. One daemon worker/slot per dispatcher bounds native resolver work and prevents further claims while it lingers, without holding up interpreter exit. Stop prevents new claims and leaves unfinished outcomes pending for recovery. Storage operations and whole-process cleanup retain separate budgets; supervisor deadlines and receiver deduplication remain required. Direct `process_due_once()` embedders must stop their dispatcher when finished.
- PostgreSQL current-schema startup now inspects without replaying DDL or rewriting metadata/retention rows, avoiding exclusive table locks against active replicas. New/older schemas retain serialized transactional migration; newer schemas fail without DDL and readiness requires exact schema compatibility. Actual upgrades still require stopping old replicas.
- PostgreSQL socket admission now carries its transaction's committed join-event sequence into local registration. The relay supplies event sequences out of band, and broadcast/archive/ban actions ignore events at or before each socket's admission. Delayed archive/reopen, ban/unban, room recreation, freeze/unfreeze, and historical presence therefore cannot overwrite a later admission; older sockets still receive applicable later actions. No schema or public event-envelope change. Embedded manager callers may opt into `after_sequence` / `event_sequence`; unsequenced local actions retain their previous behavior.
- Buffer room broadcasts during registered WebSocket initialization instead of dropping them. Flush immutable event snapshots after ready/history in local arrival order, with per-socket event/byte limits, a shared byte budget, and a deadline for the whole flush. Overflow or failed/slow synchronization closes 1013; cancellation and every detachment path release the buffer. Embedded `ConnectionManager` callers may tune `max_pending_events`, `max_pending_bytes`, and `max_total_pending_bytes`; application defaults are 64, 262144, and 8388608 respectively. This is bounded handoff, not durable per-client delivery or exactly-once replay.
- `ConnectionManager.send()` now returns false for unknown or detached sockets, `close()` is a no-op for them, and duplicate active registration is rejected. Embedders must register an accepted socket before using manager-owned send/close; pre-admission protocol frames remain the caller's responsibility.
- `ConnectionManager.close()` returns the detached `(room_id, username)` to the winning closer, or `None` for an already-detached socket, allowing one-owner finalization without losing departure metadata.
- `ConnectionManager.close(..., event=...)` atomically owns a final notification and physical close, avoiding duplicate lifecycle frames when heartbeat and relay closers race.
- AnyIO, already used transitively by Starlette, is an explicit bounded runtime dependency for ASGI cancellation-scope protection.

### Security and operations

- Multi-process SQLite is explicitly rejected: WAL is same-host-only, single-writer, and current SQLite documentation identifies a concurrent WAL-reset corruption race affecting versions through 3.51.2.
- Version 0.12 remains a one-process/one-replica release until the PostgreSQL acceptance gates pass; no horizontal-scale claim is introduced by this architecture increment.
- PostgreSQL credentials are excluded from translated availability errors; backend selection is explicit and remains a guarded preview.
- Message deletion, room deletion, and automatic or explicit retention scrub message bodies from retained realtime event and terminal-webhook envelopes in the same transaction, and cancel unsent sensitive payloads.
- PostgreSQL event and webhook envelopes remain bounded at 512 KiB so every valid 100,000-character message, including four-byte Unicode, fits without turning a valid domain write into a coordination failure.
- PostgreSQL webhook claims can be acknowledged only by the live lease owner. Expired claims are safely redelivered with the same ID; receivers must still deduplicate because delivery is at least once.
- A PostgreSQL relay that loses its database lease closes every local socket before renewing the same durable cursor. Unsupported internal event types are not forwarded onto the public WebSocket protocol.
- Expired process IDs rotate their generation token on re-registration, preventing a restarted replica from reviving phantom occupancy while leaving stale socket rows available for bounded presence convergence. Archived, expired, and generation-mismatched reservations stop consuming capacity even before physical cleanup.
- Distributed rate buckets persist only a scope-separated SHA-256 digest of the caller key. The digest reduces routine identity exposure but is not anonymization; database access and retention still require normal privacy controls.
- Internal typing coordination events retain an opaque origin connection ID for sender exclusion. The public relay strips that ID before forwarding; clients must continue treating advertised expiry as the stop-event backstop.
- A restarted process rotates its database generation token after lease expiry. Connection, typing, count, renewal, and cleanup queries require the matching generation so stale sockets cannot regain capacity, activity, or presence merely because an operator reused a stable instance name.
- Event pruning records the greatest intentionally removed sequence even when no event rows remain. A returning stale relay cannot mistake that empty window for a healthy cursor: it closes local sockets before skipping to current authoritative state, and its generation rotation makes old connection leases non-live.
- PostgreSQL replica IDs are exclusive generation-owned claims: a duplicate active owner fails closed, graceful shutdown releases the exact generation, and a stale process cannot heartbeat, read, or acknowledge after a replacement takes ownership.
- PostgreSQL readiness and socket admission require a locally unexpired relay claim without pending fencing/recovery, not merely a running task. Failed lease cleanup defers to expiry while still closing the pool and omits exception contents from logs.
- Socket admission revalidates a database-generation/local-epoch token under the connection-manager lock, so reservations cannot register after an intervening relay fence or same-generation recovery; rejected reservations release their capacity.
- Process acceptance tests drain child diagnostics concurrently into a bounded tail and limit accelerated pool timeouts to the interrupted replica.
- Configurable PostgreSQL operation deadlines cover checked-out transactions and startup migrations, discard stalled sessions before cancellation cleanup, and complement finite connection/statement/idle-transaction limits. Timeout responses explicitly do not promise rollback of an ambiguously acknowledged write.
- PostgreSQL fault acceptance now includes a silent bidirectional application-traffic stall over open TCP connections, healthy-peer progress, and recovery without an application restart. Database-independent deadline tests run across the Python/OS matrix.
- Socket teardown detaches membership before physical close and rejects queued stale broadcast snapshots and late sends. A send already in progress can finish before the serialized close. Failed sends now attempt a bounded physical close with code 1013 instead of only forgetting the socket.
- Authenticated WebSocket session cleanup now covers initial database reads, admission, ready/history delivery, background tasks, and the receive loop. ASGI cancellation scopes and repeated direct cancellation cannot abandon its bounded cleanup operations. Storage failures send a non-leaking `storage_unavailable` error and close with 1012; unexpected failures close with 1011.
- Individual manager closes retain ownership through their bounded physical close even if their caller is cancelled after detachment; cancelling a closing heartbeat cannot strand the transport.
- Ambiguous PostgreSQL admission outcomes trigger best-effort idempotent reservation release, with lease expiry as the outage backstop. Known duplicate-reservation errors do not release the existing owner. Live tests inspect physical lease rows after failed handshakes and verify reconnect.
- PostgreSQL admission and failed renewal distinguish an authoritatively archived or deleted room from storage/lease failure, even after maintenance removed the reservation. Admission returns `room_archived`/4409 or `room_not_found`/4404; an established heartbeat can send `room.archived`/4409 before the relay. Healthy renewal remains one query; uncertain storage/lease failures still close with 1012.
- The asymmetric-authentication, test, and development dependency ranges now require `cryptography>=50,<51`, excluding the `cryptography>=44.0.0,<50.0.0` range affected by `PYSEC-2026-3552`.

## 0.12.0 — 2026-08-02

### Added

- Verification-only access-token mode backed by a bounded static public JSON Web Key Set.
- EdDSA/Ed25519 and RS256 verification with required `kid`, explicit per-key algorithm binding, issuer/audience/type checks, and overlapping-key rotation.
- `asymmetric-auth` installation extra and asymmetric dependencies in the supported container image.
- Adversarial crypto, key-set, configuration, HTTP, and WebSocket tests plus a production cutover/rotation runbook.

### Changed

- Python distribution and service metadata advance to 0.12.0; the TypeScript client remains 0.3.0 because its wire protocol is unchanged.
- The HS256 issuer/verifier remains compatible, while HS256 and JWKS trust modes are intentionally mutually exclusive.
- Measured multi-instance work moves to v0.13 so signing authority can first be separated without adding storage or network coordination failure modes.

### Security and operations

- Hosts can retain private token-signing authority while the chat engine receives public verification keys only.
- Static JWKS input is limited to 64 KiB, 1–32 unique public signing keys, Ed25519 or RSA at least 2048 bits, and verification-only key use.
- Tokens cannot redirect key loading: `jku`, `x5u`, `x5c`, critical, and unencoded-payload headers are rejected, and no remote JWKS retrieval occurs.
- Key-set errors omit file paths and contents. Rotation is operator-controlled and requires a restart; per-token revocation and automatic remote refresh remain out of scope.
- SQLite schema remains version 5 and the supported deployment remains one process/replica.

## 0.11.0 — 2026-08-02

### Added

- Multi-stage Python 3.14 slim container image running one Samsarix process as numeric UID/GID 10001.
- Hardened single-replica Compose profile with a read-only root filesystem, dropped capabilities, no-new-privileges, bounded PIDs/tmpfs, local rotating logs, loopback port publishing, and a persistent SQLite volume.
- `_FILE` alternatives for operator, token-signing, and current/previous webhook secrets, with strict single-line UTF-8 bounds and direct/file conflict rejection.
- Container build, health, non-root/read-only, authenticated API, restart-persistence, and cleanup smoke in CI.
- Container deployment, secret creation, reverse-proxy, backup, upgrade, rollback, and single-replica runbook.

### Changed

- Python distribution and service metadata advance to 0.11.0; the TypeScript client remains 0.3.0 because its public protocol surface is unchanged.
- Measured multi-instance work moves to v0.12: research confirmed that a broker alone would not solve authoritative storage, lifecycle leadership, migration/restore, and distributed quota semantics.

### Security and operations

- Compose secrets are mounted as files instead of ordinary secret environment values; secret contents are never included in configuration errors.
- First-party GitHub Actions are pinned to reviewed immutable commit SHAs.
- The default published port remains host-loopback only. TLS, host firewalling, Docker-daemon security, backups, and patching remain operator responsibilities.
- SQLite schema remains version 5. Exactly one container/process/replica is supported against a volume.

## 0.10.0 — 2026-08-02

### Added

- Authorized `GET /v1/rooms/{room_id}/messages/search` retrieval for the current retained content of one room.
- Unicode NFKC/casefold substring matching, chronological cursor pagination, and immediate edit/delete convergence.
- An independent `SAMSARIX_CHAT_SEARCHES_PER_MINUTE` abuse limit keyed by signed subject or operator/local client address.
- `searchMessages` in `@samsarix/chat-client` plus backend, authorization, pagination, Unicode, mutation, and SDK tests.

### Changed

- Python distribution and service metadata advance to 0.10.0; `@samsarix/chat-client` advances to 0.3.0.
- The roadmap moves measured multi-instance work to v0.11 so a named support retrieval need lands without premature scale claims.

### Security, privacy, and operating cost

- Search requires the existing room-scoped `room:read` authorization and moderation checks; there is no cross-room or global endpoint.
- Deleted bodies are excluded, edits replace prior searchable text immediately, and retention naturally removes content from results.
- Query work is bounded by the configured per-room retained-message cap. Search is normalized substring matching, not fuzzy/full-text ranking, and SQLite schema 5 is unchanged.

## 0.9.0 — 2026-08-02

### Added

- Transactional SQLite webhook outbox for selected committed message create/update/delete and member-moderation events.
- Standard Webhooks HMAC-SHA256 ID/timestamp/body signatures with a temporary previous-secret rotation window.
- Restart-safe at-least-once background delivery, bounded multi-day retries with jitter, `Retry-After` handling, and stable replay IDs.
- Admin-only delivery metadata/filter/pagination and manual replay endpoints that omit message payloads and receiver bodies.
- Receiver verification, retry recovery, rotation, SSRF/egress, privacy, backup, and rollback runbooks.
- SQLite schema v5 with an empty in-place outbox migration from earlier supported versions.

### Changed

- Python distribution and service metadata advance to 0.9.0; the TypeScript client remains 0.2.0 because its public protocol surface is unchanged.
- Optional webhook-enabled writes fail atomically with `507 webhook_capacity_reached` only when every row at the configured outbox cap is still pending.

### Security, privacy, and reliability

- Remote destinations require HTTPS, redirects and URL credentials/query secrets are rejected, platform TLS verification remains enabled, and non-public address resolution is denied unless a trusted operator explicitly opts in; the validated address is pinned for the actual connection while TLS verifies the original hostname.
- Every network attempt has a bounded timeout; secrets and receiver response bodies are never logged or exposed through the operations API.
- Delivery is honestly documented as at-least-once and potentially reordered; receivers verify the signed raw body/timestamp and durably deduplicate the stable ID.
- Message/room deletion and age/count retention cancel related pending payloads and scrub completed/terminal outbox bodies while retaining non-replayable operational metadata.

## 0.8.0 — 2026-08-01

### Added

- Signed-user, per-room read cursors with current unread counts and HTTP get/advance/self-clear operations.
- Ephemeral WebSocket typing commands and transition events with independent rate limits and automatic expiry.
- TypeScript client methods and strict event types for read state and typing.
- A complete two-party support-room example and application-workflow integration guide.
- SQLite schema v4 with bounded read-state storage, authenticated message-author metadata, and in-place migration.

### Changed

- Python distribution and service metadata advance to 0.8.0; `@samsarix/chat-client` advances to 0.2.0.
- The application CORS contract permits `PUT` for monotonic read-cursor updates.

### Security and privacy

- Read state is available only to a stable signed subject, excludes self-authored and deleted messages, cannot regress, and can be erased by its owner.
- Typing activity is transition-only, separately rate-limited, automatically cleared, and never persisted, exported, or audited.

## 0.7.0 — 2026-08-01

### Added

- Checked-in `@samsarix/chat-client` TypeScript package with explicit ESM exports and generated declarations.
- Typed wrappers for room, message, lifecycle, and moderation HTTP operations with stable `SamsarixApiError` handling.
- Browser-safe first-message WebSocket authentication, discriminated protocol events, connection-state observers, async credential refresh, and bounded exponential reconnect.
- Fake-transport unit tests, package-content verification, production-dependency audit, and a dedicated Node 24 CI job.

### Changed

- Python distribution and service metadata advance to 0.7.0; the server database remains schema 3 and the NDJSON export remains schema 2.

### Security

- The SDK never places tokens or API keys in WebSocket URLs and does not persist credentials, messages, or telemetry.
- API-key WebSocket use requires an explicit username and is documented for trusted processes rather than browser bundles.

## 0.6.0 — 2026-08-01

### Added

- Author/admin message editing and idempotent tombstone deletion with live `message.updated` and `message.deleted` events.
- Administrator room freeze/unfreeze for announcement-mode conversations.
- Stable-token-subject mute and ban controls, including immediate WebSocket eviction on active bans.
- Persisted message and room state that lets reconnecting clients recover edits, tombstones, and freeze state.
- Metadata-only audit events for room freeze, moderation, and message changes.
- Database schema v3 and in-place migration from v1/v2.
- Conversation-control runbook and integration coverage across HTTP, WebSocket, authorization, migration, and audit behavior.

### Changed

- Room responses add nullable `frozen_at`; message responses add nullable `edited_at` and `deleted_at`.
- NDJSON room export advances to schema 2 for the new nullable lifecycle fields.

### Security

- Mutation transactions re-check mute/ban state so a concurrent moderation change cannot race an already-authorized write.
- Deletes erase current message content while retaining only the ordering tombstone and metadata-only audit record.

## 0.5.0 — 2026-08-01

- Added room export, archive/reopen, confirmed deletion, age retention, bounded administrative audit, and SQLite backup/restore.
- Added schema v2 with safe migration and future-version refusal.

## 0.4.0 — 2026-08-01

- Added signed per-room access tokens, enforced sender identity, action authorization, and browser WebSocket authentication.

## 0.3.0 — 2026-07-28

- Migrated canonical product identity from Helix to Samsarix while retaining deprecated compatibility aliases.
