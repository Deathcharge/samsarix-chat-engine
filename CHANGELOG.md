# Changelog

This project follows semantic versioning while it is in alpha: minor versions may add or revise public contracts, and those changes are called out here.

## Unreleased

### Added

- Accepted PostgreSQL multi-instance architecture and explicit cross-process correctness, recovery, capacity, moderation, webhook, migration, and failure-test gates for v0.13.
- Storage-neutral `ChatStorage` protocol between application/webhook orchestration and the existing SQLite backend.
- Optional `postgres` installation extra and an internal PostgreSQL foundation with advisory-lock schema initialization, a transaction-coupled ordered realtime event log, and durable per-instance cursors.
- Internal PostgreSQL schema v2 and core store for rooms, bounded messages, Unicode-normalized search, moderation, audit history, database-time ordering, and cross-instance idempotency/capacity enforcement.

### Security and operations

- Multi-process SQLite is explicitly rejected: WAL is same-host-only, single-writer, and current SQLite documentation identifies a concurrent WAL-reset corruption race affecting versions through 3.51.2.
- Version 0.12 remains a one-process/one-replica release until the PostgreSQL acceptance gates pass; no horizontal-scale claim is introduced by this architecture increment.
- PostgreSQL credentials remain internal to the optional foundation and are excluded from translated availability errors; the incomplete backend is not selectable through public configuration.
- Message deletion, room deletion, and automatic retention scrub message bodies from retained realtime event envelopes in the same transaction; the event envelope remains bounded while supporting the existing 100,000-character message contract.

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
