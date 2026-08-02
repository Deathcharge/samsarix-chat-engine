# Changelog

This project follows semantic versioning while it is in alpha: minor versions may add or revise public contracts, and those changes are called out here.

## 0.9.0 — 2026-08-01

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

- Remote destinations require HTTPS, redirects and URL credentials/query secrets are rejected, platform TLS verification remains enabled, and non-public address resolution is denied unless a trusted operator explicitly opts in.
- Every network attempt has a bounded timeout; secrets and receiver response bodies are never logged or exposed through the operations API.
- Delivery is honestly documented as at-least-once and potentially reordered; receivers verify the signed raw body/timestamp and durably deduplicate the stable ID.
- Message/room deletion cancels related pending payloads and scrubs completed/terminal outbox bodies while retaining non-replayable operational metadata.

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
