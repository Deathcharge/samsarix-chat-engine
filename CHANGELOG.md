# Changelog

This project follows semantic versioning while it is in alpha: minor versions may add or revise public contracts, and those changes are called out here.

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
