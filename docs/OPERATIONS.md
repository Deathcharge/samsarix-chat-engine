# Data lifecycle operations

Samsarix Chat Engine 0.5 provides explicit export, archive, deletion, retention, audit, backup, and restore controls. These controls help an operator apply a deployment-specific policy; they are not a claim of regulatory compliance.

All HTTP examples below require an operator API key or an access token with `admin`. User room tokens cannot call lifecycle or audit endpoints.

## Export a room

Exports are newline-delimited JSON so a client can process large histories without loading the entire room into memory:

```bash
curl --fail-with-body \
  -H "X-API-Key: $SAMSARIX_CHAT_API_KEY" \
  -o general-messages.ndjson \
  http://127.0.0.1:8000/v1/rooms/general/export
```

Line 1 has `type: samsarix.room_export`, `schema_version: 1`, an export timestamp, and room metadata. Each later line has `type: message` and one complete message. The audit action is `room.export_requested`: it records that the export request was accepted, not that the client received every byte.

Exports contain plaintext message bodies and sender identifiers. Protect them like the database, transmit them over TLS, and delete working copies according to your policy.

## Archive, reopen, and delete

Archive makes a room read-only and closes its active WebSockets with a final `room.archived` event and close code 4409:

```bash
curl --fail-with-body -X PATCH \
  -H "X-API-Key: $SAMSARIX_CHAT_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"archived":true}' \
  http://127.0.0.1:8000/v1/rooms/general
```

History and export remain readable while archived. Reopen with the same request and `{"archived":false}`.

Deletion is irreversible. It is accepted only when the room is archived and the confirmation header exactly repeats its ID:

```bash
curl --fail-with-body -X DELETE \
  -H "X-API-Key: $SAMSARIX_CHAT_API_KEY" \
  -H "X-Confirm-Room-Delete: general" \
  http://127.0.0.1:8000/v1/rooms/general
```

A successful deletion returns 204 and removes the room plus its messages in one transaction. The metadata-only `room.deleted` audit event survives and includes the deleted message count. Backups and prior exports are separate copies and are not erased by this operation.

## Configure and run retention

Set `SAMSARIX_CHAT_MESSAGE_RETENTION_DAYS` to an integer from 1 through 3650. New message commits trim messages older than the configured age. An operator can also run a pass explicitly:

```bash
curl --fail-with-body -X POST \
  -H "X-API-Key: $SAMSARIX_CHAT_API_KEY" \
  http://127.0.0.1:8000/v1/admin/retention/run
```

The response reports the UTC cutoff and deleted row count. Retention is permanent for the live database but does not rewrite backups. Count caps still apply independently.

## Review the administrative audit trail

```bash
curl --fail-with-body \
  -H "X-API-Key: $SAMSARIX_CHAT_API_KEY" \
  'http://127.0.0.1:8000/v1/admin/audit-events?limit=50'
```

The log covers room creation, archive/reopen, export requests, deletion, explicit retention, and automatic age/count-cap trimming. It stores actor/room/time and small operational details, never message bodies or credentials. Automatic policy actions use `system:retention`; a shared API key can identify only `operator-api-key`, not the individual human using it. Use signed admin tokens with distinct subjects when individual attribution matters.

The trail is bounded by `SAMSARIX_CHAT_MAX_AUDIT_EVENTS` (default 100000) to prevent unbounded disk use. Export it to an appropriately protected external audit system if your policy requires longer or tamper-resistant retention. A database administrator can still modify local SQLite data.

## Back up and restore

The backup command uses SQLite's online backup API, validates the snapshot with `PRAGMA integrity_check`, and atomically places the finished file. It can safely snapshot a running service:

```bash
samsarix-chat database backup backups/chat-2026-08-01.db
```

Use `--database path/to/chat.db` to override `SAMSARIX_CHAT_DATABASE`. Existing backup files are protected unless `--replace` is explicit.

Test restoration regularly into a separate path:

```bash
samsarix-chat database --database restore-test.db restore backups/chat-2026-08-01.db
SAMSARIX_CHAT_DATABASE=restore-test.db samsarix-chat serve --port 8001
```

Check `/readyz`, inspect representative rooms/history, then stop the test service. For an actual restore:

1. Stop the chat process and retain the current database as a rollback copy.
2. Run `samsarix-chat database restore BACKUP --replace` against the configured database.
3. Start one process and verify `/readyz`, room history, and the audit trail.
4. Keep the rollback copy until application-level verification is complete.

Restoring replaces the live database state with the snapshot state. Messages and audit events created after the backup are lost. The running service holds a cross-process database lifecycle lock, so the restore command fails while that service is active; stop it and retry. The adjacent `.lock` file may remain after shutdown—the operating-system lock, not file presence, signals use. This coordinates Samsarix processes using the same resolved database path, not unrelated tools that modify SQLite directly.

## Upgrade and rollback

Opening a v0.4 schema (version 1) with v0.5 adds nullable `rooms.archived_at`, creates `audit_events`, and sets schema version 2. Existing rooms and messages are preserved. The engine refuses a schema version newer than it understands.

Take a verified backup before upgrade. Because v0.4 does not understand schema version 2 as a supported contract, rollback means stopping v0.5 and restoring the pre-upgrade backup before starting v0.4.
