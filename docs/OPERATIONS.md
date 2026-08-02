# Data lifecycle operations

Samsarix Chat Engine 0.9 provides explicit export, archive, deletion, retention, audit, webhook recovery, backup, and restore controls. These controls help an operator apply a deployment-specific policy; they are not a claim of regulatory compliance.

All HTTP examples below require an operator API key or an access token with `admin`. User room tokens cannot call lifecycle or audit endpoints.

## Export a room

Exports are newline-delimited JSON so a client can process large histories without loading the entire room into memory:

```bash
curl --fail-with-body \
  -H "X-API-Key: $SAMSARIX_CHAT_API_KEY" \
  -o general-messages.ndjson \
  http://127.0.0.1:8000/v1/rooms/general/export
```

Line 1 has `type: samsarix.room_export`, `schema_version: 2`, an export timestamp, and room metadata. Each later line has `type: message` and one complete current message or tombstone. Export schema 2 adds room `frozen_at` and message `edited_at`/`deleted_at`; readers should reject unknown major schema values. The audit action is `room.export_requested`: it records that the export request was accepted, not that the client received every byte.

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

## Monitor and recover application webhooks

When an application webhook is configured, message and moderation transactions create an outbox row in the same SQLite commit. The in-process worker sends due rows after commit; receiver failure never converts an already committed chat response into an error. Monitor failed and pending metadata through `/v1/admin/webhook-deliveries`, and replay a known row through `/v1/admin/webhook-deliveries/{delivery_id}/retry` after correcting the receiver.

An outbox containing only pending rows at `SAMSARIX_CHAT_MAX_WEBHOOK_DELIVERIES` rejects the originating chat/moderation transaction with 507 instead of silently losing the promised event. Message/room deletion and age/count retention cancel related pending bodies and scrub completed/terminal payload copies; metadata remains visible with `replayable: false`. They cannot recall an accepted delivery, and a worker-claimed delivery may finish concurrently, so downstream erasure remains the receiver operator's responsibility. Treat sustained pending growth, repeated `last_error` codes, terminal failures, and capacity rejection as operational alerts. The engine does not send email or telemetry on failure; the deployment must scrape/poll this operator endpoint or inspect structured service logs.

See [Reliable application webhooks](WEBHOOKS.md) for endpoint validation, receiver signature verification, secret rotation, retry timing, privacy, SSRF/egress boundaries, and exact recovery commands.

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

Restoring replaces the live database state with the snapshot state. Messages, audit events, read state, and webhook acknowledgements created after the backup are lost. Restored pending or previously unacknowledged webhook rows may deliver again, so receivers must deduplicate the stable ID. The running service holds a cross-process database lifecycle lock, so the restore command fails while that service is active; stop it and retry. The adjacent `.lock` file may remain after shutdown—the operating-system lock, not file presence, signals use. This coordinates Samsarix processes using the same resolved database path, not unrelated tools that modify SQLite directly.

## Upgrade and rollback

Opening an older supported database with v0.9 migrates it to schema version 5. The migration preserves existing rooms/messages, lifecycle metadata, moderation controls, read state, and audit records, then creates an empty webhook outbox. Legacy and operator/local messages still have no authenticated author and therefore count as other-authored for signed-user unread state. The engine refuses a schema version newer than it understands.

Take a verified backup before upgrade. Earlier releases do not understand schema 5. For a conservative rollback, stop v0.9 and restore the pre-v0.9 backup before starting the older version. Removing webhook environment variables stops new outbox insertion and delivery but is not a database downgrade.

## Read-state lifecycle

Each explicit mark-read operation persists one row keyed by room and signed token subject, up to `SAMSARIX_CHAT_MAX_READ_STATES_PER_ROOM`. Reading the state alone creates no row. The subject can delete its own row through the API; deleting a room cascades all of its rows. Cursor timestamps remain useful when normal retention removes the referenced message.

Read-state rows are contained in normal SQLite backups. They are intentionally not included in the room's NDJSON message export or administrative audit event stream. If a deployment treats read activity as personal data, its subject-erasure workflow should call `DELETE /v1/rooms/{room_id}/read-state` for each mapped room before invalidating the account's token access.
