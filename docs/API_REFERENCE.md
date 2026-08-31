# API reference

The canonical machine-readable contract is generated at `/openapi.json`; interactive documentation is at `/docs`. This document explains the stable v0.12 behavior that OpenAPI does not fully describe, especially authentication, search, read state, streaming export, durable webhooks, and WebSockets.

## Authentication

When both authentication settings are unset, `/v1` is unauthenticated and the CLI binds to loopback by default. `SAMSARIX_CHAT_API_KEY` is the all-room operator credential and can be sent as:

```text
X-API-Key: <secret>
Authorization: Bearer <secret>
```

When `SAMSARIX_CHAT_TOKEN_SIGNING_SECRET` or `SAMSARIX_CHAT_TOKEN_VERIFICATION_JWKS_FILE` is configured, application clients send a signed token as `Authorization: Bearer <token>`. Tokens bind a `sub` identity to room IDs and `room:read`, `room:write`, or `admin`. Room creation/listing and `/v1/stats` require operator/admin access. Room lookup/history require read access; posting requires write access. Health and readiness remain unauthenticated. See [Identity and room authorization](AUTHORIZATION.md) for issuance, static-JWKS rotation, and the strict claim profile.

## HTTP endpoints

### `GET /healthz`

Returns `200 {"status":"ok"}` when the process can serve requests.

### `GET /readyz`

Returns `200 {"status":"ready"}` when the configured storage backend answers a query, or `503 {"status":"not_ready"}`. SQLite is the supported v0.12 backend; the guarded v0.13 PostgreSQL preview applies the same public response contract.

### `POST /v1/rooms`

Creates a room and returns 201 with a `Location` header. An omitted `id` becomes a 32-character generated ID.

```json
{
  "id": "general",
  "name": "General",
  "description": "Team discussion"
}
```

Caller-selected IDs must match `^[a-z0-9][a-z0-9_-]{0,63}$`. Names are 1–80 characters; descriptions are at most 500. Duplicate IDs return `409 room_already_exists`; a full store returns `507 room_capacity_reached`.

### `GET /v1/rooms?limit=100`

Lists up to 100 rooms in creation order.

### `GET /v1/rooms/{room_id}`

Returns a room, including nullable `archived_at` and `frozen_at`, or `404 room_not_found`. An actively banned token subject receives `403 room_banned`.

### `PATCH /v1/rooms/{room_id}`

Admin-only. Send `{"archived":true}` to make a room read-only and close active clients, or `{"archived":false}` to reopen it. Send `{"frozen":true}` to keep read sessions connected while restricting message creation and edits to administrators; `false` resumes member writes. Either or both fields may be supplied. Repeating the current state is idempotent and does not add a duplicate audit event.

### `GET /v1/rooms/{room_id}/export`

Admin-only. Streams `application/x-ndjson`: a `samsarix.room_export` metadata record with `schema_version: 2`, followed by one current `message` record or tombstone per line in chronological order. Schema 2 adds room freeze and message edit/delete timestamps. The response is an attachment and the operation records `room.export_requested`.

### `DELETE /v1/rooms/{room_id}`

Admin-only and irreversible. The room must first be archived, and `X-Confirm-Room-Delete` must exactly equal the room ID. Success returns 204 and transactionally deletes the room and messages while retaining a metadata-only deletion audit event.

### `POST /v1/rooms/{room_id}/messages`

Persists a message, broadcasts it to the room, and returns 201:

```json
{
  "sender": "Andrew",
  "content": "Hello",
  "client_message_id": "client-generated-id"
}
```

`sender` is 1–64 characters for operator or local access. Token clients may omit it; the signed subject is persisted. A conflicting value returns `403 identity_mismatch`. `content` is nonblank and subject to `SAMSARIX_CHAT_MAX_MESSAGE_CHARS`. `client_message_id` is optional and at most 128 characters. `Idempotency-Key` can be used instead; if both are present, they must match. Replaying an ID returns the first persisted message with HTTP 200 and does not broadcast it again. Archived rooms reject new messages with `409 room_archived`; frozen rooms reject member writes with `409 room_frozen`; active controls return `403 room_muted` or `403 room_banned`.

### `PATCH /v1/rooms/{room_id}/messages/{message_id}`

Replaces a non-deleted message's content and sets `edited_at`. The signed author or an administrator may edit; other writers receive `403 message_not_owned`. Edited content is bounded by `SAMSARIX_CHAT_MAX_MESSAGE_CHARS`; oversized edits return `413 message_too_large`. Deleted messages return `409 message_deleted`. Success broadcasts `message.updated` after commit. Earlier content is overwritten and is not copied to the audit trail.

```json
{"content":"Corrected text"}
```

### `DELETE /v1/rooms/{room_id}/messages/{message_id}`

The signed author or an administrator may delete. Success returns 204, replaces content with an empty string, sets `deleted_at`, and broadcasts `message.deleted` once. Repeating the delete is idempotent. The tombstone stays in chronological history so clients do not reorder surrounding messages; deleted content is not retained by this feature. Administrators may remove content while a room is frozen or archived.

### `GET /v1/rooms/{room_id}/messages?limit=50&before={message_id}`

Returns messages in chronological order:

```json
{
  "items": [],
  "next_before": null
}
```

Pages contain the newest matching messages. Message objects include nullable `edited_at` and `deleted_at`; a deleted message has empty `content`. When `next_before` is non-null, pass it as `before` to fetch the next older page. An unknown or cross-room cursor returns `400 invalid_cursor`.

### `GET /v1/rooms/{room_id}/messages/search?q={query}&limit=50&before={message_id}`

Requires `room:read` for the target room and returns the same chronological `MessagePage` shape as history. `q` must contain 2–100 characters after trimming and Unicode NFKC/casefold normalization. Matching is a case-insensitive normalized substring of current message `content`; sender names are not searched. Deleted messages are excluded, edits take effect immediately, and age/count retention removes results naturally.

Pages contain the newest matches in chronological order. The cursor may be any current or tombstoned message in the same room and preserves the normal `(created_at, id)` boundary; an unknown or cross-room cursor returns `400 invalid_cursor`. There is no global or cross-room search, fuzzy matching, relevance rank, highlight markup, or historical-version index.

Each instance allows `SAMSARIX_CHAT_SEARCHES_PER_MINUTE` searches per signed subject, falling back to the operator/local client address, independently of message and typing limits. Excess returns `429 search_rate_limit_exceeded` with `Retry-After: 60`. Work is bounded by `SAMSARIX_CHAT_MAX_STORED_MESSAGES_PER_ROOM`; operators should lower that cap or search allowance when room histories or concurrent query volume make linear scans unsuitable.

Because `q` is a GET query parameter, it may appear in ordinary reverse-proxy access logs. Do not search with credentials or secrets, and configure the deployment's log collection and retention for the same sensitivity as room content.

### `GET /v1/rooms/{room_id}/read-state`

Signed application users with `room:read` receive their current room cursor and a count derived from committed messages:

```json
{
  "room_id": "general",
  "subject": "user-123",
  "last_read_message_id": null,
  "last_read_at": null,
  "unread_count": 2
}
```

No row is created by a read. With no stored cursor, all non-deleted messages from other authenticated subjects count as unread. The exclusion uses signed-token author metadata rather than the public display sender; operator/local messages and legacy rows therefore count as other-authored. Shared operator-key and unauthenticated local callers receive `403 stable_subject_required` because they do not identify one durable user.

### `PUT /v1/rooms/{room_id}/read-state`

Advances the signed subject's cursor through one room message:

```json
{"message_id":"message-id"}
```

Send `{}` to advance through the room's latest current position. The operation is monotonic and idempotent: submitting an older message never regresses the cursor. An unknown or cross-room ID returns `404 message_not_found`; a new cursor beyond `SAMSARIX_CHAT_MAX_READ_STATES_PER_ROOM` returns `507 read_state_capacity_reached`. The response is the current read state and unread count.

### `DELETE /v1/rooms/{room_id}/read-state`

Deletes the signed caller's stored cursor and returns 204. Repeating the request is idempotent. It never affects another subject.

### `PATCH /v1/rooms/{room_id}/members/{subject}/moderation`

Admin-only. Applies relative durations of up to one year to a stable signed-token subject. Omit one field to preserve it; use zero to clear it.

```json
{"muted_for_seconds":900,"banned_for_seconds":3600}
```

A mute preserves reads and blocks writes. A ban blocks HTTP room reads/writes, rejects new WebSocket sessions, and sends `member.banned` before closing matching live sockets with 4403. Operators and admin tokens bypass member controls. Expired timestamps are ignored. Each change records metadata (subject and expiry), never message bodies or credentials.

### `GET /v1/stats`

Returns the current process's active WebSocket connection count in SQLite mode. In the guarded PostgreSQL preview, the count is deployment-wide across active instance leases.

### `GET /v1/admin/audit-events?limit=50&before={event_id}`

Admin-only. Returns chronological pages of room lifecycle, export-request, moderation, message-change, explicit-retention, and automatic-retention metadata. Events contain no message bodies or credentials. The shared API-key actor is `operator-api-key`, automatic policy actions use `system:retention`, and signed admin tokens use their subject.

### `GET /v1/admin/webhook-deliveries?status={pending|delivered|failed}&limit=50&before={delivery_id}`

Admin-only. Returns newest-first delivery metadata: stable ID, selected event type, room, attempt count/timestamps, next attempt, terminal/delivered time, last HTTP status, sanitized error code, and whether its body remains `replayable`. The optional status filter applies to each page; concurrently changing delivery status can move a row between filtered views. Payloads, destination URLs, secrets, and receiver response bodies are never returned. An unknown cursor returns `400 invalid_cursor`.

### `POST /v1/admin/webhook-deliveries/{delivery_id}/retry`

Admin-only. Resets a known delivery to pending, preserves its stable `webhook-id`, wakes the configured worker, and returns the reset metadata with HTTP 202. It returns `409 webhook_not_configured` when no destination is active, `409 webhook_payload_unavailable` when message/room deletion scrubbed the body, and `404 webhook_delivery_not_found` for an unknown ID. Receivers must treat replay as a duplicate-safe operation.

### `POST /v1/admin/retention/run`

Admin-only. Deletes messages older than `SAMSARIX_CHAT_MESSAGE_RETENTION_DAYS`, returns the UTC cutoff and row count, and adds `retention.executed`. Returns `409 retention_not_configured` when maximum age is unset.

## Error envelope

HTTP errors use a stable envelope:

```json
{
  "error": {
    "code": "room_not_found",
    "message": "Room not found"
  }
}
```

Validation failures use `invalid_request` and include field locations, messages, and types without echoing submitted values. A query outside the normalized search bounds returns `invalid_search_query`. Rate limits return 429 with `Retry-After: 60`. Unexpected SQLite failures return `503 storage_unavailable` without internal details. If the configured webhook outbox contains only pending rows at its hard cap, the originating message/moderation change returns `507 webhook_capacity_reached` and its transaction is rolled back.

HTTP request bodies are byte-bounded in addition to field validation. The derived limit accommodates the configured maximum message even when Unicode is JSON-escaped; oversized bodies return `413 request_too_large` before their contents are retained for validation.

## WebSocket endpoint

```text
/v1/rooms/{room_id}/ws
```

Token clients derive their username from `sub`. Local and operator clients add `?username={display_name}`, 1–64 characters. Only JSON text frames are supported. The CLI configures its WebSocket implementation to reject frames larger than `SAMSARIX_CHAT_WS_MAX_BYTES`; the application also checks accepted text commands.

### Authentication sequence

Non-browser clients may send an API key or bearer token in the upgrade request. Otherwise, an authenticated server accepts the transport but exposes no room data and sends:

```json
{"type":"auth.required","message":"Send an auth command before any chat commands","example":{"type":"auth","token":"..."}}
```

The client must reply before the configured deadline:

```json
{"type":"auth","token":"..."}
```

The legacy `{"type":"auth","api_key":"..."}` command is also accepted. Failure closes with 4401. Room or identity escalation closes with 4403. Credentials are not accepted in query parameters.

After successful authentication, a storage failure during room validation, admission, history initialization, or the receive loop sends `{"type":"error","code":"storage_unavailable","message":"Chat storage is temporarily unavailable"}` and attempts close code 1012. The error can arrive instead of `ready`/`history`, or after initialization; clients must not assume those initial frames are guaranteed. Cancelled sessions also attempt 1012; unexpected server failures attempt 1011. A broken transport may prevent any final frame reaching the client.

The request retains ownership until bounded cleanup has stopped its heartbeat/typing tasks, attempted its owned local close, and attempted any owned database reservation release. Cancellation does not turn that cleanup into an untracked background task. An individual closer retains ownership through its bounded physical close even when the calling task is cancelled; concurrent closes do not take that ownership away. A failed handshake does not announce a SQLite departure for a client that never joined. For PostgreSQL, reservation-derived presence can briefly show a join followed by its compensating leave; presence remains best effort, not proof that the ready/history handshake completed.

In the PostgreSQL preview, a room archived or deleted between validation and admission yields `room_archived`/4409 or `room_not_found`/4404. An established connection's heartbeat may observe that lifecycle change before the polling relay: archive then sends `room.archived` with the observed room snapshot and closes 4409; deletion sends `room_not_found` and closes 4404. Only the winning closer sends its final lifecycle notification. If room state cannot be verified, or an otherwise active room has an expired/missing lease, the server retains the conservative `storage_unavailable`/1012 outcome. Concurrent failures do not guarantee a particular final frame; clients reauthorize and reload current state on reconnect.

### Server events

In the PostgreSQL preview, each socket starts after the committed sequence of its own admission/join transaction. The relay ignores events at or before that boundary for that socket, including message/room/presence/typing broadcasts and archive/ban teardown. An old archive or ban can still close older sockets, but cannot close a new connection admitted after reopen/unban; the same boundary protects a recreated room ID. The sequence is internal metadata, not a client-supplied cursor or a new public envelope field. No comparison of wall-clock timestamps is involved. Embedded manager callers can supply `after_sequence` at registration and `event_sequence` on broadcast/room/member close (nonnegative integers); omitted sequences preserve local/SQLite behavior and do not prevent unconditional shutdown fences.

From successful registration until activation, room broadcasts are queued rather than discarded. On a successful handshake the server sends `ready`, then `history`, then queued events in their local broadcast-arrival order before admitting ordinary live broadcasts. Events arriving while the queue drains join its tail. Archive/ban, storage failure, cancellation, and transport failure can still interrupt initialization; initial frames are not guaranteed on those paths.

The application uses `ConnectionManager` defaults of **64 events and 262144 serialized UTF-8 JSON bytes per pending connection**, with an **8388608-byte aggregate budget**. Payloads in an activation send remain charged until that send finishes, even if the socket has detached. These are retained-payload bounds, not a claim about total process RSS or sustainable traffic capacity. Embedded callers can set `max_pending_events`, `max_pending_bytes`, and `max_total_pending_bytes` to positive integers. Normal active delivery does not use this queue.

Activation has one deadline equal to `SAMSARIX_CHAT_WS_SEND_TIMEOUT`, including time waiting for its per-socket operation lock. New arrivals do not reset it. Queue overflow or failed/timed-out activation attempts close **1013**; activation cancellation attempts **1012**. Clients reconnect with backoff and reload history; they must not treat an interrupted initialization as a complete stream. Buffering adds no durable client cursor, end-of-catch-up marker, or exactly-once guarantee. Events after admission may still overlap the later history snapshot: merge messages by ID, apply edits/tombstones, and reload current state after reconnect. One measured PostgreSQL profile combines lifecycle changes, a paused/lag-fenced replica and a bounded reconnect storm; arbitrary outage timing, device networks and sustained capacity remain separate gates.

After authentication and room validation, events begin with:

```json
{
  "type": "ready",
  "room": {"id":"general","name":"General","description":"","created_at":"...","archived_at":null,"frozen_at":null},
  "username": "Andrew",
  "active_connections": 1,
  "max_message_chars": 4000
}
```

```json
{"type":"history","items":[],"next_before":null}
```

Live events are:

- `message.created`: contains `message` and `idempotent_replay`.
- `message.updated`: contains the committed current `message`.
- `message.deleted`: contains the committed message tombstone.
- `presence.joined` / `presence.left`: contains `username` and the room connection count when the event was produced; best effort only. Queued or delayed events can predate the count in `ready`, so their counts are not fresh measurements at receipt and are never an authorization input.
- `typing.started`: contains `username` and `expires_in`; sent to other connections only when a user transitions to typing.
- `typing.stopped`: contains `username`; sent after an explicit stop, successful publish, disconnect, or server timeout.
- `pong`: response to an application-level ping command.
- `error`: contains a stable `code` and human-readable `message`.
- `room.archived`: final room metadata before the server closes the connection with 4409.
- `room.frozen` / `room.unfrozen`: current room metadata; connections remain open.
- `member.banned`: the subject and expiry before matching connections close with 4403.

### Client commands

```json
{"type":"message","content":"Hello","client_message_id":"optional-id"}
```

```json
{"type":"ping"}
```

```json
{"type":"typing","active":true}
```

Every WebSocket publish and typing command checks `room:write` and the current room/member state. Read-only, muted, and frozen-member sessions receive a structured error and remain connected. Typing commands have a separate `SAMSARIX_CHAT_TYPING_EVENTS_PER_MINUTE` allowance and return `typing_rate_limit_exceeded` without consuming the message allowance. Active typing expires after `SAMSARIX_CHAT_TYPING_TIMEOUT`. A banned session closes with 4403. After three invalid commands the server closes with 1008. Binary frames close after repeated rejection with 1003. Oversized frames close with 1009. Capacity rejection uses 1013; a missing room uses 4404.

## Delivery semantics

Message create/update/delete events are emitted only after the configured storage transaction commits. In supported v0.12 SQLite mode, broadcast, presence, and typing are in-process and at-most-once. In the guarded v0.13 PostgreSQL preview, application events commit to an ordered database log and are relayed across instances; connection leases and expiring typing state also live in PostgreSQL. Delivery to each WebSocket remains at-most-once, and slow or failed clients are removed after the configured send timeout. Clients must honor `expires_in` even if a typing stop event is missed. Reconnecting clients recover current edits and tombstones from history and current unread state over HTTP rather than relying on missed events. They should reconnect with backoff, consume the initial history event, and use the HTTP message cursor endpoint for older messages.

When configured, selected application webhook rows commit atomically with message/moderation state and deliver later with at-least-once semantics. Retries and manual replay keep the same `webhook-id`; each attempt gets a new signed timestamp. Delivery can be duplicated or reordered, so receivers validate the Standard Webhooks signature/timestamp and durably deduplicate IDs before side effects. See [Reliable application webhooks](WEBHOOKS.md) for the exact envelope, verification procedure, retry schedule, rotation, network policy, and recovery runbook.

Multi-worker or multi-host fan-out is not implemented in supported v0.12 SQLite mode; lifecycle, webhook worker, and ban teardown are deterministic only within that single process. The guarded [PostgreSQL multi-instance preview](POSTGRES_PREVIEW.md) is application-wired but unreleased until its remaining process-failure and measured-load gates pass. The checked-in [TypeScript client](../clients/typescript/README.md) implements the reconnect recovery sequence for browser and Node integrations. The current [container profile](CONTAINER_DEPLOYMENT.md) packages exactly the SQLite one-process topology and must not be scaled to multiple replicas.
