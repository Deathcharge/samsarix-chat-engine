# API reference

The canonical machine-readable contract is generated at `/openapi.json`; interactive documentation is at `/docs`. This document explains the stable v0.12 behavior that OpenAPI does not fully describe, especially authentication, search, read state, streaming export, durable webhooks, and WebSockets.

## Authentication

When both authentication settings are unset, `/v1` is unauthenticated and the CLI binds to loopback by default. `SAMSARIX_CHAT_API_KEY` is the all-room operator credential and can be sent as:

```text
X-API-Key: <secret>
Authorization: Bearer <secret>
```

When `SAMSARIX_CHAT_TOKEN_SIGNING_SECRET` or `SAMSARIX_CHAT_TOKEN_VERIFICATION_JWKS_FILE` is configured, application clients send a signed token as `Authorization: Bearer <token>`. Tokens bind a `sub` identity to room IDs and `room:read`, `room:write`, `room:pin`, `room:read-receipts`, or `admin`. Room creation/listing and `/v1/stats` require operator/admin access. Room lookup/history require read access; posting requires write access; shared pin changes require read plus pin access; participant receipt visibility requires read plus read-receipts access. Health and readiness remain unauthenticated. See [Identity and room authorization](AUTHORIZATION.md) for issuance, static-JWKS rotation, and the strict claim profile.

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

Admin-only. Streams `application/x-ndjson`: a `samsarix.room_export` metadata record with `schema_version: 8`, followed by one current `message` record or tombstone per line in chronological order. Schema 8 retains nullable `parent_message_id`, grouped `reactions`, nullable `pinned_at`/`pinned_by`, bounded message `metadata`, and ordered host-owned `attachments`, then adds ordered host-resolved `mentioned_subjects`. The response is an attachment and the operation records `room.export_requested`.

### `DELETE /v1/rooms/{room_id}`

Admin-only and irreversible. The room must first be archived, and `X-Confirm-Room-Delete` must exactly equal the room ID. Success returns 204 and transactionally deletes the room and messages while retaining a metadata-only deletion audit event.

### `POST /v1/rooms/{room_id}/messages`

Persists a message, broadcasts it to the room, and returns 201:

```json
{
  "sender": "Andrew",
  "content": "Hello",
  "client_message_id": "client-generated-id",
  "parent_message_id": null,
  "metadata": {"ticket.id":"SUP-42","priority":2},
  "attachments": [{"id":"upload:SUP-42:trace","name":"trace.json","media_type":"application/json","size_bytes":256}],
  "mentioned_subjects": ["agent-7","billing-oncall"]
}
```

`sender` is 1–64 characters for operator or local access. Token clients may omit it; the signed subject is persisted. A conflicting value returns `403 identity_mismatch`. `content` is subject to `SAMSARIX_CHAT_MAX_MESSAGE_CHARS` and must be nonblank unless at least one attachment is supplied. `client_message_id` is optional and at most 128 characters. `Idempotency-Key` can be used instead; if both are present, they must match. Replaying an ID returns the first persisted message with HTTP 200 and does not broadcast it again.

Set `parent_message_id` to a non-deleted top-level message in the same room to create a reply. Threads are exactly one level deep: replying to a reply returns `409 thread_depth_exceeded`; a deleted parent returns `409 parent_message_deleted`; an unknown or cross-room parent returns `404 parent_message_not_found`. Archived rooms reject new messages with `409 room_archived`; frozen rooms reject member writes with `409 room_frozen`; active controls return `403 room_muted` or `403 room_banned`.

Optional `metadata` is a flat JSON object for host-application display and integration context. It accepts at most 20 unique lowercase ASCII keys matching `^[a-z][a-z0-9_.-]{0,63}$`; values are strings, booleans, null, or finite numbers, with integers restricted to the JavaScript-safe range. Canonical UTF-8 JSON is capped at 4096 bytes. Arrays and nested objects are rejected. Treat every value as untrusted data: it is not authorization, server routing, HTML, or executable UI. Idempotent create replay returns the first persisted metadata and ignores a different retry body.

Optional `attachments` contains at most five ordered opaque descriptors totaling at most 8192 canonical UTF-8 JSON bytes. Each requires a unique portable `id` (1–128 characters), untrusted display `name` (1–255, no controls), lowercase `media_type`, and non-negative JavaScript-safe `size_bytes`; `sha256` is an optional lowercase 64-hex digest. Unknown fields and URLs are rejected. The engine does not upload, fetch, scan, authorize, or delete the file. The host resolves IDs and issues a fresh authorized download after checking current room access; never persist an expiring signed URL. See [Host-owned attachment references](ATTACHMENTS.md).

Optional `mentioned_subjects` contains at most ten ordered, unique, case-sensitive stable IDs of 1–64 characters without surrounding whitespace. Samsarix does not parse `@` display text, validate target membership, expand roles, or deliver notifications. The host resolves targets, re-checks membership/preferences, and owns provider delivery, deduplication, abuse and cost controls. Supplying the field on edit replaces the array; omitting it or sending null preserves the current value, and `[]` clears it. See [Host-resolved message mentions](MENTIONS.md).

### `PATCH /v1/rooms/{room_id}/messages/{message_id}`

Replaces a non-deleted message's content and sets `edited_at`. The signed author or an administrator may edit; other writers receive `403 message_not_owned`. Edited content is bounded by `SAMSARIX_CHAT_MAX_MESSAGE_CHARS`; oversized edits return `413 message_too_large`. Deleted messages return `409 message_deleted`. Success broadcasts `message.updated` after commit. Earlier content is overwritten and is not copied to the audit trail. Omit `metadata` (or send null) to preserve it, send `{}` to clear it, or send a complete replacement object using the create limits.

```json
{"content":"Corrected text","metadata":{"ticket.status":"resolved"}}
```

### `DELETE /v1/rooms/{room_id}/messages/{message_id}`

The signed author or an administrator may delete. Success returns 204, replaces content with an empty string, clears application metadata, attachment references, reaction actors/counts, and pin metadata, sets `deleted_at`, and broadcasts `message.deleted` once. Repeating the delete is idempotent. The tombstone stays in chronological history so clients do not reorder surrounding messages; deleted content, application context, attachment identifiers, reaction identity, and pin identity are not retained by this feature. Administrators may remove content while a room is frozen or archived.

### `PUT|DELETE /v1/rooms/{room_id}/messages/{message_id}/reactions/{reaction_key}`

Requires `room:write`. `PUT` idempotently adds and `DELETE` idempotently removes one actor's reaction. Keys must match `^[a-z0-9][a-z0-9_+\-]{0,29}$`; a message may have at most 20 distinct keys. Signed users send `{}` and the server uses the token subject. Operator/local callers send `{"reactor":"display-or-stable-id"}`; a token cannot impersonate another reactor.

The response contains the updated message, key, reactor, desired `present` state, whether storage `changed`, and `updated_at`. The reactor identity is therefore visible to authorized room clients and selected webhook receivers even though message history exposes only grouped counts. A real change broadcasts `message.reaction.updated` and can enqueue the same optional signed webhook; an idempotent replay does neither. Unknown, deleted, archived, frozen, muted, banned, capacity, and identity failures use the normal stable error contracts.

### `GET /v1/rooms/{room_id}/messages/pins?limit=50&before={message_id}`

Requires `room:read`. Returns pinned messages newest-pin-first in the normal `MessagePage` envelope, ordered by `(pinned_at, id)`. `next_before` is the last message ID in the current page. The cursor must still identify a pinned message in this room; concurrent unpinning can invalidate it, in which case clients reload the list. Deleted messages never appear because tombstoning clears their pin.

### `PUT|DELETE /v1/rooms/{room_id}/messages/{message_id}/pin`

Requires both `room:read` and the least-privilege room-scoped `room:pin` permission. `PUT` idempotently pins and `DELETE` idempotently unpins one message. Signed users send `{}` and the server binds the action to the token subject. Operator/local callers send `{"pinner":"stable-or-display-id"}` because their shared credential has no end-user identity; they cannot use that supplied value to alter the administrative audit actor.

The response contains the complete current `message`, action actor `pinner`, desired `pinned` state, storage `changed`, and `updated_at`. A real change records metadata-only `message.pin.updated` audit state, broadcasts the event, and may enqueue the same optional signed webhook; an idempotent replay does none of those. Frozen rooms limit changes to administrators, archived rooms reject changes, and muted/banned/deleted/unknown/identity failures use the normal stable errors.

### `GET /v1/rooms/{room_id}/messages?limit=50&before={message_id}`

Returns messages in chronological order:

```json
{
  "items": [],
  "next_before": null
}
```

Pages contain the newest matching messages. Message objects include nullable `parent_message_id`, sorted `reactions: [{"key":"ack","count":2}]`, nullable `pinned_at`/`pinned_by`, application `metadata`, ordered `attachments`, ordered `mentioned_subjects`, `edited_at`, and `deleted_at`; a deleted message has empty `content`, metadata, attachments and mentions, no reactions, and no pin. Top-level history intentionally includes replies in the same flat chronological stream for backward compatibility. When `next_before` is non-null, pass it as `before` to fetch the next older page. An unknown or cross-room cursor returns `400 invalid_cursor`.

### `GET /v1/rooms/{room_id}/messages/{parent_message_id}/replies?limit=50&before={message_id}`

Requires `room:read` and returns the normal chronological `MessagePage` shape containing only direct replies to one top-level message. The parent may be a tombstone so clients can still recover its surviving replies. Passing a reply as the parent returns `409 thread_depth_exceeded`; an unknown or cross-room parent returns `404 parent_message_not_found`. A cursor must identify a reply in this exact thread or the server returns `400 invalid_cursor`.

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

### `POST /v1/read-states/query`

Returns one content-free inbox snapshot for 1–100 unique room IDs:

```json
{"room_ids":["support-42","incident-9"]}
```

The response preserves caller order and includes current aggregate totals:

```json
{
  "subject": "agent-7",
  "items": [
    {
      "room_id": "support-42",
      "last_read_message_id": "message-4",
      "last_read_at": "2026-09-01T12:00:00Z",
      "unread_count": 2,
      "latest_message_id": "message-6",
      "latest_message_at": "2026-09-01T12:05:00Z"
    },
    {
      "room_id": "incident-9",
      "last_read_message_id": null,
      "last_read_at": null,
      "unread_count": 0,
      "latest_message_id": null,
      "latest_message_at": null
    }
  ],
  "total_unread_count": 2,
  "unread_room_count": 1
}
```

Only a signed subject may query. Its token must grant `room:read` for every requested ID before storage is consulted; an unauthorized room fails the whole request with `403 authorization_denied`. An authorized but missing room returns `404 room_not_found`, and any active room ban returns `403 room_banned`. Duplicate, malformed, empty, or oversized room sets return `422 invalid_request`. Reading does not create or update cursor rows.

`latest_message_id` and `latest_message_at` identify the newest non-deleted retained message, or are both null. Bodies, senders, metadata, attachments, mentions, reactions, and pin state are never copied into this response; fetch one selected room's authorized history for a preview. The host application supplies the candidate room IDs from its own membership/assignment database and may sort the returned items by unread state or latest activity. This endpoint does not discover membership or list rooms for a user.

SQLite evaluates the requested set in one database statement. PostgreSQL uses one statement and one snapshot across the set. Each signed subject receives `SAMSARIX_CHAT_READ_STATE_QUERIES_PER_MINUTE` requests per 60 seconds, independently keyed from message search; excess returns `429 read_state_query_rate_limit_exceeded` with `Retry-After: 60`. The 100-room bound, retained-message caps, and content-free result limit query and response amplification.

### `PUT /v1/rooms/{room_id}/read-state`

Advances the signed subject's cursor through one room message:

```json
{"message_id":"message-id"}
```

Send `{}` to advance through the room's latest current position. The operation is monotonic and idempotent: submitting an older message never regresses the cursor. An unknown or cross-room ID returns `404 message_not_found`; a new cursor beyond `SAMSARIX_CHAT_MAX_READ_STATES_PER_ROOM` returns `507 read_state_capacity_reached`. The response is the current read state and unread count.

### `DELETE /v1/rooms/{room_id}/read-state`

Deletes the signed caller's stored cursor and returns 204. Repeating the request is idempotent. It never affects another subject.

### `POST /v1/rooms/{room_id}/read-receipts/query`

Requires both `room:read` and `room:read-receipts`. The caller supplies 1–100 unique stable subjects; the engine preserves that order and returns null cursor fields for a supplied subject with no stored state:

```json
{"subjects":["customer-42","agent-7"]}
```

```json
{
  "room_id":"support-42",
  "items":[
    {"subject":"customer-42","last_read_message_id":"message-6","last_read_message_at":"2026-09-01T12:05:00Z","last_read_at":"2026-09-01T12:05:03Z"},
    {"subject":"agent-7","last_read_message_id":null,"last_read_message_at":null,"last_read_at":null}
  ]
}
```

The message timestamp and ID form the exact server ordering cursor; `last_read_at` is when that cursor advanced. The endpoint does not enumerate membership or validate supplied subjects against a host directory. Empty, duplicate, padded, malformed, or oversized subject sets return `422 invalid_request`. Snapshot queries and read-state updates have independent per-caller budgets derived from `SAMSARIX_CHAT_READ_STATE_QUERIES_PER_MINUTE`; excess returns `429 read_receipt_query_rate_limit_exceeded` or `read_receipt_update_rate_limit_exceeded`.

A real monotonic advance emits `read.updated` with the same receipt object; a real clear emits null cursor fields. Idempotent and regressive writes emit nothing. Only WebSocket principals with `room:read-receipts` (or `admin`) receive the event. Because delivery is at-most-once, query a fresh snapshot after every connect/reconnect and then apply live updates. See [Participant read receipts](READ_RECEIPTS.md) for privacy and retention boundaries.

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

Activation has one deadline equal to `SAMSARIX_CHAT_WS_SEND_TIMEOUT`, including time waiting for its per-socket operation lock. New arrivals do not reset it. Queue overflow or failed/timed-out activation attempts close **1013**; activation cancellation attempts **1012**. Clients reconnect with backoff and reload history; they must not treat an interrupted initialization as a complete stream. Events after admission may overlap the later history snapshot: replace the newest page from `history`, then upsert complete message objects from subsequent events by ID.

The `ready` event advertises `snapshot_sync_v1`. A capable client sends `{"type":"sync"}` only after consuming `history`. The server cannot read that command until activation has flushed every event then queued on this process; it responds with `sync.completed`, echoing the snapshot item count and older-page cursor. Receiving the marker therefore completes the **local snapshot handoff**. It does not add a durable client cursor, replay missed ephemeral events, provide exactly-once delivery, or prove that a different PostgreSQL process has polled through the global event head. A failed connection before the marker leaves the snapshot incomplete. One measured PostgreSQL profile combines lifecycle changes, a paused/lag-fenced replica and a bounded reconnect storm; arbitrary outage timing, device networks and sustained capacity remain separate gates.

After authentication and room validation, events begin with:

```json
{
  "type": "ready",
  "room": {"id":"general","name":"General","description":"","created_at":"...","archived_at":null,"frozen_at":null},
  "username": "Andrew",
  "active_connections": 1,
  "max_message_chars": 4000,
  "capabilities": ["snapshot_sync_v1"]
}
```

```json
{"type":"history","items":[],"next_before":null}
```

After consuming history, capable clients request and receive:

```json
{"type":"sync"}
{"type":"sync.completed","strategy":"snapshot","history_count":0,"next_before":null}
```

Live events are:

- `message.created`: contains `message` and `idempotent_replay`.
- `message.updated`: contains the committed current `message`.
- `message.deleted`: contains the committed message tombstone.
- `message.pin.updated`: contains the complete current `message`, action actor `pinner`, desired `pinned` state, `changed: true`, and `updated_at`. Replace the message by ID and refresh the pinned list if its ordering matters.
- `message.reaction.updated`: contains the complete current `message`, reaction `key`, `reactor`, desired `present` state, `changed: true`, and `updated_at`. Replace the message by ID; do not increment a cached count independently.
- `read.updated`: contains one participant receipt after a real cursor advance or clear and is sent only to sockets with `room:read-receipts` (or admin). Replace that subject's receipt; reload an explicit HTTP snapshot after reconnect.
- `presence.joined` / `presence.left`: contains `username` and the room connection count when the event was produced; best effort only. Queued or delayed events can predate the count in `ready`, so their counts are not fresh measurements at receipt and are never an authorization input.
- `typing.started`: contains `username` and `expires_in`; sent to other connections only when a user transitions to typing.
- `typing.stopped`: contains `username`; sent after an explicit stop, successful publish, disconnect, or server timeout.
- `pong`: response to an application-level ping command.
- `sync.completed`: response to a post-history `sync` command after the local initialization buffer has drained; `history_count` and `next_before` bind it to the preceding snapshot.
- `error`: contains a stable `code` and human-readable `message`.
- `room.archived`: final room metadata before the server closes the connection with 4409.
- `room.frozen` / `room.unfrozen`: current room metadata; connections remain open.
- `member.banned`: the subject and expiry before matching connections close with 4403.

### Client commands

```json
{"type":"message","content":"Hello","client_message_id":"optional-id","metadata":{"ticket.id":"SUP-42"},"attachments":[],"mentioned_subjects":["agent-7"]}
```

```json
{"type":"message","content":"Follow-up","parent_message_id":"top-level-message-id","client_message_id":"optional-id"}
```

```json
{"type":"ping"}
```

```json
{"type":"sync"}
```

```json
{"type":"typing","active":true}
```

Every WebSocket publish and typing command checks `room:write` and the current room/member state. Read-only, muted, and frozen-member sessions receive a structured error and remain connected. Typing commands have a separate `SAMSARIX_CHAT_TYPING_EVENTS_PER_MINUTE` allowance and return `typing_rate_limit_exceeded` without consuming the message allowance. Active typing expires after `SAMSARIX_CHAT_TYPING_TIMEOUT`. A banned session closes with 4403. After three invalid commands the server closes with 1008. Binary frames close after repeated rejection with 1003. Oversized frames close with 1009. Capacity rejection uses 1013; a missing room uses 4404.

## Delivery semantics

Message create/update/delete events are emitted only after the configured storage transaction commits. Replies use the same events and carry `message.parent_message_id`; no separate thread event stream exists. In supported v0.12 SQLite mode, broadcast, presence, and typing are in-process and at-most-once. In the guarded v0.13 PostgreSQL preview, application events commit to an ordered database log and are relayed across instances; connection leases and expiring typing state also live in PostgreSQL. Delivery to each WebSocket remains at-most-once, and slow or failed clients are removed after the configured send timeout. Clients must honor `expires_in` even if a typing stop event is missed. Reconnecting clients recover current edits and tombstones from the newest history snapshot and current unread state over HTTP rather than relying on missed events. They should reconnect with backoff, complete the advertised snapshot-sync handshake, and use the HTTP message cursor endpoint for older messages or the replies endpoint for a selected thread.

When configured, selected application webhook rows commit atomically with message/moderation state and deliver later with at-least-once semantics. Retries and manual replay keep the same `webhook-id`; each attempt gets a new signed timestamp. Delivery can be duplicated or reordered, so receivers validate the Standard Webhooks signature/timestamp and durably deduplicate IDs before side effects. See [Reliable application webhooks](WEBHOOKS.md) for the exact envelope, verification procedure, retry schedule, rotation, network policy, and recovery runbook.

Multi-worker or multi-host fan-out is not implemented in supported v0.12 SQLite mode; lifecycle, webhook worker, and ban teardown are deterministic only within that single process. The guarded [PostgreSQL multi-instance preview](POSTGRES_PREVIEW.md) is application-wired but unreleased until its remaining process-failure and measured-load gates pass. The checked-in [TypeScript client](../clients/typescript/README.md) implements the reconnect recovery sequence for browser and Node integrations. The current [container profile](CONTAINER_DEPLOYMENT.md) packages exactly the SQLite one-process topology and must not be scaled to multiple replicas.
