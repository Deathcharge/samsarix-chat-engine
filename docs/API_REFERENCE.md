# API reference

The canonical machine-readable contract is generated at `/openapi.json`; interactive documentation is at `/docs`. This document explains the stable v0.5 behavior that OpenAPI does not fully describe, especially streaming export and WebSockets.

## Authentication

When both authentication settings are unset, `/v1` is unauthenticated and the CLI binds to loopback by default. `SAMSARIX_CHAT_API_KEY` is the all-room operator credential and can be sent as:

```text
X-API-Key: <secret>
Authorization: Bearer <secret>
```

When `SAMSARIX_CHAT_TOKEN_SIGNING_SECRET` is configured, application clients send a signed token as `Authorization: Bearer <token>`. Tokens bind a `sub` identity to room IDs and `room:read`, `room:write`, or `admin`. Room creation/listing and `/v1/stats` require operator/admin access. Room lookup/history require read access; posting requires write access. Health and readiness remain unauthenticated. See [Identity and room authorization](AUTHORIZATION.md) for issuance and the strict claim profile.

## HTTP endpoints

### `GET /healthz`

Returns `200 {"status":"ok"}` when the process can serve requests.

### `GET /readyz`

Returns `200 {"status":"ready"}` when SQLite answers a query, or `503 {"status":"not_ready"}`.

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

Returns a room, including nullable `archived_at`, or `404 room_not_found`.

### `PATCH /v1/rooms/{room_id}`

Admin-only. Send `{"archived":true}` to make a room read-only and close active clients, or `{"archived":false}` to reopen it. Repeating the current state is idempotent and does not add a duplicate audit event.

### `GET /v1/rooms/{room_id}/export`

Admin-only. Streams `application/x-ndjson`: a `samsarix.room_export` metadata record with `schema_version: 1`, followed by one `message` record per line in chronological order. The response is an attachment and the operation records `room.export_requested`.

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

`sender` is 1–64 characters for operator or local access. Token clients may omit it; the signed subject is persisted. A conflicting value returns `403 identity_mismatch`. `content` is nonblank and subject to `SAMSARIX_CHAT_MAX_MESSAGE_CHARS`. `client_message_id` is optional and at most 128 characters. `Idempotency-Key` can be used instead; if both are present, they must match. Replaying an ID returns the first persisted message with HTTP 200 and does not broadcast it again. Archived rooms reject new messages with `409 room_archived`; their history remains readable until deletion.

### `GET /v1/rooms/{room_id}/messages?limit=50&before={message_id}`

Returns messages in chronological order:

```json
{
  "items": [],
  "next_before": null
}
```

Pages contain the newest matching messages. When `next_before` is non-null, pass it as `before` to fetch the next older page. An unknown or cross-room cursor returns `400 invalid_cursor`.

### `GET /v1/stats`

Returns the current process's active WebSocket connection count.

### `GET /v1/admin/audit-events?limit=50&before={event_id}`

Admin-only. Returns chronological pages of room lifecycle, export-request, explicit-retention, and automatic-retention metadata. Events contain no message bodies or credentials. The shared API-key actor is `operator-api-key`, automatic policy actions use `system:retention`, and signed admin tokens use their subject.

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

Validation failures use `invalid_request` and include field locations, messages, and types without echoing submitted values. Rate limits return 429 with `Retry-After: 60`. Unexpected SQLite failures return `503 storage_unavailable` without internal details.

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

### Server events

After authentication and room validation, events begin with:

```json
{
  "type": "ready",
  "room": {"id":"general","name":"General","description":"","created_at":"...","archived_at":null},
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
- `presence.joined` / `presence.left`: contains `username` and the current room connection count; best effort only.
- `pong`: response to an application-level ping command.
- `error`: contains a stable `code` and human-readable `message`.
- `room.archived`: final room metadata before the server closes the connection with 4409.

### Client commands

```json
{"type":"message","content":"Hello","client_message_id":"optional-id"}
```

```json
{"type":"ping"}
```

Every WebSocket publish checks `room:write`; read-only sessions receive `authorization_denied` and remain connected. After three invalid commands the server closes with 1008. Binary frames close after repeated rejection with 1003. Oversized frames close with 1009. Capacity rejection uses 1013; a missing room uses 4404.

## Delivery semantics

A `message.created` event is emitted only after SQLite commits the message. Broadcast is in-process and at-most-once; slow or failed clients are removed after the configured send timeout. Clients should reconnect with backoff, consume the initial history event, and use the HTTP cursor endpoint for older messages. Multi-worker or multi-host fan-out is not implemented in v0.5; archive teardown is deterministic only within the supported single process.
