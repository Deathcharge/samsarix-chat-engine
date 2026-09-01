# Participant read receipts

Participant receipts are an opt-in derived view over the existing monotonic read cursor. They fit small private support, classroom, and incident rooms where a product has a clear reason to show acknowledgement. They are not proof that a person saw or understood a message, and they are not network delivery receipts.

## Authorization and membership boundary

A signed user needs only `room:read` to get, advance, or clear their own read state. Seeing receipt state for supplied participants is separately gated by both `room:read` and `room:read-receipts`; an `admin` or operator principal also qualifies. Grant the receipt capability only to roles and rooms covered by the host application's visibility and consent policy.

The host owns membership. It sends 1–100 explicit, unique stable subject IDs to `POST /v1/rooms/{room_id}/read-receipts/query`. Samsarix preserves their order and returns a null receipt for an unknown or non-stored subject. It does not list members, reveal whether a supplied subject exists in another system, or infer membership from stored cursors.

```ts
const receipts = await chat.queryReadReceipts("support-42", ["customer-42", "agent-7"]);
```

Each receipt contains:

- `last_read_message_id`: the committed cursor message, or null;
- `last_read_message_at`: that message's `created_at`, or null;
- `last_read_at`: when the subject most recently advanced the cursor, or null.

Compare a message's `(created_at, id)` with `(last_read_message_at, last_read_message_id)` using the same timestamp-then-ID ordering as the server. Do not compare IDs alone. Advancing through an empty room may record `last_read_at` while leaving both message cursor fields null.

## Snapshot and live recovery

Subscribe to `read.updated` for low-latency changes, but treat HTTP as authoritative:

1. connect the room session;
2. query the explicit participant snapshot;
3. apply later `read.updated` receipts by subject;
4. repeat the snapshot after every reconnect.

The WebSocket stream is at-most-once and has no public durable receipt cursor. An idempotent/regressive mark or repeated clear produces no event. PostgreSQL commits a changed cursor and its relay event in one transaction; SQLite broadcasts after the local write commits. This prevents an event for an uncommitted change but does not make client delivery exactly once.

## Bounds and privacy

Query bodies are limited to 100 subjects, reject duplicates and padded identifiers, and use one database statement. Receipt queries and read-state updates each receive an independent per-caller budget configured by `SAMSARIX_CHAT_READ_STATE_QUERIES_PER_MINUTE`; cross-room inbox queries retain their own budget as well.

Read activity is behavioral data. Use opaque stable account IDs rather than email addresses or display names, disclose the behavior in the host product, and avoid enabling it in rooms where participant expectations conflict with visibility. Clearing read state deletes the current cursor, but clients that already received an event may retain a copy. In PostgreSQL preview mode, bounded relay-log copies remain until ordinary event pruning; backups may retain prior database state under the deployment owner's policy. Samsarix does not provide a consent registry, legal-compliance claim, or remote-client erasure protocol.

Current provider documentation uses read cursors and realtime receipt updates as familiar chat primitives, while some hosted products expose privacy controls. Samsarix therefore keeps receipt visibility as a separate least-privilege capability instead of attaching it to every reader. References checked 2026-09-01: [Sendbird read receipts](https://sendbird.com/docs/chat/platform-api/v3/message/read-receipts/read-receipts-overview) and [Stream unread/read events](https://getstream.io/chat/docs/node/unread_messages/).
