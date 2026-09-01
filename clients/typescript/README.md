# `@samsarix/chat-client`

Dependency-free TypeScript client for Samsarix Chat Engine's implemented HTTP and WebSocket contracts, including the 0.12 server and guarded PostgreSQL preview. Unpublished SDK 0.12.0 ships ESM and generated declarations, a reconnect-aware bounded in-memory room timeline, works with browser globals, and accepts injected `fetch`/`WebSocket` implementations for Node runtimes and tests. Node 18 requires an injected WebSocket implementation; the live smoke uses Node's newer native WebSocket.

This package is part of the Samsarix Chat Engine repository and is not yet published to npm.

## Install from a packed artifact

```bash
cd clients/typescript
npm ci
npm run build
npm pack
npm install ./samsarix-chat-client-0.12.0.tgz
```

## Token client

```ts
import { SamsarixChatClient } from "@samsarix/chat-client";

const client = new SamsarixChatClient({
  baseUrl: "https://chat.example.com",
  credential: async () => ({ token: await obtainShortLivedRoomToken() }),
});

const message = await client.createMessage(
  "support-42",
  {
    content: "Hello",
    client_message_id: crypto.randomUUID(),
    metadata: { "ticket.id": "SUP-42", priority: 2 },
    mentioned_subjects: ["agent-7"],
  },
  "request-42",
);
const reply = await client.createMessage("support-42", {
  content: "Here is the next step",
  parent_message_id: message.id,
  client_message_id: crypto.randomUUID(),
});
const evidence = await client.createMessage("support-42", {
  attachments: [{
    id: "upload:SUP-42:trace",
    name: "trace.json",
    media_type: "application/json",
    size_bytes: 256,
  }],
});
const thread = await client.listReplies("support-42", message.id, { limit: 25 });
const acknowledgement = await client.addReaction("support-42", message.id, "ack");
await client.pinMessage("support-42", message.id);
const pinned = await client.listPinnedMessages("support-42");

const room = client.roomSession("support-42");
room.onStateChange((state) => console.log("chat state", state));
room.timeline.onChange((snapshot) => {
  renderMessages(snapshot.items);
  showOfflineState(snapshot.status === "stale");
  // Page older history over HTTP when snapshot.nextBefore is non-null.
});
room.onEvent((event) => {
  if (event.type === "message.created") {
    console.log(event.message);
  }
  if (event.type === "message.reaction.updated") {
    console.log(event.key, event.message.reactions);
  }
  if (event.type === "message.pin.updated") {
    console.log(event.pinned, event.message.pinned_by);
  }
});
await room.connect();
const unread = await client.getReadState("support-42");
const inbox = await client.queryReadStates(["support-42", "support-43"]);
const matches = await client.searchMessages("support-42", "payment failed", { limit: 25 });
console.log("matches", matches.items);
console.log("unread", unread.unread_count);
console.log("inbox unread", inbox.total_unread_count);
await client.markRead("support-42");
room.setTyping(true);
room.sendMessage("Live follow-up", crypto.randomUUID(), { "ticket.id": "SUP-42" }, undefined, ["agent-7"]);
room.sendReply(message.id, "Threaded follow-up", crypto.randomUUID(), { action: "escalate" }, undefined, ["lead"]);
room.sendMessage("", crypto.randomUUID(), undefined, evidence.attachments);
room.setTyping(false);
```

The credential provider is called again on reconnect, allowing the host application to refresh short-lived tokens. Authentication secrets are sent in the first WebSocket message, never in the URL.

Read-state methods require a signed application-user token because the server binds the cursor to its stable subject; operator API keys cannot stand in for an end user. `queryReadStates()` accepts 1–100 unique room IDs, validates them before transport, and preserves caller order. The token must grant `room:read` for every requested room. Its content-free result includes each cursor/count and latest visible message ID/time plus aggregate unread totals; the host still owns room discovery, assignment, labels, and previews. Typing is ephemeral and automatically expires server-side if a client misses its stop transition.

Replies are one level deep. `listReplies()` pages one top-level message's replies, while `sendReply()` publishes through the existing `message.created` event with `message.parent_message_id` set. Room history remains a flat chronological stream, so reconcile all messages by ID and use the parent field only for presentation/grouping. The output field remains optional in the SDK type so current clients can consume released 0.12 WebSocket events, which predate the field; the threaded development server always returns either a parent ID or null.

`addReaction()` and `removeReaction()` mutate one validated reaction key for the signed subject and return the complete updated message plus `changed`/`present`. Operator-key callers pass a fourth `reactor` argument because an operator key does not identify an end user. Reaction updates arrive as `message.reaction.updated`; replace the message by ID rather than incrementing counts locally. The `reactions` output is optional in the SDK type so 0.6.0 can still consume released 0.12 messages, while the current development server always returns a sorted array. The server caps distinct keys at 20 per message and accepts lowercase ASCII keys such as `ack`, `resolved`, or `needs_attention`; it does not host emoji assets.

`listPinnedMessages()` returns shared room pins newest-first; `pinMessage()` and `unpinMessage()` require a token with `room:read` plus `room:pin`. Operator-key callers pass the optional third `pinner` argument. A real change arrives as `message.pin.updated`; replace the message by ID and refresh the list when ordering matters. `pinned_at` and `pinned_by` are optional SDK fields for released-0.12 compatibility, while the development server always returns them as a timestamp/actor or null.

Message `metadata` is a flat, bounded object for application context such as ticket IDs, assignment references, incident severity, or action names. Keys and scalar values are validated client-side before HTTP or WebSocket transport; the server remains authoritative. `updateMessage(roomId, messageId, content)` preserves metadata, while a fourth `{}` clears it and a complete object replaces it. The field is optional on `ChatMessage` so this SDK can still consume released 0.12 events. Treat it as untrusted display/integration data, never authorization, HTML, routing, or executable component configuration.

Message `attachments` is an ordered list of at most five validated, opaque host-owned file descriptors. `createMessage()` can omit content when one is present; `sendMessage()`/`sendReply()` accept descriptors in their attachment argument. The engine and SDK reject URL fields: upload bytes, verified type/size/digest, malware controls, authorization, fresh short-lived download URLs, object cleanup and storage/egress cost belong to the host application. The field stays optional on `ChatMessage` for released-0.12 compatibility. Treat names/media types as untrusted display text and see the repository's [attachment boundary](../../docs/ATTACHMENTS.md).

Message `mentioned_subjects` is an ordered list of at most ten unique, case-sensitive stable host IDs. `createMessage()` accepts it in the payload; `sendMessage()`/`sendReply()` accept it after the attachment argument; and `updateMessage()` accepts it after metadata. Omission preserves mentions during an edit, while `[]` clears them. The SDK validates shape before transport, but only the host can resolve membership, aliases and preferences. Samsarix does not parse `@` text or send notifications. The field stays optional on `ChatMessage` for released-0.12 compatibility; see the repository's [mention boundary](../../docs/MENTIONS.md).

## Operator session

API keys are administrative credentials and must not be embedded in browser bundles. A trusted Node process can use an injected WebSocket implementation and must supply the operator display name separately:

```ts
const operator = new SamsarixChatClient({
  baseUrl: "http://127.0.0.1:8000",
  credential: { apiKey: process.env.SAMSARIX_CHAT_API_KEY! },
  webSocketFactory: (url) => new WebSocket(url),
});

const session = operator.roomSession("incident", { username: "On-call" });
```

Administrative exports remain streaming responses rather than being buffered by the SDK:

```ts
const response = await operator.exportRoom("incident");
for await (const chunk of response.body!) {
  // Process schema-versioned NDJSON incrementally.
}
```

## Reconnect behavior

Unexpected transport loss retries with exponential backoff, bounded attempts, and jitter. Authentication, authorization, missing-room, archived-room, protocol, normal-client-close, and policy close codes are terminal. Every successful reconnect produces fresh `ready` and `history` events. `RoomSession.timeline` keeps the last known page as `stale` during disconnection, replaces it from the new history snapshot, applies complete created/updated/deleted/reaction/pin message objects by ID, and becomes `synchronized` only at the activation boundary. Its generation increments once per completed initial connection or reconnect. It sorts by `created_at` then ID and exposes `nextBefore` for older HTTP pagination.

The timeline retains at most 1000 messages by default. Set `timelineMaxMessages` from 1 through 10000 on `roomSession()` to choose another bound, or construct a standalone `RoomTimeline({ maxMessages })`. When the bound evicts older entries, `snapshot.truncated` becomes true and `nextBefore` advances to the oldest retained message so the host can load older state over HTTP. The cursor can still expire under server retention; reload current history after `invalid_cursor`.

**0.12.0 contract change:** current servers advertise `snapshot_sync_v1` in `ready.capabilities`. The SDK sends a post-history `sync` command and waits for `sync.completed`, whose count/cursor must match that history snapshot. The server processes the command only after its local initialization buffer has flushed. Against an older server that does not advertise the capability, the SDK retains the 0.4.0 post-history ping/pong fallback. Both activation replies are delivered to event listeners. Register listeners before calling `connect()`; do not send from a `ready`/`history` listener while synchronization is pending. Await `connect()` or observe `connected` before sending.

The marker completes a local snapshot handoff, not durable event replay. There is no public event cursor, exactly-once guarantee, recovery of missed ephemeral presence/typing, or claim that a different PostgreSQL replica has polled to the global head. The timeline is an in-memory newest-page reducer, not an offline database: fetch older pages separately, and reload other derived views such as pin ordering and read state when needed.

One `handshakeTimeoutMs` deadline covers each attempt from credential lookup through transport/authentication, initial history, and activation reply. Expiry rejects the pending promise with `SamsarixConnectionError.code === 4008`, detaches that attempt, requests transport closure, and consumes the same bounded retry budget as other transient failures. The SDK cannot cancel arbitrary work inside an application credential provider, abort a WebSocket transport beyond its `close()` API, or guarantee timer execution while a browser tab/event loop is suspended. Late credentials and callbacks cannot revive an expired attempt. A provider that never settles remains shared by that client's credential requests; time-bound the provider in the host application.

`maxAttempts` counts automatic retries after the initial connection. Merely receiving `ready`, history or an activation reply does **not** reset it. Reset occurs only after the activated connection stays locally open for `stableConnectionMs`. This prevents repeated initialization failures or short-lived connections from restoring the retry budget. The stability interval is not an ongoing heartbeat/liveness guarantee. Exhaustion ends in `closed`; a deliberate new `connect()` starts a fresh budget. Avoid implementing an unbounded automatic `connect()` loop in a `closed` listener.

A `connect()` promise belongs to its current/next attempt and rejects on that attempt's failure even if automatic retries continue; observe state changes and handle promise rejection. Concurrent callers share the same pending promise. `close()` cancels owned timers, rejects pending connection work and detaches callbacks before requesting transport closure. Listener-driven close/reconnect does not leave stale timers or settle a newer promise.

A synchronous application-send failure throws `SamsarixConnectionError` and starts bounded connection recovery. It does not prove the message was uncommitted, and the SDK never automatically replays it. Reconcile history and use the same `client_message_id` or HTTP idempotency key when deliberately retrying a write.

```ts
const session = client.roomSession("general", {
  handshakeTimeoutMs: 10_000,
  timelineMaxMessages: 1_000,
  reconnect: {
    initialDelayMs: 250,
    maxDelayMs: 5_000,
    maxAttempts: 8,
    jitter: 0.2,
    stableConnectionMs: 10_000,
  },
  onListenerError: (error) => reportClientError(error),
});
```

Both new durations are positive integers from 1 to 300000 milliseconds and cannot be disabled. Reconnect delays are integers from 0 to 2147483647, with `maxDelayMs >= initialDelayMs`; jitter remains 0–1 and the final scheduled delay is capped at `maxDelayMs`. These limits avoid [Node timer overflow turning long delays into near-immediate retries](https://nodejs.org/api/timers.html#settimeoutcallback-delay-args). Actual timer scheduling can be later than requested.

Public `close(code, reason)` follows the [browser WebSocket close contract](https://websockets.spec.whatwg.org/#dom-websocket-close): code 1000 or 3000–4999, and at most 123 UTF-8 bytes for the reason. Invalid arguments throw before changing session state. SDK protocol errors remain terminal local errors with code 1002, but request browser-legal wire closure 4002; other local transport failures use 4000 and handshake deadlines use 4008. A failing injected transport's close method cannot prevent local cleanup; injected implementations must still honor their physical transport ownership.

The client does not persist tokens, timeline messages, or telemetry. The host application owns any durable/offline cache and should treat `stale` timeline content accordingly. Timeline snapshots clone nested message collections so consumer mutation cannot corrupt subsequent reconciliation; deterministic oldest-first eviction prevents a long-lived room from growing automatic SDK state without limit.

Deterministic fake-clock tests cover flapping, deadlines, recovery-budget reset, late callbacks, cancellation, reentrant listeners, capability fallback, snapshot replacement and buffered mutation reconciliation. The live smoke uses a real server and native Node WebSocket, deliberately closes one connection with a retryable code, holds credential refresh while another client writes/edits, then verifies explicit synchronization, history and resumed delivery. This is not a browser compatibility matrix, network-fault simulation or measured reconnect-storm/load benchmark.

## Development

```bash
npm ci
npm run check
npm test
npm run pack:check
```
