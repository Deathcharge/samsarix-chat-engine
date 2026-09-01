# `@samsarix/chat-client`

Dependency-free TypeScript client for Samsarix Chat Engine's implemented HTTP and WebSocket contracts, including the 0.12 server and guarded PostgreSQL preview. Unpublished SDK 0.6.0 ships ESM and generated declarations, works with browser globals, and accepts injected `fetch`/`WebSocket` implementations for Node runtimes and tests. Node 18 requires an injected WebSocket implementation; the live smoke uses Node's newer native WebSocket.

This package is part of the Samsarix Chat Engine repository and is not yet published to npm.

## Install from a packed artifact

```bash
cd clients/typescript
npm ci
npm run build
npm pack
npm install ./samsarix-chat-client-0.6.0.tgz
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
  { content: "Hello", client_message_id: crypto.randomUUID() },
  "request-42",
);
const reply = await client.createMessage("support-42", {
  content: "Here is the next step",
  parent_message_id: message.id,
  client_message_id: crypto.randomUUID(),
});
const thread = await client.listReplies("support-42", message.id, { limit: 25 });
const acknowledgement = await client.addReaction("support-42", message.id, "ack");

const room = client.roomSession("support-42");
room.onStateChange((state) => console.log("chat state", state));
room.onEvent((event) => {
  if (event.type === "history") {
    // Reconcile event.items by message ID, including edits and tombstones.
    // Page older history over HTTP when event.next_before is non-null.
  }
  if (event.type === "message.created") {
    console.log(event.message);
  }
  if (event.type === "message.reaction.updated") {
    console.log(event.key, event.message.reactions);
  }
});
await room.connect();
const unread = await client.getReadState("support-42");
const matches = await client.searchMessages("support-42", "payment failed", { limit: 25 });
console.log("matches", matches.items);
console.log("unread", unread.unread_count);
await client.markRead("support-42");
room.setTyping(true);
room.sendMessage("Live follow-up", crypto.randomUUID());
room.sendReply(message.id, "Threaded follow-up", crypto.randomUUID());
room.setTyping(false);
```

The credential provider is called again on reconnect, allowing the host application to refresh short-lived tokens. Authentication secrets are sent in the first WebSocket message, never in the URL.

Read-state methods require a signed application-user token because the server binds the cursor to its stable subject; operator API keys cannot stand in for an end user. Typing is ephemeral and automatically expires server-side if a client misses its stop transition.

Replies are one level deep. `listReplies()` pages one top-level message's replies, while `sendReply()` publishes through the existing `message.created` event with `message.parent_message_id` set. Room history remains a flat chronological stream, so reconcile all messages by ID and use the parent field only for presentation/grouping. The output field remains optional in the SDK type so current clients can consume released 0.12 WebSocket events, which predate the field; the threaded development server always returns either a parent ID or null.

`addReaction()` and `removeReaction()` mutate one validated reaction key for the signed subject and return the complete updated message plus `changed`/`present`. Operator-key callers pass a fourth `reactor` argument because an operator key does not identify an end user. Reaction updates arrive as `message.reaction.updated`; replace the message by ID rather than incrementing counts locally. The `reactions` output is optional in the SDK type so 0.6.0 can still consume released 0.12 messages, while the current development server always returns a sorted array. The server caps distinct keys at 20 per message and accepts lowercase ASCII keys such as `ack`, `resolved`, or `needs_attention`; it does not host emoji assets.

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

Unexpected transport loss retries with exponential backoff, bounded attempts, and jitter. Authentication, authorization, missing-room, archived-room, protocol, normal-client-close, and policy close codes are terminal. Every successful reconnect produces fresh `ready` and `history` events so the application can reconcile current edits and tombstones.

**0.4.0 contract change:** `connect()` resolves and state becomes `connected` only after `ready`, `history`, and the reply to an SDK-generated post-history `ping`. The server processes that ping after its initialization buffer has flushed. The handshake's `pong` is also delivered to event listeners. Register listeners before calling `connect()`; do not send from a `ready`/`history` listener while synchronization is pending. Await `connect()` or observe `connected` before sending. There is no new server frame, durable client cursor, exactly-once guarantee or claim that other replicas have caught up. Continue reconciling live events by message ID.

One `handshakeTimeoutMs` deadline covers each attempt from credential lookup through transport/authentication, initial history, and activation reply. Expiry rejects the pending promise with `SamsarixConnectionError.code === 4008`, detaches that attempt, requests transport closure, and consumes the same bounded retry budget as other transient failures. The SDK cannot cancel arbitrary work inside an application credential provider, abort a WebSocket transport beyond its `close()` API, or guarantee timer execution while a browser tab/event loop is suspended. Late credentials and callbacks cannot revive an expired attempt. A provider that never settles remains shared by that client's credential requests; time-bound the provider in the host application.

`maxAttempts` counts automatic retries after the initial connection. Merely receiving `ready`, history or an activation pong does **not** reset it. Reset occurs only after the activated connection stays locally open for `stableConnectionMs`. This prevents repeated initialization failures or short-lived connections from restoring the retry budget. The stability interval is not an ongoing heartbeat/liveness guarantee. Exhaustion ends in `closed`; a deliberate new `connect()` starts a fresh budget. Avoid implementing an unbounded automatic `connect()` loop in a `closed` listener.

A `connect()` promise belongs to its current/next attempt and rejects on that attempt's failure even if automatic retries continue; observe state changes and handle promise rejection. Concurrent callers share the same pending promise. `close()` cancels owned timers, rejects pending connection work and detaches callbacks before requesting transport closure. Listener-driven close/reconnect does not leave stale timers or settle a newer promise.

A synchronous application-send failure throws `SamsarixConnectionError` and starts bounded connection recovery. It does not prove the message was uncommitted, and the SDK never automatically replays it. Reconcile history and use the same `client_message_id` or HTTP idempotency key when deliberately retrying a write.

```ts
const session = client.roomSession("general", {
  handshakeTimeoutMs: 10_000,
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

The client does not persist tokens, messages, or telemetry. The host application owns UI state and any durable cache.

Deterministic fake-clock tests cover flapping, deadlines, recovery-budget reset, late callbacks, cancellation and reentrant listeners. The live smoke uses a real server and native Node WebSocket, deliberately closes one connection with a retryable code, holds credential refresh while another client writes/edits, then verifies history and resumed delivery. This is not a browser compatibility matrix, network-fault simulation or measured reconnect-storm/load benchmark.

## Development

```bash
npm ci
npm run check
npm test
npm run pack:check
```
