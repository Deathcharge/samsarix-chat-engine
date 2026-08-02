# `@samsarix/chat-client`

Dependency-free TypeScript client for Samsarix Chat Engine 0.8 HTTP and WebSocket contracts. It ships ESM and generated declarations, works with browser globals, and accepts injected `fetch`/`WebSocket` implementations for Node runtimes and tests.

This package is part of the Samsarix Chat Engine repository and is not yet published to npm.

## Install from a packed artifact

```bash
cd clients/typescript
npm ci
npm run build
npm pack
npm install ./samsarix-chat-client-0.2.0.tgz
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

const room = client.roomSession("support-42");
room.onStateChange((state) => console.log("chat state", state));
room.onEvent((event) => {
  if (event.type === "message.created") {
    console.log(event.message);
  }
});
await room.connect();
const unread = await client.getReadState("support-42");
console.log("unread", unread.unread_count);
await client.markRead("support-42");
room.setTyping(true);
room.sendMessage("Live follow-up", crypto.randomUUID());
room.setTyping(false);
```

The credential provider is called again on reconnect, allowing the host application to refresh short-lived tokens. Authentication secrets are sent in the first WebSocket message, never in the URL.

Read-state methods require a signed application-user token because the server binds the cursor to its stable subject; operator API keys cannot stand in for an end user. Typing is ephemeral and automatically expires server-side if a client misses its stop transition.

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

Unexpected transport loss retries with exponential backoff, bounded attempts, and jitter. Authentication, authorization, missing-room, archived-room, protocol, normal-client-close, and policy close codes are terminal. Every reconnect produces fresh `ready` and `history` events so the application can reconcile current edits and tombstones.

```ts
const session = client.roomSession("general", {
  reconnect: {
    initialDelayMs: 250,
    maxDelayMs: 5_000,
    maxAttempts: 8,
    jitter: 0.2,
  },
  onListenerError: (error) => reportClientError(error),
});
```

The client does not persist tokens, messages, or telemetry. The host application owns UI state and any durable cache.

## Development

```bash
npm ci
npm run check
npm test
npm run pack:check
```
