// Copyright (c) 2026 Samsarix LLC
// SPDX-License-Identifier: MPL-2.0

import assert from "node:assert/strict";

import { SamsarixChatClient } from "../dist/index.js";

const [baseUrl, token] = process.argv.slice(2);
if (!baseUrl || !token) {
  throw new Error("usage: node test/integration.mjs BASE_URL TOKEN");
}

const client = new SamsarixChatClient({ baseUrl, credential: { token } });
const room = await client.getRoom("sdk-room");
assert.equal(room.name, "SDK Room");

const created = await client.createMessage(
  "sdk-room",
  { content: "TypeScript HTTP", client_message_id: "sdk-http-1" },
  "sdk-http-1",
);
assert.equal(created.sender, "sdk-user");

const session = client.roomSession("sdk-room", {
  reconnect: { initialDelayMs: 10, maxDelayMs: 50, maxAttempts: 2, jitter: 0 },
});
const events = [];
session.onEvent((event) => events.push(event));
await session.connect();
await waitFor(() => events.some((event) => event.type === "history"), "history event");

const websocketCreated = nextEvent(session, "message.created");
session.sendMessage("TypeScript WebSocket", "sdk-ws-1");
const createdEvent = await websocketCreated;
assert.equal(createdEvent.message.client_message_id, "sdk-ws-1");

const updatedEvent = nextEvent(session, "message.updated");
const updated = await client.updateMessage("sdk-room", created.id, "TypeScript edited");
assert.equal(updated.content, "TypeScript edited");
assert.equal((await updatedEvent).message.id, created.id);

const deletedEvent = nextEvent(session, "message.deleted");
await client.deleteMessage("sdk-room", created.id);
assert.equal((await deletedEvent).message.content, "");

const history = await client.listMessages("sdk-room");
const tombstone = history.items.find((message) => message.id === created.id);
assert.equal(tombstone?.content, "");
assert.ok(tombstone?.deleted_at);
session.close();

console.log("typescript_client_http=ok websocket=ok reconnect_contract=ok edit_delete=ok");

function nextEvent(roomSession, type) {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      unsubscribe();
      reject(new Error(`Timed out waiting for ${type}`));
    }, 5_000);
    const unsubscribe = roomSession.onEvent((event) => {
      if (event.type === type) {
        clearTimeout(timeout);
        unsubscribe();
        resolve(event);
      }
    });
  });
}

async function waitFor(predicate, label) {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(`Timed out waiting for ${label}`);
}
