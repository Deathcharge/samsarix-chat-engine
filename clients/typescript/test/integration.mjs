// Copyright (c) 2026 Samsarix LLC
// SPDX-License-Identifier: MPL-2.0

import assert from "node:assert/strict";

// Optional local file URL lets the same journey verify an installed tarball.
const { SamsarixChatClient } = await import(process.env.SAMSARIX_TEST_SDK_MODULE ?? "../dist/index.js");

const [baseUrl, token] = process.argv.slice(2);
if (!baseUrl || !token) {
  throw new Error("usage: node test/integration.mjs BASE_URL TOKEN");
}

const sockets = [];
let holdCredential = false;
let releaseCredential;
let refreshEntered;
const refreshStarted = new Promise((resolve) => { refreshEntered = resolve; });
const resumeCredential = new Promise((resolve) => { releaseCredential = resolve; });
const client = new SamsarixChatClient({
  baseUrl,
  credential: async () => {
    if (holdCredential) {
      refreshEntered();
      await resumeCredential;
    }
    return { token };
  },
  webSocketFactory: (url) => {
    const socket = new WebSocket(url);
    sockets.push(socket);
    return socket;
  },
});
const room = await client.getRoom("sdk-room");
assert.equal(room.name, "SDK Room");

const initialReadState = await client.getReadState("sdk-room");
assert.equal(initialReadState.unread_count, 1);

const created = await client.createMessage(
  "sdk-room",
  {
    content: "TypeScript HTTP",
    client_message_id: "sdk-http-1",
    metadata: { "ticket.id": "SDK-1", priority: 2 },
    attachments: [{
      id: "sdk-trace-http",
      name: "request-trace.json",
      media_type: "application/json",
      size_bytes: 512,
      sha256: "a".repeat(64),
    }],
  },
  "sdk-http-1",
);
assert.equal(created.sender, "sdk-user");
assert.equal(created.parent_message_id, null);
assert.deepEqual(created.metadata, { priority: 2, "ticket.id": "SDK-1" });
assert.deepEqual(created.attachments?.map((attachment) => attachment.id), ["sdk-trace-http"]);
const httpReply = await client.createMessage("sdk-room", {
  content: "TypeScript HTTP reply",
  parent_message_id: created.id,
  client_message_id: "sdk-reply-1",
});
assert.equal(httpReply.parent_message_id, created.id);
assert.deepEqual((await client.listReplies("sdk-room", created.id)).items.map((message) => message.id), [httpReply.id]);
const search = await client.searchMessages("sdk-room", "typescript http");
assert.deepEqual(search.items.map((message) => message.id), [created.id, httpReply.id]);
const markedRead = await client.markRead("sdk-room", created.id);
assert.equal(markedRead.unread_count, 0);
await client.clearReadState("sdk-room");
assert.equal((await client.getReadState("sdk-room")).unread_count, 1);
await client.markRead("sdk-room");

const session = client.roomSession("sdk-room", {
  reconnect: { initialDelayMs: 10, maxDelayMs: 50, maxAttempts: 2, jitter: 0 },
});
const events = [];
session.onEvent((event) => events.push(event));
await session.connect();
assert.equal(session.state, "connected");
assert.ok(events.some((event) => event.type === "history"));
assert.ok(events.some((event) => event.type === "pong"));

const websocketCreated = nextEvent(session, "message.created");
session.sendMessage("", "sdk-ws-1", { source: "sdk-smoke" }, [{
  id: "sdk-trace-ws",
  name: "websocket-trace.txt",
  media_type: "text/plain",
  size_bytes: 128,
}]);
const createdEvent = await websocketCreated;
assert.equal(createdEvent.message.client_message_id, "sdk-ws-1");
assert.deepEqual(createdEvent.message.metadata, { source: "sdk-smoke" });
assert.equal(createdEvent.message.content, "");
assert.deepEqual(createdEvent.message.attachments?.map((attachment) => attachment.id), ["sdk-trace-ws"]);
const websocketReply = nextEvent(session, "message.created");
session.sendReply(created.id, "TypeScript WebSocket reply", "sdk-ws-reply-1");
assert.equal((await websocketReply).message.parent_message_id, created.id);

const reactionEvent = nextEvent(session, "message.reaction.updated");
const reaction = await client.addReaction("sdk-room", createdEvent.message.id, "ack");
assert.equal(reaction.changed, true);
assert.deepEqual(reaction.message.reactions, [{ key: "ack", count: 1 }]);
assert.deepEqual((await reactionEvent).message.reactions, [{ key: "ack", count: 1 }]);
const reactionRemovedEvent = nextEvent(session, "message.reaction.updated");
const removedReaction = await client.removeReaction("sdk-room", createdEvent.message.id, "ack");
assert.equal(removedReaction.present, false);
assert.deepEqual(removedReaction.message.reactions, []);
assert.equal((await reactionRemovedEvent).present, false);

const pinEvent = nextEvent(session, "message.pin.updated");
await assert.rejects(
  () => client.pinMessage("sdk-room", createdEvent.message.id, "sdk-agent"),
  (error) => error.code === "identity_mismatch",
);
const pinned = await client.pinMessage("sdk-room", createdEvent.message.id, "sdk-user");
assert.equal(pinned.changed, true);
assert.equal(pinned.message.pinned_by, "sdk-user");
assert.equal((await pinEvent).message.pinned_by, "sdk-user");
assert.equal((await client.listPinnedMessages("sdk-room")).items[0].id, createdEvent.message.id);
const unpinEvent = nextEvent(session, "message.pin.updated");
const unpinned = await client.unpinMessage("sdk-room", createdEvent.message.id, "sdk-user");
assert.equal(unpinned.pinned, false);
assert.equal(unpinned.message.pinned_at, null);
assert.equal((await unpinEvent).pinned, false);

const updatedEvent = nextEvent(session, "message.updated");
const updated = await client.updateMessage("sdk-room", created.id, "TypeScript edited");
assert.equal(updated.content, "TypeScript edited");
assert.deepEqual(updated.metadata, { priority: 2, "ticket.id": "SDK-1" });
assert.deepEqual(updated.attachments, created.attachments);
assert.equal((await updatedEvent).message.id, created.id);
assert.deepEqual(
  (await client.searchMessages("sdk-room", "typescript http")).items.map((message) => message.id),
  [httpReply.id],
);

const deletedEvent = nextEvent(session, "message.deleted");
await client.deleteMessage("sdk-room", created.id);
const deletedMessageEvent = await deletedEvent;
assert.equal(deletedMessageEvent.message.content, "");
assert.deepEqual(deletedMessageEvent.message.attachments, []);

const history = await client.listMessages("sdk-room");
const tombstone = history.items.find((message) => message.id === created.id);
assert.equal(tombstone?.content, "");
assert.deepEqual(tombstone?.metadata, {});
assert.deepEqual(tombstone?.attachments, []);
assert.ok(tombstone?.deleted_at);

// Close the native transport with a retryable application code, then hold the
// refreshed credential while a separate authenticated writer changes durable state.
// This is a real reconnect/history journey, not a kernel/network-fault simulation.
holdCredential = true;
sockets[0].close(4000, "Test reconnect");
await Promise.race([
  refreshStarted,
  new Promise((_, reject) => {
    const deadline = setTimeout(() => reject(new Error("No reconnect credential refresh")), 5_000);
    refreshStarted.then(() => clearTimeout(deadline));
  }),
]);
assert.equal(session.state, "reconnecting");
const writer = new SamsarixChatClient({ baseUrl, credential: { token } });
const offlineCreated = await writer.createMessage("sdk-room", { content: "Written during reconnect" });
const offlineEdited = await writer.updateMessage("sdk-room", createdEvent.message.id, "Edited during reconnect");
const recoveredHistory = nextEvent(session, "history");
const reconnected = session.connect();
holdCredential = false;
releaseCredential();
const recovered = await recoveredHistory;
await reconnected;
assert.equal(sockets.length, 2);
assert.equal(session.state, "connected");
assert.deepEqual(recovered.items.find((message) => message.id === offlineCreated.id), offlineCreated);
assert.deepEqual(recovered.items.find((message) => message.id === offlineEdited.id), offlineEdited);
assert.ok(recovered.items.find((message) => message.id === created.id)?.deleted_at);
const resumed = nextEvent(session, "message.created");
session.sendMessage("Live after reconnect", "sdk-resumed-1");
assert.equal((await resumed).message.client_message_id, "sdk-resumed-1");
session.close();

console.log("typescript_client_http=ok metadata=ok attachments=ok threads=ok reactions=ok pins=ok search=ok websocket=ok activation=ok reconnect_history=ok resumed_delivery=ok edit_delete=ok");

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
