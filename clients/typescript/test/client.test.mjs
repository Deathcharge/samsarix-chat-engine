// Copyright (c) 2026 Samsarix LLC
// SPDX-License-Identifier: MPL-2.0

import assert from "node:assert/strict";
import test from "node:test";

import { SamsarixApiError, SamsarixChatClient } from "../dist/index.js";

function jsonResponse(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

test("REST methods authenticate, encode identifiers, and preserve idempotency", async () => {
  const requests = [];
  let credentials = 0;
  const client = new SamsarixChatClient({
    baseUrl: "https://chat.example/",
    credential: async () => {
      credentials += 1;
      return { token: "room-token" };
    },
    fetch: async (url, init) => {
      requests.push({ url: String(url), init });
      return jsonResponse({
        id: "message-1",
        room_id: "room/a",
        sender: "user",
        content: "hello",
        created_at: "2026-08-01T00:00:00Z",
        client_message_id: "request-1",
        parent_message_id: null,
        edited_at: null,
        deleted_at: null,
      });
    },
  });

  const message = await client.createMessage("room/a", { content: "hello" }, "request-1");

  assert.equal(message.id, "message-1");
  assert.equal(credentials, 1);
  assert.equal(requests[0].url, "https://chat.example/v1/rooms/room%2Fa/messages");
  assert.equal(requests[0].init.method, "POST");
  const headers = new Headers(requests[0].init.headers);
  assert.equal(headers.get("Authorization"), "Bearer room-token");
  assert.equal(headers.get("Idempotency-Key"), "request-1");
  assert.equal(headers.get("Content-Type"), "application/json");
  assert.deepEqual(JSON.parse(requests[0].init.body), { content: "hello" });
});

test("message metadata is canonicalized, updated, cleared, and bounded before transport", async () => {
  const requests = [];
  const client = new SamsarixChatClient({
    baseUrl: "https://chat.example",
    credential: { token: "room-token" },
    fetch: async (url, init) => {
      requests.push({ url: String(url), init });
      return jsonResponse({});
    },
  });

  await client.createMessage("support", {
    content: "Investigating",
    metadata: { "ticket.id": "SUP-42", priority: 2 },
  });
  await client.updateMessage("support", "message-1", "Resolved", {});
  await client.updateMessage("support", "message-1", "Copy edit");

  assert.deepEqual(JSON.parse(requests[0].init.body).metadata, { priority: 2, "ticket.id": "SUP-42" });
  assert.deepEqual(JSON.parse(requests[1].init.body), { content: "Resolved", metadata: {} });
  assert.deepEqual(JSON.parse(requests[2].init.body), { content: "Copy edit" });
  await assert.rejects(
    client.createMessage("support", { content: "bad", metadata: { Bad: "key" } }),
    RangeError,
  );
  await assert.rejects(
    client.createMessage("support", { content: "bad", metadata: { number: Number.NaN } }),
    RangeError,
  );
  await assert.rejects(
    client.createMessage("support", { content: "bad", metadata: { context: "é".repeat(2050) } }),
    RangeError,
  );
  assert.equal(requests.length, 3);
});

test("concurrent operations share one in-flight credential refresh", async () => {
  let credentials = 0;
  let releaseCredential;
  const credentialReady = new Promise((resolve) => {
    releaseCredential = resolve;
  });
  const client = new SamsarixChatClient({
    baseUrl: "https://chat.example",
    credential: async () => {
      credentials += 1;
      await credentialReady;
      return { token: `token-${credentials}` };
    },
    fetch: async () => jsonResponse([]),
  });

  const first = client.listRooms();
  const second = client.listRooms();
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.equal(credentials, 1);
  releaseCredential();
  await Promise.all([first, second]);
  await client.listRooms();
  assert.equal(credentials, 2);
});

test("operator operations set confirmation headers and accept empty responses", async () => {
  const requests = [];
  const client = new SamsarixChatClient({
    baseUrl: "http://127.0.0.1:8000",
    credential: { apiKey: "operator-key" },
    fetch: async (url, init) => {
      requests.push({ url: String(url), init });
      return new Response(null, { status: 204 });
    },
  });

  await client.deleteRoom("general");

  const headers = new Headers(requests[0].init.headers);
  assert.equal(headers.get("X-API-Key"), "operator-key");
  assert.equal(headers.get("X-Confirm-Room-Delete"), "general");
});

test("read-state methods preserve the signed-subject workflow", async () => {
  const requests = [];
  const readState = {
    room_id: "support",
    subject: "customer",
    last_read_message_id: "message-1",
    last_read_at: "2026-08-01T00:00:00Z",
    unread_count: 0,
  };
  const client = new SamsarixChatClient({
    baseUrl: "https://chat.example",
    credential: { token: "customer-token" },
    fetch: async (url, init) => {
      requests.push({ url: String(url), init });
      return init?.method === "DELETE" ? new Response(null, { status: 204 }) : jsonResponse(readState);
    },
  });

  assert.deepEqual(await client.getReadState("support"), readState);
  assert.deepEqual(await client.markRead("support", "message-1"), readState);
  assert.deepEqual(await client.markRead("support"), readState);
  await client.clearReadState("support");

  assert.equal(requests[1].init.method, "PUT");
  assert.deepEqual(JSON.parse(requests[1].init.body), { message_id: "message-1" });
  assert.deepEqual(JSON.parse(requests[2].init.body), {});
  assert.equal(requests[3].init.method, "DELETE");
});

test("message search encodes normalized queries and pagination", async () => {
  const requests = [];
  const page = { items: [], next_before: null };
  const client = new SamsarixChatClient({
    baseUrl: "https://chat.example",
    credential: { token: "support-token" },
    fetch: async (url, init) => {
      requests.push({ url: String(url), init });
      return jsonResponse(page);
    },
  });

  assert.deepEqual(
    await client.searchMessages("support/eu", "  ＰＡＹＭＥＮＴ & café  ", { limit: 25, before: "message-9" }),
    page,
  );
  const url = new URL(requests[0].url);
  assert.equal(url.pathname, "/v1/rooms/support%2Feu/messages/search");
  assert.equal(url.searchParams.get("q"), "PAYMENT & café");
  assert.equal(url.searchParams.get("limit"), "25");
  assert.equal(url.searchParams.get("before"), "message-9");

  await client.searchMessages("support/eu", "𝑎".repeat(51));
  await client.searchMessages("support/eu", "ß".repeat(100));
  assert.equal(new URL(requests[1].url).searchParams.get("q"), "a".repeat(51));
  assert.equal(new URL(requests[2].url).searchParams.get("q"), "ß".repeat(100));
});

test("thread reply listing encodes parent identifiers and pagination", async () => {
  const requests = [];
  const page = { items: [], next_before: null };
  const client = new SamsarixChatClient({
    baseUrl: "https://chat.example",
    credential: { token: "support-token" },
    fetch: async (url, init) => {
      requests.push({ url: String(url), init });
      return jsonResponse(page);
    },
  });

  assert.deepEqual(
    await client.listReplies("support/eu", "parent/1", { limit: 25, before: "reply-9" }),
    page,
  );
  const url = new URL(requests[0].url);
  assert.equal(url.pathname, "/v1/rooms/support%2Feu/messages/parent%2F1/replies");
  assert.equal(url.searchParams.get("limit"), "25");
  assert.equal(url.searchParams.get("before"), "reply-9");
});

test("reaction methods validate keys and encode idempotent actor mutations", async () => {
  const requests = [];
  const mutation = {
    message: {
      id: "message/1",
      room_id: "support/eu",
      sender: "agent",
      content: "Acknowledged",
      created_at: "2026-08-31T00:00:00Z",
      client_message_id: null,
      parent_message_id: null,
      reactions: [{ key: "ack", count: 1 }],
      edited_at: null,
      deleted_at: null,
    },
    key: "ack",
    reactor: "agent",
    present: true,
    changed: true,
    updated_at: "2026-08-31T00:00:01Z",
  };
  const client = new SamsarixChatClient({
    baseUrl: "https://chat.example",
    credential: { apiKey: "operator-key" },
    fetch: async (url, init) => {
      requests.push({ url: String(url), init });
      return jsonResponse(mutation);
    },
  });

  assert.deepEqual(await client.addReaction("support/eu", "message/1", "ack", "agent"), mutation);
  await client.removeReaction("support/eu", "message/1", "ack", "agent");

  assert.equal(
    requests[0].url,
    "https://chat.example/v1/rooms/support%2Feu/messages/message%2F1/reactions/ack",
  );
  assert.equal(requests[0].init.method, "PUT");
  assert.deepEqual(JSON.parse(requests[0].init.body), { reactor: "agent" });
  assert.equal(requests[1].init.method, "DELETE");
  await assert.rejects(client.addReaction("support", "message", "Not Valid"), RangeError);
  await assert.rejects(client.addReaction("support", "message", "ack", " "), RangeError);
  assert.equal(requests.length, 2);
});

test("pin methods and pagination encode room and message identifiers", async () => {
  const requests = [];
  const mutation = {
    message: {
      id: "message/1",
      room_id: "support/eu",
      sender: "agent",
      content: "Resolution",
      created_at: "2026-08-31T00:00:00Z",
      client_message_id: null,
      pinned_at: "2026-08-31T00:00:01Z",
      pinned_by: "agent",
      edited_at: null,
      deleted_at: null,
    },
    pinner: "agent",
    pinned: true,
    changed: true,
    updated_at: "2026-08-31T00:00:01Z",
  };
  const client = new SamsarixChatClient({
    baseUrl: "https://chat.example",
    credential: { apiKey: "operator-key" },
    fetch: async (url, init) => {
      requests.push({ url: String(url), init });
      return jsonResponse(requests.length === 1 ? { items: [], next_before: null } : mutation);
    },
  });

  await client.listPinnedMessages("support/eu", { limit: 25, before: "message/9" });
  assert.deepEqual(await client.pinMessage("support/eu", "message/1", "agent"), mutation);
  await client.unpinMessage("support/eu", "message/1", "agent");

  const listUrl = new URL(requests[0].url);
  assert.equal(listUrl.pathname, "/v1/rooms/support%2Feu/messages/pins");
  assert.equal(listUrl.searchParams.get("limit"), "25");
  assert.equal(listUrl.searchParams.get("before"), "message/9");
  assert.equal(requests[1].url, "https://chat.example/v1/rooms/support%2Feu/messages/message%2F1/pin");
  assert.equal(requests[1].init.method, "PUT");
  assert.deepEqual(JSON.parse(requests[1].init.body), { pinner: "agent" });
  assert.equal(requests[2].init.method, "DELETE");
  await assert.rejects(client.pinMessage("support", "message", " "), RangeError);
  await assert.rejects(client.listPinnedMessages("support", { limit: 101 }), RangeError);
  assert.equal(requests.length, 3);
});

test("room exports preserve the streaming response for operator-controlled consumption", async () => {
  const requests = [];
  const client = new SamsarixChatClient({
    baseUrl: "https://chat.example",
    credential: { apiKey: "operator-key" },
    fetch: async (url, init) => {
      requests.push({ url: String(url), init });
      return new Response('{"type":"samsarix.room_export","schema_version":3}\n', {
        headers: { "Content-Type": "application/x-ndjson" },
      });
    },
  });

  const response = await client.exportRoom("room/a");

  assert.equal(response.bodyUsed, false);
  assert.equal(requests[0].url, "https://chat.example/v1/rooms/room%2Fa/export");
  assert.equal(new Headers(requests[0].init.headers).get("Accept"), "application/x-ndjson");
});

test("stable API errors retain status, code, message, and details", async () => {
  const client = new SamsarixChatClient({
    baseUrl: "https://chat.example",
    credential: { token: "room-token" },
    fetch: async () =>
      jsonResponse(
        { error: { code: "room_muted", message: "Muted", details: { until: "later" } } },
        403,
      ),
  });

  await assert.rejects(client.getRoom("general"), (error) => {
    assert.ok(error instanceof SamsarixApiError);
    assert.equal(error.status, 403);
    assert.equal(error.code, "room_muted");
    assert.equal(error.message, "Muted");
    assert.deepEqual(error.details, { until: "later" });
    return true;
  });
});

test("list limits and credential shapes fail before transport", async () => {
  const client = new SamsarixChatClient({
    baseUrl: "https://chat.example",
    credential: { token: "room-token" },
    fetch: async () => {
      throw new Error("transport should not run");
    },
  });
  await assert.rejects(client.listRooms(101), RangeError);
  await assert.rejects(client.listMessages("general", { limit: 0 }), RangeError);
  await assert.rejects(client.listReplies("general", "parent", { limit: 0 }), RangeError);
  await assert.rejects(client.searchMessages("general", " "), RangeError);
  await assert.rejects(client.searchMessages("general", "𝑎"), RangeError);
  await assert.rejects(client.searchMessages("general", "valid", { limit: 101 }), RangeError);

  const invalid = new SamsarixChatClient({
    baseUrl: "https://chat.example",
    credential: { token: "" },
    fetch: async () => jsonResponse({}),
  });
  await assert.rejects(invalid.getRoom("general"), TypeError);
  assert.throws(
    () => new SamsarixChatClient({ baseUrl: "file:///chat", credential: { token: "x" }, fetch }),
    TypeError,
  );
  assert.throws(
    () => new SamsarixChatClient({ baseUrl: "https://user:secret@chat.example", credential: { token: "x" }, fetch }),
    TypeError,
  );

  const ambiguous = new SamsarixChatClient({
    baseUrl: "https://chat.example",
    credential: { token: "x", apiKey: "y" },
    fetch: async () => jsonResponse({}),
  });
  await assert.rejects(ambiguous.getRoom("general"), /exactly one/);
});
