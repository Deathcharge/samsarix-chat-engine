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

test("room exports preserve the streaming response for operator-controlled consumption", async () => {
  const requests = [];
  const client = new SamsarixChatClient({
    baseUrl: "https://chat.example",
    credential: { apiKey: "operator-key" },
    fetch: async (url, init) => {
      requests.push({ url: String(url), init });
      return new Response('{"type":"samsarix.room_export","schema_version":2}\n', {
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
