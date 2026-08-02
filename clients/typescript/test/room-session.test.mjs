// Copyright (c) 2026 Samsarix LLC
// SPDX-License-Identifier: MPL-2.0

import assert from "node:assert/strict";
import test from "node:test";

import { SamsarixChatClient, SamsarixConnectionError } from "../dist/index.js";

class FakeSocket {
  readyState = 0;
  onopen = null;
  onmessage = null;
  onclose = null;
  onerror = null;
  sent = [];
  closes = [];

  open() {
    this.readyState = 1;
    this.onopen?.();
  }

  receive(event) {
    this.onmessage?.({ data: JSON.stringify(event) });
  }

  receiveRaw(data) {
    this.onmessage?.({ data });
  }

  send(data) {
    this.sent.push(JSON.parse(data));
  }

  close(code = 1000, reason = "") {
    this.readyState = 3;
    this.closes.push([code, reason]);
    this.onclose?.({ code, reason });
  }

  serverClose(code, reason = "") {
    this.readyState = 3;
    this.onclose?.({ code, reason });
  }
}

async function waitFor(predicate, message = "condition") {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
  throw new Error(`Timed out waiting for ${message}`);
}

test("browser authentication, typed events, publish, ping, and close", async () => {
  const sockets = [];
  const urls = [];
  const client = new SamsarixChatClient({
    baseUrl: "https://chat.example/base/",
    credential: { token: "token-1" },
    fetch: async () => new Response(),
    webSocketFactory: (url) => {
      urls.push(url);
      const socket = new FakeSocket();
      sockets.push(socket);
      return socket;
    },
  });
  const session = client.roomSession("room/a", { reconnect: { enabled: false } });
  const events = [];
  const states = [];
  session.onEvent((event) => events.push(event));
  session.onStateChange((state) => states.push(state));

  const connected = session.connect();
  await waitFor(() => sockets.length === 1, "socket creation");
  sockets[0].open();
  sockets[0].receive({ type: "auth.required", message: "authenticate" });
  assert.deepEqual(sockets[0].sent[0], { type: "auth", token: "token-1" });
  sockets[0].receive({
    type: "ready",
    room: {},
    username: "user",
    active_connections: 1,
    max_message_chars: 10,
  });
  await connected;

  session.sendMessage("hello", "client-1");
  session.ping();
  assert.throws(() => session.sendMessage(" "), TypeError);
  assert.throws(() => session.sendMessage("more than ten"), RangeError);
  assert.throws(() => session.sendMessage("hello", "x".repeat(129)), RangeError);
  assert.deepEqual(sockets[0].sent.slice(1), [
    { type: "message", content: "hello", client_message_id: "client-1" },
    { type: "ping" },
  ]);
  assert.equal(urls[0], "wss://chat.example/base/v1/rooms/room%2Fa/ws");
  assert.deepEqual(states, ["idle", "connecting", "connected"]);
  assert.equal(events[0].type, "auth.required");

  session.close();
  assert.equal(session.state, "closed");
  assert.deepEqual(sockets[0].closes.at(-1), [1000, "Client closed"]);
});

test("unexpected loss refreshes credentials and reconnects with bounded state", async () => {
  const sockets = [];
  let credentialCalls = 0;
  const client = new SamsarixChatClient({
    baseUrl: "http://127.0.0.1:8000",
    credential: () => ({ token: `token-${++credentialCalls}` }),
    fetch: async () => new Response(),
    webSocketFactory: () => {
      const socket = new FakeSocket();
      sockets.push(socket);
      return socket;
    },
  });
  const session = client.roomSession("general", {
    reconnect: { initialDelayMs: 0, maxDelayMs: 0, maxAttempts: 2, jitter: 0 },
  });
  const states = [];
  session.onStateChange((state) => states.push(state));
  const connected = session.connect();
  await waitFor(() => sockets.length === 1);
  sockets[0].receive({ type: "auth.required", message: "authenticate" });
  sockets[0].receive({ type: "ready", room: {}, username: "user", active_connections: 1, max_message_chars: 4000 });
  await connected;

  sockets[0].serverClose(1006, "network loss");
  const reconnected = session.connect();
  await waitFor(() => sockets.length === 2, "reconnect");
  sockets[1].receive({ type: "auth.required", message: "authenticate" });
  assert.deepEqual(sockets[1].sent[0], { type: "auth", token: "token-2" });
  sockets[1].receive({ type: "ready", room: {}, username: "user", active_connections: 1, max_message_chars: 4000 });
  await reconnected;

  assert.equal(credentialCalls, 2);
  assert.equal(sockets.length, 2);
  assert.ok(states.includes("reconnecting"));
  session.close();
});

test("terminal policy closes do not reconnect", async () => {
  const sockets = [];
  const client = new SamsarixChatClient({
    baseUrl: "https://chat.example",
    credential: { token: "token" },
    fetch: async () => new Response(),
    webSocketFactory: () => {
      const socket = new FakeSocket();
      sockets.push(socket);
      return socket;
    },
  });
  const session = client.roomSession("general", {
    reconnect: { initialDelayMs: 0, maxDelayMs: 0, maxAttempts: 2, jitter: 0 },
  });
  const connected = session.connect();
  await waitFor(() => sockets.length === 1);
  sockets[0].serverClose(4403, "Room access revoked");
  await assert.rejects(connected, (error) => {
    assert.ok(error instanceof SamsarixConnectionError);
    assert.equal(error.code, 4403);
    return true;
  });
  await new Promise((resolve) => setTimeout(resolve, 5));
  assert.equal(sockets.length, 1);
  assert.equal(session.state, "closed");
});

test("API-key sessions require username and authenticate without URL credentials", async () => {
  const sockets = [];
  const urls = [];
  const client = new SamsarixChatClient({
    baseUrl: "http://localhost:8000",
    credential: { apiKey: "operator-secret" },
    fetch: async () => new Response(),
    webSocketFactory: (url) => {
      urls.push(url);
      const socket = new FakeSocket();
      sockets.push(socket);
      return socket;
    },
  });
  const session = client.roomSession("general", { username: "Operator", reconnect: { enabled: false } });
  const connected = session.connect();
  await waitFor(() => sockets.length === 1);
  assert.equal(urls[0], "ws://localhost:8000/v1/rooms/general/ws?username=Operator");
  assert.ok(!urls[0].includes("operator-secret"));
  sockets[0].receive({ type: "auth.required", message: "authenticate" });
  assert.deepEqual(sockets[0].sent[0], { type: "auth", api_key: "operator-secret" });
  sockets[0].receive({ type: "ready", room: {}, username: "Operator", active_connections: 1, max_message_chars: 4000 });
  await connected;
  session.close();

  const invalid = client.roomSession("general", { reconnect: { enabled: false } });
  await assert.rejects(invalid.connect(), (error) => {
    assert.ok(error instanceof SamsarixConnectionError);
    assert.match(error.message, /username must be/);
    return true;
  });
});

test("invalid server frames close with protocol error and disconnected sends fail", async () => {
  const sockets = [];
  const client = new SamsarixChatClient({
    baseUrl: "https://chat.example",
    credential: { token: "token" },
    fetch: async () => new Response(),
    webSocketFactory: () => {
      const socket = new FakeSocket();
      sockets.push(socket);
      return socket;
    },
  });
  const session = client.roomSession("general", { reconnect: { enabled: false } });
  assert.throws(() => session.sendMessage("too early"), SamsarixConnectionError);
  const connected = session.connect();
  await waitFor(() => sockets.length === 1);
  sockets[0].receiveRaw("not-json");
  await assert.rejects(connected, SamsarixConnectionError);
  assert.deepEqual(sockets[0].closes[0], [1002, "Invalid JSON event"]);
});

test("malformed event envelopes close cleanly with a protocol error", async () => {
  const sockets = [];
  const client = new SamsarixChatClient({
    baseUrl: "https://chat.example",
    credential: { token: "token" },
    fetch: async () => new Response(),
    webSocketFactory: () => {
      const socket = new FakeSocket();
      sockets.push(socket);
      return socket;
    },
  });
  const session = client.roomSession("general", { reconnect: { enabled: false } });
  const connected = session.connect();
  await waitFor(() => sockets.length === 1);
  sockets[0].receiveRaw("null");
  await assert.rejects(connected, SamsarixConnectionError);
  assert.deepEqual(sockets[0].closes[0], [1002, "Invalid event envelope"]);
});

test("consumer callback failures are reported without disrupting state", async () => {
  const sockets = [];
  const reported = [];
  const client = new SamsarixChatClient({
    baseUrl: "https://chat.example",
    credential: { token: "token" },
    fetch: async () => new Response(),
    webSocketFactory: () => {
      const socket = new FakeSocket();
      sockets.push(socket);
      return socket;
    },
  });
  const session = client.roomSession("general", {
    reconnect: { enabled: false },
    onListenerError: (error) => reported.push(error),
  });
  session.onStateChange(() => {
    throw new Error("state listener failed");
  });
  session.onEvent(() => {
    throw new Error("event listener failed");
  });

  const connected = session.connect();
  await waitFor(() => sockets.length === 1);
  sockets[0].receive({ type: "ready", room: {}, username: "user", active_connections: 1, max_message_chars: 4000 });
  await connected;

  assert.equal(session.state, "connected");
  assert.equal(reported.length, 4);
  session.close();
});
