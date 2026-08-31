// Copyright (c) 2026 Samsarix LLC
// SPDX-License-Identifier: MPL-2.0
import assert from "node:assert/strict";
import test from "node:test";
import { SamsarixChatClient } from "../dist/index.js";

const READY = { type: "ready", room: { id: "room", name: "Room", description: "", created_at: "2026-08-31", archived_at: null, frozen_at: null }, username: "user", active_connections: 1, max_message_chars: 4000 };
const HISTORY = { type: "history", items: [], next_before: null };
const flush = async () => { for (let i = 0; i < 12; i++) await Promise.resolve(); };

class BrowserSocket {
  readyState = 1;
  onopen = null; onmessage = null; onclose = null; onerror = null;
  sent = []; closes = [];
  receive(event) { this.onmessage?.({ data: JSON.stringify(event) }); }
  send(data) { this.sent.push(JSON.parse(data)); }
  close(code = 1000, reason = "") {
    if (code !== 1000 && !(code >= 3000 && code <= 4999)) throw new Error("InvalidAccessError");
    this.closes.push([code, reason]); this.readyState = 3;
    this.onclose?.({ code, reason });
  }
  loss(code = 1012) { this.readyState = 3; this.onclose?.({ code, reason: "retry" }); }
}

function setup(t, options = {}, credential = { token: "test-token" }) {
  let now = 0, id = 0;
  const timers = new Map();
  const originalSet = globalThis.setTimeout, originalClear = globalThis.clearTimeout;
  globalThis.setTimeout = (callback, delay = 0) => { timers.set(++id, { at: now + delay, callback }); return id; };
  globalThis.clearTimeout = (timer) => timers.delete(timer);
  const sockets = [];
  const client = new SamsarixChatClient({ baseUrl: "http://localhost", credential, webSocketFactory: () => { const socket = new BrowserSocket(); sockets.push(socket); return socket; } });
  const session = client.roomSession("room", { handshakeTimeoutMs: 100, reconnect: { initialDelayMs: 10, maxDelayMs: 40, maxAttempts: 1, jitter: 0, stableConnectionMs: 50 }, ...options });
  t.after(() => { session.close(); globalThis.setTimeout = originalSet; globalThis.clearTimeout = originalClear; });
  return { session, sockets, timers, async tick(ms) {
    const end = now + ms;
    for (;;) {
      const next = [...timers].filter(([, timer]) => timer.at <= end).sort((a, b) => a[1].at - b[1].at)[0];
      if (!next) break;
      now = next[1].at; timers.delete(next[0]); next[1].callback(); await flush();
    }
    now = end; await flush();
  } };
}

function observe(promise) {
  const outcome = { state: "pending" };
  promise.then(() => { outcome.state = "resolved"; }, (error) => { outcome.state = "rejected"; outcome.error = error; });
  return outcome;
}
function activate(socket) { socket.receive(READY); socket.receive(HISTORY); socket.receive({ type: "pong" }); }

test("connect waits for history and its post-history activation pong", async (t) => {
  const { session, sockets } = setup(t);
  const outcome = observe(session.connect()); await flush();
  sockets[0].receive(READY); await flush();
  assert.equal(outcome.state, "pending");
  assert.throws(() => session.sendMessage("too early"));
  sockets[0].receive({ type: "pong" }); sockets[0].receive(HISTORY); await flush();
  assert.equal(outcome.state, "pending");
  assert.deepEqual(sockets[0].sent, [{ type: "ping" }]);
  sockets[0].receive({ type: "pong" }); await flush();
  assert.equal(outcome.state, "resolved"); assert.equal(session.state, "connected");
});

for (const phase of ["ready", "history", "active"]) {
  test(`flapping after ${phase} cannot renew the retry budget`, async (t) => {
    const { session, sockets, tick } = setup(t);
    observe(session.connect()); await flush();
    for (let attempt = 0; attempt < 2; attempt++) {
      const socket = sockets[attempt];
      socket.receive(READY);
      if (phase !== "ready") socket.receive(HISTORY);
      if (phase === "active") socket.receive({ type: "pong" });
      socket.loss(); await tick(10);
    }
    assert.equal(session.state, "closed"); assert.equal(sockets.length, 2);
  });
}

for (const phase of ["credential", "opening", "ready", "history"]) {
  test(`deadline bounds an attempt stalled at ${phase}`, async (t) => {
    const { session, sockets, tick } = setup(t, {}, phase === "credential" ? () => new Promise(() => {}) : undefined);
    const outcome = observe(session.connect()); await flush();
    if (phase === "ready" || phase === "history") sockets[0].receive(READY);
    if (phase === "history") sockets[0].receive(HISTORY);
    await tick(100);
    assert.equal(outcome.state, "rejected");
    assert.equal(outcome.error.code, 4008);
    await tick(110);
    assert.equal(session.state, "closed");
  });
}

test("protocol cleanup uses browser-legal close codes and terminates locally", async (t) => {
  const { session, sockets } = setup(t);
  const outcome = observe(session.connect()); await flush();
  assert.doesNotThrow(() => sockets[0].receive(null)); await flush();
  assert.equal(outcome.state, "rejected"); assert.equal(outcome.error.code, 1002);
  assert.equal(sockets[0].closes[0][0], 4002); assert.equal(session.state, "closed");
});

test("closing from a reconnect state listener leaves no retry scheduled", async (t) => {
  const { session, sockets, tick } = setup(t);
  observe(session.connect()); await flush(); activate(sockets[0]);
  session.onStateChange((state) => { if (state === "reconnecting") session.close(); });
  sockets[0].loss(); await tick(200);
  assert.equal(session.state, "closed"); assert.equal(sockets.length, 1);
});
