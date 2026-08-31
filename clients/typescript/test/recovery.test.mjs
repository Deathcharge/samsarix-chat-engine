// Copyright (c) 2026 Samsarix LLC
// SPDX-License-Identifier: MPL-2.0
import assert from "node:assert/strict";
import test from "node:test";
import { SamsarixChatClient } from "../dist/index.js";

const READY = {
  type: "ready",
  room: {
    id: "room",
    name: "Room",
    description: "",
    created_at: "2026-08-31",
    archived_at: null,
    frozen_at: null,
  },
  username: "user",
  active_connections: 1,
  max_message_chars: 4000,
};
const HISTORY = { type: "history", items: [], next_before: null };
const flush = async () => {
  for (let i = 0; i < 12; i++) await Promise.resolve();
};

class BrowserSocket {
  readyState = 1;
  onopen = null;
  onmessage = null;
  onclose = null;
  onerror = null;
  sent = [];
  closes = [];
  receive(event) {
    this.onmessage?.({ data: JSON.stringify(event) });
  }
  send(data) {
    this.sent.push(JSON.parse(data));
  }
  close(code = 1000, reason = "") {
    if (code !== 1000 && !(code >= 3000 && code <= 4999))
      throw new Error("InvalidAccessError");
    this.closes.push([code, reason]);
    this.readyState = 3;
    this.onclose?.({ code, reason });
  }
  loss(code = 1012) {
    this.readyState = 3;
    this.onclose?.({ code, reason: "retry" });
  }
}

function setup(t, options = {}, credential = { token: "test-token" }) {
  let now = 0,
    id = 0;
  const timers = new Map();
  const originalSet = globalThis.setTimeout,
    originalClear = globalThis.clearTimeout;
  globalThis.setTimeout = (callback, delay = 0) => {
    timers.set(++id, { at: now + delay, callback });
    return id;
  };
  globalThis.clearTimeout = (timer) => timers.delete(timer);
  const sockets = [];
  const client = new SamsarixChatClient({
    baseUrl: "http://localhost",
    credential,
    webSocketFactory: () => {
      const socket = new BrowserSocket();
      sockets.push(socket);
      return socket;
    },
  });
  const session = client.roomSession("room", {
    handshakeTimeoutMs: 100,
    reconnect: {
      initialDelayMs: 10,
      maxDelayMs: 40,
      maxAttempts: 1,
      jitter: 0,
      stableConnectionMs: 50,
    },
    ...options,
  });
  t.after(() => {
    session.close();
    globalThis.setTimeout = originalSet;
    globalThis.clearTimeout = originalClear;
  });
  return {
    session,
    sockets,
    timers,
    async tick(ms) {
      const end = now + ms;
      for (;;) {
        const next = [...timers]
          .filter(([, timer]) => timer.at <= end)
          .sort((a, b) => a[1].at - b[1].at)[0];
        if (!next) break;
        now = next[1].at;
        timers.delete(next[0]);
        next[1].callback();
        await flush();
      }
      now = end;
      await flush();
    },
  };
}

function observe(promise) {
  const outcome = { state: "pending" };
  promise.then(
    () => {
      outcome.state = "resolved";
    },
    (error) => {
      outcome.state = "rejected";
      outcome.error = error;
    },
  );
  return outcome;
}
function activate(socket) {
  socket.receive(READY);
  socket.receive(HISTORY);
  socket.receive({ type: "pong" });
}

test("connect waits for history and its post-history activation pong", async (t) => {
  const { session, sockets } = setup(t);
  const outcome = observe(session.connect());
  await flush();
  sockets[0].receive(READY);
  await flush();
  assert.equal(outcome.state, "pending");
  assert.throws(() => session.sendMessage("too early"));
  sockets[0].receive({ type: "pong" });
  sockets[0].receive(HISTORY);
  await flush();
  assert.equal(outcome.state, "pending");
  assert.deepEqual(sockets[0].sent, [{ type: "ping" }]);
  sockets[0].receive({ type: "pong" });
  await flush();
  assert.equal(outcome.state, "resolved");
  assert.equal(session.state, "connected");
});

for (const phase of ["ready", "history", "active"]) {
  test(`flapping after ${phase} cannot renew the retry budget`, async (t) => {
    const { session, sockets, tick } = setup(t);
    observe(session.connect());
    await flush();
    for (let attempt = 0; attempt < 2; attempt++) {
      const socket = sockets[attempt];
      socket.receive(READY);
      if (phase !== "ready") socket.receive(HISTORY);
      if (phase === "active") socket.receive({ type: "pong" });
      socket.loss();
      await tick(10);
    }
    assert.equal(session.state, "closed");
    assert.equal(sockets.length, 2);
  });
}

for (const phase of ["credential", "opening", "ready", "history"]) {
  test(`deadline bounds an attempt stalled at ${phase}`, async (t) => {
    const { session, sockets, tick } = setup(
      t,
      {},
      phase === "credential" ? () => new Promise(() => {}) : undefined,
    );
    const outcome = observe(session.connect());
    await flush();
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
  const outcome = observe(session.connect());
  await flush();
  assert.doesNotThrow(() => sockets[0].receive(null));
  await flush();
  assert.equal(outcome.state, "rejected");
  assert.equal(outcome.error.code, 1002);
  assert.equal(sockets[0].closes[0][0], 4002);
  assert.equal(session.state, "closed");
});

test("closing from a reconnect state listener leaves no retry scheduled", async (t) => {
  const { session, sockets, tick, timers } = setup(t);
  observe(session.connect());
  await flush();
  activate(sockets[0]);
  session.onStateChange((state) => {
    if (state === "reconnecting") session.close();
  });
  sockets[0].loss();
  assert.equal(timers.size, 0);
  await tick(200);
  assert.equal(session.state, "closed");
  assert.equal(sockets.length, 1);
});

test("only an activated stable connection restores the automatic retry budget", async (t) => {
  const { session, sockets, tick } = setup(t);
  observe(session.connect());
  await flush();
  sockets[0].loss();
  await tick(10);
  activate(sockets[1]);
  await tick(49);
  assert.equal(session.state, "connected");
  await tick(1);
  sockets[1].loss();
  await tick(10);
  assert.equal(sockets.length, 3);
  activate(sockets[2]);
  sockets[2].loss();
  assert.equal(session.state, "closed");
  const manual = observe(session.connect());
  await flush();
  activate(sockets[3]);
  await flush();
  assert.equal(manual.state, "resolved");
  sockets[3].loss();
  await tick(10);
  assert.equal(sockets.length, 5);
});

test("old socket events and stability timers cannot revive or reset a new attempt", async (t) => {
  const { session, sockets, tick, timers } = setup(t);
  observe(session.connect());
  await flush();
  activate(sockets[0]);
  const oldMessage = sockets[0].onmessage,
    oldClose = sockets[0].onclose;
  const staleTimer = [...timers.values()][0].callback;
  await tick(40);
  sockets[0].loss();
  await tick(10);
  oldMessage({ data: JSON.stringify(READY) });
  oldClose({ code: 1012 });
  assert.equal(sockets.length, 2);
  assert.equal(session.state, "reconnecting");
  activate(sockets[1]);
  staleTimer();
  sockets[1].loss();
  assert.equal(session.state, "closed");
  assert.equal(timers.size, 0);
});

test("late credentials cannot create a socket after close or timeout", async (t) => {
  let resolve;
  const { session, sockets, tick } = setup(
    t,
    {},
    () =>
      new Promise((done) => {
        resolve = done;
      }),
  );
  const pending = observe(session.connect());
  await flush();
  await tick(100);
  assert.equal(pending.state, "rejected");
  session.close();
  resolve({ token: "late" });
  await flush();
  await tick(1000);
  assert.equal(sockets.length, 0);
  assert.equal(session.state, "closed");
});

test("manual close from connecting and history callbacks settles without reviving", async (t) => {
  const { session, sockets, timers } = setup(t);
  const unsubscribe = session.onStateChange((state) => {
    if (state === "connecting") session.close();
  });
  const first = observe(session.connect());
  await flush();
  assert.equal(first.state, "rejected");
  assert.equal(sockets.length, 0);
  assert.equal(timers.size, 0);
  unsubscribe();
  const second = observe(session.connect());
  await flush();
  session.onEvent((event) => {
    if (event.type === "history") session.close();
  });
  activate(sockets[0]);
  await flush();
  assert.equal(second.state, "rejected");
  assert.equal(session.state, "closed");
  assert.equal(sockets[0].sent.length, 0);
  assert.equal(timers.size, 0);
});

test("connected listener can close and create a new pending connection", async (t) => {
  const { session, sockets } = setup(t);
  let newer;
  session.onStateChange((state) => {
    if (state === "connected" && !newer) {
      session.close();
      newer = observe(session.connect());
    }
  });
  const first = observe(session.connect());
  await flush();
  activate(sockets[0]);
  await flush();
  assert.equal(first.state, "resolved");
  assert.equal(newer.state, "pending");
  assert.equal(sockets.length, 2);
  activate(sockets[1]);
  await flush();
  assert.equal(newer.state, "resolved");
});

test("transport errors, missing close callbacks and throwing close still settle once", async (t) => {
  const { session, sockets, tick } = setup(t);
  const pending = observe(session.connect());
  await flush();
  const oldClose = sockets[0].onclose;
  sockets[0].close = () => {
    throw new Error("broken transport");
  };
  sockets[0].onerror();
  oldClose({ code: 1006 });
  await flush();
  assert.equal(pending.state, "rejected");
  await tick(10);
  assert.equal(sockets.length, 2);
  sockets[1].onerror();
  assert.equal(session.state, "closed");
});

test("a failed authentication send closes the attempt instead of stranding it", async (t) => {
  const { session, sockets } = setup(t);
  const pending = observe(session.connect());
  await flush();
  sockets[0].send = () => {
    throw new Error("send failed");
  };
  assert.doesNotThrow(() =>
    sockets[0].receive({ type: "auth.required", message: "auth" }),
  );
  await flush();
  assert.equal(pending.state, "rejected");
  assert.equal(session.state, "reconnecting");
});

test("a failed application send reports uncertainty and reconnects without replaying it", async (t) => {
  const { session, sockets, tick } = setup(t);
  observe(session.connect());
  await flush();
  activate(sockets[0]);
  sockets[0].send = () => {
    throw new Error("transport write failed");
  };
  assert.throws(
    () => session.sendMessage("Maybe sent", "retry-id"),
    /transport write failed/,
  );
  assert.equal(session.state, "reconnecting");
  await tick(10);
  activate(sockets[1]);
  assert.deepEqual(sockets[1].sent, [{ type: "ping" }]);
});

test("a late failure from an old send cannot tear down a listener-created connection", async (t) => {
  const { session, sockets } = setup(t);
  observe(session.connect());
  await flush();
  activate(sockets[0]);
  let newer;
  sockets[0].send = () => {
    session.close();
    newer = observe(session.connect());
    throw new Error("old send failed after reconnect");
  };
  assert.throws(() => session.sendMessage("Maybe sent"), /old send failed/);
  await flush();
  assert.equal(newer.state, "pending");
  assert.equal(sockets.length, 2);
  activate(sockets[1]);
  await flush();
  assert.equal(newer.state, "resolved");
});

for (const sequence of [[HISTORY], [READY, READY], [READY, HISTORY, HISTORY]]) {
  test(`out-of-order or duplicate handshake is terminal: ${sequence.map((event) => event.type)}`, async (t) => {
    const { session, sockets } = setup(t);
    const pending = observe(session.connect());
    await flush();
    for (const event of sequence) sockets[0].receive(event);
    await flush();
    assert.equal(pending.error.code, 1002);
    assert.equal(session.state, "closed");
  });
}

test("invalid manual close arguments preserve an otherwise usable connection", async (t) => {
  const { session, sockets } = setup(t);
  observe(session.connect());
  await flush();
  activate(sockets[0]);
  for (const code of [1002, 1012, 2999, 5000, 4000.5, NaN])
    assert.throws(() => session.close(code), RangeError);
  assert.throws(() => session.close(1000, "😀".repeat(31)), RangeError);
  assert.equal(session.state, "connected");
  session.ping();
  session.close(4001, "😀".repeat(30));
  assert.equal(session.state, "closed");
});

for (const key of ["handshakeTimeoutMs", "stableConnectionMs"]) {
  for (const value of [0, -1, 300001, Infinity, NaN, true, 1.5]) {
    test(`${key} rejects ${value}`, () => {
      const client = new SamsarixChatClient({
        baseUrl: "http://localhost",
        credential: { token: "test" },
      });
      const options =
        key === "handshakeTimeoutMs"
          ? { [key]: value }
          : { reconnect: { [key]: value } };
      assert.throws(() => client.roomSession("room", options), RangeError);
    });
  }
}

test("reconnect jitter is capped and unsafe timer durations are rejected", async (t) => {
  const { session, sockets, timers } = setup(t, {
    reconnect: { initialDelayMs: 40, maxDelayMs: 40, jitter: 1 },
  });
  observe(session.connect());
  await flush();
  const original = Math.random;
  Math.random = () => 1;
  try {
    sockets[0].loss();
  } finally {
    Math.random = original;
  }
  assert.deepEqual(
    [...timers.values()].map((timer) => timer.at),
    [40],
  );
  const client = new SamsarixChatClient({
    baseUrl: "http://localhost",
    credential: { token: "test" },
  });
  for (const key of ["initialDelayMs", "maxDelayMs"]) {
    assert.throws(
      () => client.roomSession("room", { reconnect: { [key]: 2 ** 31 } }),
      RangeError,
    );
    assert.throws(
      () => client.roomSession("room", { reconnect: { [key]: 0.5 } }),
      RangeError,
    );
  }
});
