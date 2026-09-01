// Copyright (c) 2026 Samsarix LLC
// SPDX-License-Identifier: MPL-2.0

import assert from "node:assert/strict";
import test from "node:test";

import { RoomTimeline } from "../dist/index.js";

function message(id, content, createdAt) {
  return {
    id,
    room_id: "room",
    sender: "user",
    content,
    created_at: createdAt,
    client_message_id: null,
    parent_message_id: null,
    reactions: [],
    pinned_at: null,
    pinned_by: null,
    metadata: {},
    attachments: [],
    mentioned_subjects: [],
    edited_at: null,
    deleted_at: null,
  };
}

test("timeline replaces reconnect history and applies mutations before synchronization completes", () => {
  const timeline = new RoomTimeline();
  const first = message("first", "old", "2026-09-01T00:00:00Z");
  const second = message("second", "second", "2026-09-01T00:01:00Z");

  timeline.apply({ type: "ready", room: {}, username: "user", active_connections: 1, max_message_chars: 4000 });
  timeline.apply({ type: "history", items: [first], next_before: "older" });
  first.content = "mutated outside timeline";
  assert.equal(timeline.snapshot.items[0].content, "old");
  timeline.snapshot.items[0].content = "mutated snapshot";
  assert.equal(timeline.snapshot.items[0].content, "old");
  timeline.apply({ type: "message.updated", message: { ...first, content: "current" } });
  timeline.apply({ type: "message.created", message: second });
  assert.deepEqual(timeline.snapshot, {
    status: "synchronizing",
    generation: 0,
    items: [{ ...first, content: "current" }, second],
    truncated: false,
    nextBefore: "older",
  });

  timeline.apply({
    type: "sync.completed",
    strategy: "snapshot",
    history_count: 1,
    next_before: "older",
  });
  assert.equal(timeline.snapshot.status, "synchronized");
  assert.equal(timeline.snapshot.generation, 1);

  timeline.markStale();
  assert.equal(timeline.snapshot.status, "stale");
  timeline.apply({ type: "ready", room: {}, username: "user", active_connections: 1, max_message_chars: 4000 });
  timeline.apply({ type: "history", items: [second], next_before: null });
  timeline.apply({ type: "pong" });
  assert.deepEqual(timeline.snapshot, {
    status: "synchronized",
    generation: 2,
    items: [second],
    truncated: false,
    nextBefore: null,
  });
});

test("timeline listener failures are isolated and reported", () => {
  const failures = [];
  const timeline = new RoomTimeline({ onListenerError: (error) => failures.push(error) });
  timeline.onChange(() => {
    throw new Error("render failed");
  });
  timeline.apply({ type: "history", items: [], next_before: null });
  assert.equal(failures.length, 2);
  assert.match(failures[0].message, /render failed/);
});

test("timeline bounds history and live mutations while preserving an older-page cursor", () => {
  const timeline = new RoomTimeline({ maxMessages: 2 });
  const first = message("first", "first", "2026-09-01T00:00:00Z");
  const second = message("second", "second", "2026-09-01T00:01:00Z");
  const third = message("third", "third", "2026-09-01T00:02:00Z");
  const fourth = message("fourth", "fourth", "2026-09-01T00:03:00Z");

  timeline.apply({ type: "history", items: [first, second, third], next_before: null });
  assert.deepEqual(timeline.snapshot, {
    status: "synchronizing",
    generation: 0,
    items: [second, third],
    truncated: true,
    nextBefore: "second",
  });

  timeline.apply({ type: "message.created", message: fourth });
  assert.deepEqual(timeline.snapshot.items, [third, fourth]);
  assert.equal(timeline.snapshot.truncated, true);
  assert.equal(timeline.snapshot.nextBefore, "third");
});

test("timeline default bounds sustained live traffic", () => {
  const timeline = new RoomTimeline();
  for (let index = 0; index < 1_001; index += 1) {
    timeline.apply({
      type: "message.created",
      message: message(String(index), "message", new Date(index).toISOString()),
    });
  }

  assert.equal(timeline.snapshot.items.length, 1_000);
  assert.equal(timeline.snapshot.items[0].id, "1");
  assert.equal(timeline.snapshot.truncated, true);
  assert.equal(timeline.snapshot.nextBefore, "1");
});

test("timeline rejects unsafe retention limits", () => {
  for (const maxMessages of [0, 10_001, 1.5, Number.NaN]) {
    assert.throws(() => new RoomTimeline({ maxMessages }), /maxMessages/);
  }
});
