// Copyright (c) 2026 Samsarix LLC
// SPDX-License-Identifier: MPL-2.0

import type {
  ChatMessage,
  RoomEvent,
  RoomTimelineOptions,
  RoomTimelineSnapshot,
  RoomTimelineStatus,
} from "./types.js";

type TimelineListener = (snapshot: RoomTimelineSnapshot) => void;
const DEFAULT_MAX_MESSAGES = 1_000;
const MAX_MAX_MESSAGES = 10_000;

/**
 * Reconciles snapshot history with buffered and live message events.
 *
 * A timeline deliberately keeps its last known messages while disconnected and
 * labels them stale until a fresh server snapshot has replaced them.
 */
export class RoomTimeline {
  readonly maxMessages: number;
  private readonly messages = new Map<string, ChatMessage>();
  private readonly listeners = new Set<TimelineListener>();
  private readonly onListenerError: (error: unknown) => void;
  private currentStatus: RoomTimelineStatus = "empty";
  private currentGeneration = 0;
  private currentTruncated = false;
  private currentNextBefore: string | null = null;

  constructor(options: RoomTimelineOptions = {}) {
    this.maxMessages = boundedMessageLimit(options.maxMessages ?? DEFAULT_MAX_MESSAGES);
    this.onListenerError = options.onListenerError ?? (() => undefined);
  }

  get snapshot(): RoomTimelineSnapshot {
    return {
      status: this.currentStatus,
      generation: this.currentGeneration,
      items: [...this.messages.values()].sort(compareMessages).map(cloneMessage),
      truncated: this.currentTruncated,
      nextBefore: this.currentNextBefore,
    };
  }

  onChange(listener: TimelineListener): () => void {
    this.listeners.add(listener);
    this.notify(listener, this.snapshot);
    return () => this.listeners.delete(listener);
  }

  /** Apply one validated room event. RoomSession calls this before user event listeners. */
  apply(event: RoomEvent): void {
    switch (event.type) {
      case "ready":
        this.currentStatus = "synchronizing";
        this.publish();
        return;
      case "history":
        this.messages.clear();
        for (const message of event.items) this.messages.set(message.id, cloneMessage(message));
        this.currentNextBefore = event.next_before;
        this.currentTruncated = false;
        this.enforceLimit();
        this.currentStatus = "synchronizing";
        this.publish();
        return;
      case "message.created":
      case "message.updated":
      case "message.deleted":
      case "message.pin.updated":
      case "message.reaction.updated":
        this.messages.set(event.message.id, cloneMessage(event.message));
        this.enforceLimit();
        this.publish();
        return;
      case "sync.completed":
      case "pong":
        if (this.currentStatus === "synchronizing") {
          this.currentStatus = "synchronized";
          this.currentGeneration += 1;
          this.publish();
        }
        return;
      default:
        return;
    }
  }

  /** Preserve the last snapshot but make its disconnected status explicit. */
  markStale(): void {
    const nextStatus: RoomTimelineStatus = this.messages.size > 0 || this.currentGeneration > 0 ? "stale" : "empty";
    if (this.currentStatus !== nextStatus) {
      this.currentStatus = nextStatus;
      this.publish();
    }
  }

  private publish(): void {
    if (this.listeners.size === 0) return;
    const snapshot = this.snapshot;
    for (const listener of [...this.listeners]) this.notify(listener, snapshot);
  }

  private enforceLimit(): void {
    if (this.messages.size <= this.maxMessages) return;
    const ordered = [...this.messages.values()].sort(compareMessages);
    const removeCount = ordered.length - this.maxMessages;
    for (let index = 0; index < removeCount; index += 1) {
      this.messages.delete(ordered[index]!.id);
    }
    this.currentTruncated = true;
    this.currentNextBefore = ordered[removeCount]!.id;
  }

  private notify(listener: TimelineListener, snapshot: RoomTimelineSnapshot): void {
    try {
      listener(snapshot);
    } catch (error) {
      try {
        this.onListenerError(error);
      } catch {
        // A reporting hook must not break timeline reconciliation.
      }
    }
  }
}

function boundedMessageLimit(value: number): number {
  if (!Number.isInteger(value) || value < 1 || value > MAX_MAX_MESSAGES) {
    throw new RangeError(`maxMessages must be an integer between 1 and ${MAX_MAX_MESSAGES}`);
  }
  return value;
}

function compareMessages(left: ChatMessage, right: ChatMessage): number {
  const leftTime = Date.parse(left.created_at);
  const rightTime = Date.parse(right.created_at);
  const timeOrder =
    Number.isFinite(leftTime) && Number.isFinite(rightTime)
      ? leftTime - rightTime
      : left.created_at.localeCompare(right.created_at);
  return timeOrder || left.id.localeCompare(right.id);
}

function cloneMessage(message: ChatMessage): ChatMessage {
  return {
    ...message,
    ...(message.reactions === undefined ? {} : { reactions: message.reactions.map((reaction) => ({ ...reaction })) }),
    ...(message.metadata === undefined ? {} : { metadata: { ...message.metadata } }),
    ...(message.attachments === undefined
      ? {}
      : { attachments: message.attachments.map((attachment) => ({ ...attachment })) }),
    ...(message.mentioned_subjects === undefined ? {} : { mentioned_subjects: [...message.mentioned_subjects] }),
  };
}
