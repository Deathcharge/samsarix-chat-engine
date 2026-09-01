// Copyright (c) 2026 Samsarix LLC
// SPDX-License-Identifier: MPL-2.0

import { isAttachmentReferences, normalizeAttachmentReferences } from "./attachments.js";
import { SamsarixConnectionError } from "./errors.js";
import { isMessageMetadata, normalizeMessageMetadata } from "./metadata.js";
import { isMentionedSubjects, normalizeMentionedSubjects } from "./mentions.js";
import { RoomTimeline } from "./room-timeline.js";
import type { SamsarixChatClient } from "./client.js";
import type {
  AttachmentReference,
  ConnectionState,
  Credential,
  RoomEvent,
  MessageMetadata,
  RoomSessionOptions,
  WebSocketFactory,
  WebSocketLike,
} from "./types.js";

const OPEN = 1;
const MAX_TIMER_MS = 2_147_483_647;
const TERMINAL_CLOSE_CODES = new Set([1000, 1002, 1008, 4401, 4403, 4404, 4409]);
const EVENT_TYPES = new Set([
  "auth.required",
  "error",
  "history",
  "member.banned",
  "message.created",
  "message.deleted",
  "message.pin.updated",
  "message.reaction.updated",
  "message.updated",
  "pong",
  "presence.joined",
  "presence.left",
  "read.updated",
  "ready",
  "room.archived",
  "room.frozen",
  "room.unfrozen",
  "sync.completed",
  "typing.started",
  "typing.stopped",
]);

type EventListener = (event: RoomEvent) => void;
type StateListener = (state: ConnectionState) => void;

interface ReconnectPolicy {
  enabled: boolean;
  initialDelayMs: number;
  maxDelayMs: number;
  maxAttempts: number;
  jitter: number;
  stableConnectionMs: number;
}

export class RoomSession {
  readonly roomId: string;
  readonly timeline: RoomTimeline;
  private readonly client: SamsarixChatClient;
  private readonly username?: string;
  private readonly reconnect: ReconnectPolicy;
  private readonly handshakeTimeoutMs: number;
  private readonly onListenerError: (error: unknown) => void;
  private readonly eventListeners = new Set<EventListener>();
  private readonly stateListeners = new Set<StateListener>();
  private socket: WebSocketLike | undefined;
  private connectPromise: Promise<void> | undefined;
  private resolveConnect: (() => void) | undefined;
  private rejectConnect: ((error: Error) => void) | undefined;
  private reconnectTimer: ReturnType<typeof setTimeout> | undefined;
  private handshakeTimer: ReturnType<typeof setTimeout> | undefined;
  private stableTimer: ReturnType<typeof setTimeout> | undefined;
  private phase: "opening" | "ready" | "history" | "active" = "opening";
  private generation = 0;
  private attempts = 0;
  private manuallyClosed = false;
  private currentState: ConnectionState = "idle";
  private maxMessageChars: number | undefined;
  private supportsSnapshotSync = false;
  private synchronizationHistoryCount = 0;
  private synchronizationNextBefore: string | null = null;

  constructor(client: SamsarixChatClient, roomId: string, options: RoomSessionOptions) {
    if (roomId.length === 0) {
      throw new TypeError("roomId must not be empty");
    }
    this.client = client;
    this.roomId = roomId;
    if (options.username !== undefined) {
      this.username = options.username;
    }
    this.reconnect = reconnectPolicy(options.reconnect);
    this.handshakeTimeoutMs = boundedDuration(options.handshakeTimeoutMs ?? 10_000, "handshakeTimeoutMs");
    this.onListenerError = options.onListenerError ?? (() => undefined);
    this.timeline = new RoomTimeline({
      ...(options.timelineMaxMessages === undefined ? {} : { maxMessages: options.timelineMaxMessages }),
      onListenerError: this.onListenerError,
    });
  }

  get state(): ConnectionState {
    return this.currentState;
  }

  connect(): Promise<void> {
    if (this.currentState === "connected") {
      return Promise.resolve();
    }
    if (this.connectPromise !== undefined) {
      return this.connectPromise;
    }
    this.manuallyClosed = false;
    const promise = new Promise<void>((resolve, reject) => {
      this.resolveConnect = resolve;
      this.rejectConnect = reject;
    });
    this.connectPromise = promise;
    if (this.currentState !== "reconnecting") {
      this.attempts = 0;
      void this.openSocket(false);
    }
    // A synchronous state listener may close or reconnect during openSocket.
    return promise;
  }

  close(code = 1000, reason = "Client closed"): void {
    if (!Number.isInteger(code) || (code !== 1000 && (code < 3000 || code > 4999))) {
      throw new RangeError("close code must be 1000 or an integer between 3000 and 4999");
    }
    if (new TextEncoder().encode(reason).byteLength > 123) {
      throw new RangeError("close reason must not exceed 123 UTF-8 bytes");
    }
    this.manuallyClosed = true;
    this.timeline.markStale();
    if (this.reconnectTimer !== undefined) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }
    const error = new SamsarixConnectionError(reason, code);
    const socket = this.detachAttempt();
    this.rejectPending(error);
    this.setState("closed");
    closeTransport(socket, code, reason);
  }

  sendMessage(
    content: string,
    clientMessageId?: string,
    metadata?: MessageMetadata,
    attachments?: AttachmentReference[],
    mentionedSubjects?: readonly string[],
  ): void {
    const normalizedAttachments =
      attachments === undefined ? undefined : normalizeAttachmentReferences(attachments);
    if (content.trim().length === 0 && (normalizedAttachments?.length ?? 0) === 0) {
      throw new TypeError("content or at least one attachment is required");
    }
    if (this.maxMessageChars !== undefined && content.length > this.maxMessageChars) {
      throw new RangeError(`content exceeds the ${this.maxMessageChars}-character room limit`);
    }
    if (clientMessageId !== undefined && (clientMessageId.length === 0 || clientMessageId.length > 128)) {
      throw new RangeError("clientMessageId must be between 1 and 128 characters");
    }
    this.send({
      type: "message",
      content,
      ...(clientMessageId === undefined ? {} : { client_message_id: clientMessageId }),
      ...(metadata === undefined ? {} : { metadata: normalizeMessageMetadata(metadata) }),
      ...(normalizedAttachments === undefined ? {} : { attachments: normalizedAttachments }),
      ...(mentionedSubjects === undefined
        ? {}
        : { mentioned_subjects: normalizeMentionedSubjects(mentionedSubjects) }),
    });
  }

  sendReply(
    parentMessageId: string,
    content: string,
    clientMessageId?: string,
    metadata?: MessageMetadata,
    attachments?: AttachmentReference[],
    mentionedSubjects?: readonly string[],
  ): void {
    if (parentMessageId.length === 0 || parentMessageId.length > 128) {
      throw new RangeError("parentMessageId must be between 1 and 128 characters");
    }
    const normalizedAttachments =
      attachments === undefined ? undefined : normalizeAttachmentReferences(attachments);
    if (content.trim().length === 0 && (normalizedAttachments?.length ?? 0) === 0) {
      throw new TypeError("content or at least one attachment is required");
    }
    if (this.maxMessageChars !== undefined && content.length > this.maxMessageChars) {
      throw new RangeError(`content exceeds the ${this.maxMessageChars}-character room limit`);
    }
    if (clientMessageId !== undefined && (clientMessageId.length === 0 || clientMessageId.length > 128)) {
      throw new RangeError("clientMessageId must be between 1 and 128 characters");
    }
    this.send({
      type: "message",
      content,
      parent_message_id: parentMessageId,
      ...(clientMessageId === undefined ? {} : { client_message_id: clientMessageId }),
      ...(metadata === undefined ? {} : { metadata: normalizeMessageMetadata(metadata) }),
      ...(normalizedAttachments === undefined ? {} : { attachments: normalizedAttachments }),
      ...(mentionedSubjects === undefined
        ? {}
        : { mentioned_subjects: normalizeMentionedSubjects(mentionedSubjects) }),
    });
  }

  ping(): void {
    this.send({ type: "ping" });
  }

  setTyping(active: boolean): void {
    this.send({ type: "typing", active });
  }

  onEvent(listener: EventListener): () => void {
    this.eventListeners.add(listener);
    return () => this.eventListeners.delete(listener);
  }

  onStateChange(listener: StateListener): () => void {
    this.stateListeners.add(listener);
    this.notifyListener(() => listener(this.currentState));
    return () => this.stateListeners.delete(listener);
  }

  private async openSocket(reconnecting: boolean): Promise<void> {
    const generation = ++this.generation;
    this.phase = "opening";
    this.maxMessageChars = undefined;
    this.supportsSnapshotSync = false;
    this.synchronizationHistoryCount = 0;
    this.synchronizationNextBefore = null;
    this.handshakeTimer = setTimeout(() => {
      this.failAttempt(generation, new SamsarixConnectionError("WebSocket handshake timed out", 4008), 4008);
    }, this.handshakeTimeoutMs);
    this.setState(reconnecting ? "reconnecting" : "connecting");
    if (!this.isCurrent(generation)) return;
    try {
      const credential = await this.client.credential();
      if (!this.isCurrent(generation)) return;
      let url: string;
      try {
        url = websocketUrl(this.client.baseUrl, this.roomId, credential, this.username);
      } catch (error) {
        this.failAttempt(generation, asConnectionError(error), 4000, false);
        return;
      }
      const factory = this.client.webSocketFactory ?? defaultWebSocketFactory;
      const socket = factory(url);
      if (!this.isCurrent(generation)) {
        closeTransport(socket, 1000, "Connection cancelled");
        return;
      }
      this.socket = socket;
      socket.onopen = () => undefined;
      socket.onmessage = (event) => {
        if (this.isCurrent(generation)) {
          try {
            this.handleMessage(generation, socket, credential, event.data);
          } catch (error) {
            this.failAttempt(generation, asConnectionError(error), 4000);
          }
        }
      };
      socket.onerror = () => {
        this.failAttempt(generation, new SamsarixConnectionError("WebSocket connection failed"), 4000);
      };
      socket.onclose = (event) => {
        this.failAttempt(
          generation,
          new SamsarixConnectionError(event.reason || "WebSocket closed", event.code),
          4000,
          !TERMINAL_CLOSE_CODES.has(event.code),
        );
      };
    } catch (error) {
      this.failAttempt(generation, asConnectionError(error), 4000);
    }
  }

  private handleMessage(generation: number, socket: WebSocketLike, credential: Credential, data: unknown): void {
    if (typeof data !== "string") {
      this.protocolError(generation, "JSON text events required");
      return;
    }
    let parsed: unknown;
    try {
      parsed = JSON.parse(data);
    } catch {
      this.protocolError(generation, "Invalid JSON event");
      return;
    }
    if (!isRoomEvent(parsed)) {
      this.protocolError(generation, "Invalid event envelope");
      return;
    }
    const event = parsed;
    let timelineApplied = false;
    if (event.type === "auth.required") {
      if (this.phase !== "opening") {
        this.protocolError(generation, "Unexpected authentication challenge");
        return;
      }
      socket.send(
        JSON.stringify("token" in credential ? { type: "auth", token: credential.token } : { type: "auth", api_key: credential.apiKey }),
      );
    } else if (event.type === "ready") {
      if (this.phase !== "opening") {
        this.protocolError(generation, "Unexpected ready event");
        return;
      }
      this.phase = "ready";
      this.maxMessageChars = event.max_message_chars;
      this.supportsSnapshotSync = event.capabilities?.includes("snapshot_sync_v1") ?? false;
    } else if (event.type === "history") {
      if (this.phase !== "ready") {
        this.protocolError(generation, "Unexpected history event");
        return;
      }
      this.phase = "history";
      this.synchronizationHistoryCount = event.items.length;
      this.synchronizationNextBefore = event.next_before;
    } else if ((event.type === "sync.completed" || event.type === "pong") && this.phase === "history") {
      if (
        event.type === "sync.completed" &&
        (!this.supportsSnapshotSync ||
          event.history_count !== this.synchronizationHistoryCount ||
          event.next_before !== this.synchronizationNextBefore)
      ) {
        this.protocolError(generation, "Invalid synchronization boundary");
        return;
      }
      this.phase = "active";
      clearTimeout(this.handshakeTimer);
      this.handshakeTimer = undefined;
      this.stableTimer = setTimeout(() => {
        if (!this.isCurrent(generation)) return;
        this.stableTimer = undefined;
        if (this.phase === "active" && socket.readyState === OPEN) {
          this.attempts = 0;
        }
      }, this.reconnect.stableConnectionMs);
      this.timeline.apply(event);
      timelineApplied = true;
      if (!this.isCurrent(generation)) return;
      const resolve = this.resolveConnect;
      this.clearPending();
      resolve?.();
      this.setState("connected");
    }
    if (!timelineApplied) this.timeline.apply(event);
    for (const listener of this.eventListeners) {
      if (!this.isCurrent(generation)) return;
      this.notifyListener(() => listener(event));
    }
    if (event.type === "history" && this.isCurrent(generation)) {
      // The server enters its receive loop only after flushing the initial
      // history handoff. New servers expose that boundary explicitly; ping is
      // retained as the compatible activation probe for older servers.
      socket.send(JSON.stringify({ type: this.supportsSnapshotSync ? "sync" : "ping" }));
    }
  }

  private isCurrent(generation: number): boolean {
    return generation === this.generation && !this.manuallyClosed;
  }

  private detachAttempt(): WebSocketLike | undefined {
    this.generation += 1;
    clearTimeout(this.handshakeTimer);
    clearTimeout(this.stableTimer);
    this.handshakeTimer = undefined;
    this.stableTimer = undefined;
    const socket = this.socket;
    this.socket = undefined;
    if (socket !== undefined) {
      socket.onopen = socket.onmessage = socket.onerror = socket.onclose = null;
    }
    return socket;
  }

  private protocolError(generation: number, message: string): void {
    // Browser close() rejects wire code 1002; keep it as the local error code.
    this.failAttempt(generation, new SamsarixConnectionError(message, 1002), 4002, false);
  }

  private failAttempt(generation: number, error: SamsarixConnectionError, wireCode: number, retry = true): void {
    if (!this.isCurrent(generation)) return;
    const socket = this.detachAttempt();
    this.timeline.markStale();
    this.rejectPending(error);
    this.scheduleReconnect(retry);
    closeTransport(socket, wireCode, "Client connection ended");
  }

  private scheduleReconnect(retry: boolean): void {
    if (!retry || !this.reconnect.enabled || this.attempts >= this.reconnect.maxAttempts) {
      this.setState("closed");
      return;
    }
    this.attempts += 1;
    const baseDelay = Math.min(
      this.reconnect.maxDelayMs,
      this.reconnect.initialDelayMs * 2 ** (this.attempts - 1),
    );
    const jitter = baseDelay * this.reconnect.jitter * (Math.random() * 2 - 1);
    const delay = Math.min(this.reconnect.maxDelayMs, Math.max(0, Math.round(baseDelay + jitter)));
    const generation = this.generation;
    this.reconnectTimer = setTimeout(() => {
      if (!this.isCurrent(generation)) return;
      this.reconnectTimer = undefined;
      void this.openSocket(true);
    }, delay);
    // Publish only after installing the timer so listener-driven close can cancel it.
    this.setState("reconnecting");
  }

  private send(payload: Record<string, unknown>): void {
    if (this.currentState !== "connected" || this.socket?.readyState !== OPEN) {
      throw new SamsarixConnectionError("Room session is not connected");
    }
    const generation = this.generation;
    try {
      this.socket.send(JSON.stringify(payload));
    } catch (error) {
      const failure = asConnectionError(error);
      this.failAttempt(generation, failure, 4000);
      // Delivery is ambiguous. Recover the connection, never replay a write.
      throw failure;
    }
  }

  private setState(state: ConnectionState): void {
    if (this.currentState === state) {
      return;
    }
    this.currentState = state;
    const generation = this.generation;
    for (const listener of this.stateListeners) {
      if (this.currentState !== state || this.generation !== generation) return;
      this.notifyListener(() => listener(state));
    }
  }

  private notifyListener(notify: () => void): void {
    try {
      notify();
    } catch (error) {
      try {
        this.onListenerError(error);
      } catch {
        // A reporting hook must not break the connection state machine.
      }
    }
  }

  private rejectPending(error: Error): void {
    this.rejectConnect?.(error);
    this.clearPending();
  }

  private clearPending(): void {
    this.connectPromise = undefined;
    this.resolveConnect = undefined;
    this.rejectConnect = undefined;
  }
}

function isRoomEvent(value: unknown): value is RoomEvent {
  if (!isRecord(value) || typeof value.type !== "string") {
    return false;
  }
  if (!EVENT_TYPES.has(value.type)) {
    return false;
  }
  switch (value.type) {
    case "auth.required":
      return isStringField(value, "message") && (!("example" in value) || isRecord(value.example));
    case "error":
      return isStringField(value, "code") && isStringField(value, "message");
    case "history":
      return (
        "items" in value &&
        Array.isArray(value.items) &&
        value.items.every(isChatMessage) &&
        isNullableStringField(value, "next_before")
      );
    case "member.banned":
      return isStringField(value, "subject") && isStringField(value, "banned_until");
    case "message.created":
      return (
        "message" in value &&
        isChatMessage(value.message) &&
        (!("idempotent_replay" in value) || typeof value.idempotent_replay === "boolean")
      );
    case "message.deleted":
    case "message.updated":
      return "message" in value && isChatMessage(value.message);
    case "message.pin.updated":
      return (
        "message" in value &&
        isChatMessage(value.message) &&
        isStringField(value, "pinner") &&
        typeof value.pinned === "boolean" &&
        typeof value.changed === "boolean" &&
        isStringField(value, "updated_at")
      );
    case "message.reaction.updated":
      return (
        "message" in value &&
        isChatMessage(value.message) &&
        isStringField(value, "key") &&
        isStringField(value, "reactor") &&
        typeof value.present === "boolean" &&
        typeof value.changed === "boolean" &&
        isStringField(value, "updated_at")
      );
    case "pong":
      return true;
    case "read.updated":
      return "receipt" in value && isReadReceipt(value.receipt);
    case "sync.completed":
      return (
        value.strategy === "snapshot" &&
        isNonNegativeIntegerField(value, "history_count") &&
        isNullableStringField(value, "next_before")
      );
    case "presence.joined":
    case "presence.left":
      return isStringField(value, "username") && isNonNegativeIntegerField(value, "active_connections");
    case "ready":
      return (
        "room" in value &&
        isRoom(value.room) &&
        isStringField(value, "username") &&
        isNonNegativeIntegerField(value, "active_connections") &&
        "max_message_chars" in value &&
        typeof value.max_message_chars === "number" &&
        Number.isInteger(value.max_message_chars) &&
        value.max_message_chars > 0 &&
        (!("capabilities" in value) ||
          (Array.isArray(value.capabilities) && value.capabilities.every((capability) => typeof capability === "string")))
      );
    case "room.archived":
    case "room.frozen":
    case "room.unfrozen":
      return "room" in value && isRoom(value.room);
    case "typing.started":
      return (
        isStringField(value, "username") &&
        "expires_in" in value &&
        typeof value.expires_in === "number" &&
        Number.isFinite(value.expires_in) &&
        value.expires_in > 0
      );
    case "typing.stopped":
      return isStringField(value, "username");
  }
  return false;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function isStringField(value: Record<string, unknown>, field: string): boolean {
  return typeof value[field] === "string";
}

function isNullableStringField(value: Record<string, unknown>, field: string): boolean {
  return value[field] === null || typeof value[field] === "string";
}

function isNonNegativeIntegerField(value: Record<string, unknown>, field: string): boolean {
  const fieldValue = value[field];
  return typeof fieldValue === "number" && Number.isInteger(fieldValue) && fieldValue >= 0;
}

function isRoom(value: unknown): boolean {
  return (
    isRecord(value) &&
    isStringField(value, "id") &&
    isStringField(value, "name") &&
    isStringField(value, "description") &&
    isStringField(value, "created_at") &&
    isNullableStringField(value, "archived_at") &&
    isNullableStringField(value, "frozen_at")
  );
}

function isReadReceipt(value: unknown): boolean {
  if (!isRecord(value)) return false;
  const subject = value.subject;
  if (
    typeof subject !== "string" ||
    subject.length === 0 ||
    subject.length > 64 ||
    subject !== subject.trim() ||
    !isNullableStringField(value, "last_read_message_id") ||
    !isNullableStringField(value, "last_read_message_at") ||
    !isNullableStringField(value, "last_read_at")
  ) {
    return false;
  }
  if (
    typeof value.last_read_message_id === "string" &&
    (value.last_read_message_id.length === 0 ||
      value.last_read_message_id.length > 128 ||
      value.last_read_message_at === null ||
      value.last_read_at === null)
  ) {
    return false;
  }
  if ((value.last_read_message_id === null) !== (value.last_read_message_at === null)) return false;
  return true;
}

function isChatMessage(value: unknown): boolean {
  return (
    isRecord(value) &&
    isStringField(value, "id") &&
    isStringField(value, "room_id") &&
    isStringField(value, "sender") &&
    isStringField(value, "content") &&
    isStringField(value, "created_at") &&
    isNullableStringField(value, "client_message_id") &&
    (!("parent_message_id" in value) || isNullableStringField(value, "parent_message_id")) &&
    (!("reactions" in value) ||
      (Array.isArray(value.reactions) &&
        value.reactions.every(
          (item) =>
            isRecord(item) &&
            isStringField(item, "key") &&
            "count" in item &&
            typeof item.count === "number" &&
            Number.isInteger(item.count) &&
            item.count > 0,
        ))) &&
    (!("pinned_at" in value) || isNullableStringField(value, "pinned_at")) &&
    (!("pinned_by" in value) || isNullableStringField(value, "pinned_by")) &&
    (!("metadata" in value) || isMessageMetadata(value.metadata)) &&
    (!("attachments" in value) || isAttachmentReferences(value.attachments)) &&
    (!("mentioned_subjects" in value) || isMentionedSubjects(value.mentioned_subjects)) &&
    isNullableStringField(value, "edited_at") &&
    isNullableStringField(value, "deleted_at")
  );
}

function websocketUrl(baseUrl: string, roomId: string, credential: Credential, username?: string): string {
  const url = new URL(baseUrl);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  url.pathname = `${url.pathname.replace(/\/$/, "")}/v1/rooms/${encodeURIComponent(roomId)}/ws`;
  if ("apiKey" in credential) {
    if (username === undefined || username.trim().length === 0 || username.length > 64) {
      throw new TypeError("username must be 1 to 64 non-blank characters for API-key WebSocket sessions");
    }
    url.searchParams.set("username", username);
  }
  return url.href;
}

function defaultWebSocketFactory(url: string): WebSocketLike {
  if (globalThis.WebSocket === undefined) {
    throw new TypeError("A WebSocket implementation is required");
  }
  return new globalThis.WebSocket(url) as unknown as WebSocketLike;
}

function reconnectPolicy(options: RoomSessionOptions["reconnect"]): ReconnectPolicy {
  const policy = {
    enabled: options?.enabled ?? true,
    initialDelayMs: options?.initialDelayMs ?? 250,
    maxDelayMs: options?.maxDelayMs ?? 5_000,
    maxAttempts: options?.maxAttempts ?? 8,
    jitter: options?.jitter ?? 0.2,
    stableConnectionMs: boundedDuration(options?.stableConnectionMs ?? 10_000, "stableConnectionMs"),
  };
  if (typeof policy.enabled !== "boolean") throw new TypeError("reconnect.enabled must be a boolean");
  if (!Number.isInteger(policy.initialDelayMs) || policy.initialDelayMs < 0 || policy.initialDelayMs > MAX_TIMER_MS) {
    throw new RangeError(`initialDelayMs must be an integer between 0 and ${MAX_TIMER_MS}`);
  }
  if (!Number.isInteger(policy.maxDelayMs) || policy.maxDelayMs < policy.initialDelayMs || policy.maxDelayMs > MAX_TIMER_MS) {
    throw new RangeError(`maxDelayMs must be an integer at least initialDelayMs and no greater than ${MAX_TIMER_MS}`);
  }
  if (!Number.isInteger(policy.maxAttempts) || policy.maxAttempts < 0 || policy.maxAttempts > 100) {
    throw new RangeError("maxAttempts must be an integer between 0 and 100");
  }
  if (!Number.isFinite(policy.jitter) || policy.jitter < 0 || policy.jitter > 1) {
    throw new RangeError("jitter must be between 0 and 1");
  }
  return policy;
}

function boundedDuration(value: number, name: string): number {
  if (!Number.isInteger(value) || value < 1 || value > 300_000) {
    throw new RangeError(`${name} must be an integer between 1 and 300000 milliseconds`);
  }
  return value;
}

function closeTransport(socket: WebSocketLike | undefined, code: number, reason: string): void {
  if (socket !== undefined && socket.readyState <= OPEN) {
    try {
      socket.close(code, reason);
    } catch {
      // Local ownership/timers are already invalidated; a broken injected
      // transport must not revive the session or prevent promise settlement.
    }
  }
}

function asConnectionError(error: unknown): SamsarixConnectionError {
  return error instanceof SamsarixConnectionError
    ? error
    : new SamsarixConnectionError(error instanceof Error ? error.message : "WebSocket connection failed");
}
