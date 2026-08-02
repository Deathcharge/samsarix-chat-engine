// Copyright (c) 2026 Samsarix LLC
// SPDX-License-Identifier: MPL-2.0

import { SamsarixConnectionError } from "./errors.js";
import type { SamsarixChatClient } from "./client.js";
import type {
  ConnectionState,
  Credential,
  RoomEvent,
  RoomSessionOptions,
  WebSocketCloseEventLike,
  WebSocketFactory,
  WebSocketLike,
} from "./types.js";

const OPEN = 1;
const TERMINAL_CLOSE_CODES = new Set([1000, 1002, 1008, 4401, 4403, 4404, 4409]);
const EVENT_TYPES = new Set([
  "auth.required",
  "error",
  "history",
  "member.banned",
  "message.created",
  "message.deleted",
  "message.updated",
  "pong",
  "presence.joined",
  "presence.left",
  "ready",
  "room.archived",
  "room.frozen",
  "room.unfrozen",
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
}

export class RoomSession {
  readonly roomId: string;
  private readonly client: SamsarixChatClient;
  private readonly username?: string;
  private readonly reconnect: ReconnectPolicy;
  private readonly onListenerError: (error: unknown) => void;
  private readonly eventListeners = new Set<EventListener>();
  private readonly stateListeners = new Set<StateListener>();
  private socket: WebSocketLike | undefined;
  private connectPromise: Promise<void> | undefined;
  private resolveConnect: (() => void) | undefined;
  private rejectConnect: ((error: Error) => void) | undefined;
  private reconnectTimer: ReturnType<typeof setTimeout> | undefined;
  private generation = 0;
  private attempts = 0;
  private manuallyClosed = false;
  private currentState: ConnectionState = "idle";
  private maxMessageChars: number | undefined;

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
    this.onListenerError = options.onListenerError ?? (() => undefined);
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
    this.connectPromise = new Promise<void>((resolve, reject) => {
      this.resolveConnect = resolve;
      this.rejectConnect = reject;
    });
    if (this.currentState !== "reconnecting") {
      this.attempts = 0;
      void this.openSocket(false);
    }
    return this.connectPromise;
  }

  close(code = 1000, reason = "Client closed"): void {
    this.manuallyClosed = true;
    this.generation += 1;
    if (this.reconnectTimer !== undefined) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }
    const error = new SamsarixConnectionError(reason, code);
    this.rejectPending(error);
    const socket = this.socket;
    this.socket = undefined;
    if (socket !== undefined && socket.readyState <= OPEN) {
      socket.close(code, reason);
    }
    this.setState("closed");
  }

  sendMessage(content: string, clientMessageId?: string): void {
    if (content.trim().length === 0) {
      throw new TypeError("content must not be blank");
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
    this.setState(reconnecting ? "reconnecting" : "connecting");
    try {
      const credential = await this.client.credential();
      if (generation !== this.generation || this.manuallyClosed) {
        return;
      }
      let url: string;
      try {
        url = websocketUrl(this.client.baseUrl, this.roomId, credential, this.username);
      } catch (error) {
        this.rejectPending(asConnectionError(error));
        this.setState("closed");
        return;
      }
      const factory = this.client.webSocketFactory ?? defaultWebSocketFactory;
      const socket = factory(url);
      this.socket = socket;
      socket.onopen = () => undefined;
      socket.onmessage = (event) => {
        if (generation === this.generation) {
          this.handleMessage(socket, credential, event.data);
        }
      };
      socket.onerror = () => {
        if (generation === this.generation && this.currentState !== "connected") {
          this.rejectPending(new SamsarixConnectionError("WebSocket connection failed"));
        }
      };
      socket.onclose = (event) => {
        if (generation === this.generation) {
          this.handleClose(event);
        }
      };
    } catch (error) {
      if (generation !== this.generation) {
        return;
      }
      const connectionError = asConnectionError(error);
      this.rejectPending(connectionError);
      this.scheduleReconnect(1006);
    }
  }

  private handleMessage(socket: WebSocketLike, credential: Credential, data: unknown): void {
    if (typeof data !== "string") {
      socket.close(1002, "JSON text events required");
      return;
    }
    let parsed: unknown;
    try {
      parsed = JSON.parse(data);
    } catch {
      socket.close(1002, "Invalid JSON event");
      return;
    }
    if (!isRoomEvent(parsed)) {
      socket.close(1002, "Invalid event envelope");
      return;
    }
    const event = parsed;
    if (event.type === "auth.required") {
      socket.send(
        JSON.stringify("token" in credential ? { type: "auth", token: credential.token } : { type: "auth", api_key: credential.apiKey }),
      );
    } else if (event.type === "ready") {
      this.attempts = 0;
      this.maxMessageChars = event.max_message_chars;
      this.setState("connected");
      this.resolveConnect?.();
      this.clearPending();
    }
    for (const listener of this.eventListeners) {
      this.notifyListener(() => listener(event));
    }
  }

  private handleClose(event: WebSocketCloseEventLike): void {
    this.socket = undefined;
    if (this.manuallyClosed) {
      this.setState("closed");
      return;
    }
    if (this.currentState !== "connected") {
      this.rejectPending(new SamsarixConnectionError(event.reason || "WebSocket closed before ready", event.code));
    }
    if (TERMINAL_CLOSE_CODES.has(event.code)) {
      this.setState("closed");
      return;
    }
    this.scheduleReconnect(event.code);
  }

  private scheduleReconnect(closeCode: number): void {
    if (!this.reconnect.enabled || this.attempts >= this.reconnect.maxAttempts || TERMINAL_CLOSE_CODES.has(closeCode)) {
      this.setState("closed");
      return;
    }
    this.attempts += 1;
    const baseDelay = Math.min(
      this.reconnect.maxDelayMs,
      this.reconnect.initialDelayMs * 2 ** (this.attempts - 1),
    );
    const jitter = baseDelay * this.reconnect.jitter * (Math.random() * 2 - 1);
    const delay = Math.max(0, Math.round(baseDelay + jitter));
    this.setState("reconnecting");
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      void this.openSocket(true);
    }, delay);
  }

  private send(payload: Record<string, unknown>): void {
    if (this.currentState !== "connected" || this.socket?.readyState !== OPEN) {
      throw new SamsarixConnectionError("Room session is not connected");
    }
    this.socket.send(JSON.stringify(payload));
  }

  private setState(state: ConnectionState): void {
    if (this.currentState === state) {
      return;
    }
    this.currentState = state;
    for (const listener of this.stateListeners) {
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
    case "pong":
      return true;
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
        value.max_message_chars > 0
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

function isChatMessage(value: unknown): boolean {
  return (
    isRecord(value) &&
    isStringField(value, "id") &&
    isStringField(value, "room_id") &&
    isStringField(value, "sender") &&
    isStringField(value, "content") &&
    isStringField(value, "created_at") &&
    isNullableStringField(value, "client_message_id") &&
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
  };
  if (!Number.isFinite(policy.initialDelayMs) || policy.initialDelayMs < 0) {
    throw new RangeError("initialDelayMs must be a non-negative finite number");
  }
  if (!Number.isFinite(policy.maxDelayMs) || policy.maxDelayMs < policy.initialDelayMs) {
    throw new RangeError("maxDelayMs must be finite and at least initialDelayMs");
  }
  if (!Number.isInteger(policy.maxAttempts) || policy.maxAttempts < 0 || policy.maxAttempts > 100) {
    throw new RangeError("maxAttempts must be an integer between 0 and 100");
  }
  if (!Number.isFinite(policy.jitter) || policy.jitter < 0 || policy.jitter > 1) {
    throw new RangeError("jitter must be between 0 and 1");
  }
  return policy;
}

function asConnectionError(error: unknown): SamsarixConnectionError {
  return error instanceof SamsarixConnectionError
    ? error
    : new SamsarixConnectionError(error instanceof Error ? error.message : "WebSocket connection failed");
}
