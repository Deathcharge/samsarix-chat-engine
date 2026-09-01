// Copyright (c) 2026 Samsarix LLC
// SPDX-License-Identifier: MPL-2.0

export type TokenCredential = { token: string; apiKey?: never };
export type ApiKeyCredential = { apiKey: string; token?: never };
export type Credential = TokenCredential | ApiKeyCredential;
export type CredentialProvider = Credential | (() => Credential | Promise<Credential>);
export type MessageMetadataValue = string | number | boolean | null;
export type MessageMetadata = Record<string, MessageMetadataValue>;

export interface Room {
  id: string;
  name: string;
  description: string;
  created_at: string;
  archived_at: string | null;
  frozen_at: string | null;
}

export interface RoomCreate {
  id?: string;
  name: string;
  description?: string;
}

export interface RoomUpdate {
  archived?: boolean;
  frozen?: boolean;
}

export interface ChatMessage {
  id: string;
  room_id: string;
  sender: string;
  content: string;
  created_at: string;
  client_message_id: string | null;
  /** Present on servers with threaded replies; released 0.12 servers omit it. */
  parent_message_id?: string | null;
  /** Present on servers with message reactions; released 0.12 servers omit it. */
  reactions?: ReactionSummary[];
  /** Present on servers with shared room pins; released 0.12 servers omit it. */
  pinned_at?: string | null;
  /** Stable subject or operator-supplied actor that created the current pin. */
  pinned_by?: string | null;
  /** Bounded application context; released 0.12 servers omit it. */
  metadata?: MessageMetadata;
  edited_at: string | null;
  deleted_at: string | null;
}

export interface ReactionSummary {
  key: string;
  count: number;
}

export interface ReactionMutation {
  message: ChatMessage;
  key: string;
  reactor: string;
  present: boolean;
  changed: boolean;
  updated_at: string;
}

export interface PinMutation {
  message: ChatMessage;
  pinner: string;
  pinned: boolean;
  changed: boolean;
  updated_at: string;
}

export interface MessageCreate {
  sender?: string;
  content: string;
  client_message_id?: string;
  parent_message_id?: string;
  metadata?: MessageMetadata;
}

export interface MessagePage {
  items: ChatMessage[];
  next_before: string | null;
}

export interface ReadState {
  room_id: string;
  subject: string;
  last_read_message_id: string | null;
  last_read_at: string | null;
  unread_count: number;
}

export interface MemberModerationUpdate {
  muted_for_seconds?: number;
  banned_for_seconds?: number;
}

export interface MemberModeration {
  room_id: string;
  subject: string;
  muted_until: string | null;
  banned_until: string | null;
  updated_at: string;
}

export interface ReadyEvent {
  type: "ready";
  room: Room;
  username: string;
  active_connections: number;
  max_message_chars: number;
}

export interface HistoryEvent {
  type: "history";
  items: ChatMessage[];
  next_before: string | null;
}

export interface MessageCreatedEvent {
  type: "message.created";
  message: ChatMessage;
  idempotent_replay?: boolean;
}

export interface MessageUpdatedEvent {
  type: "message.updated";
  message: ChatMessage;
}

export interface MessageDeletedEvent {
  type: "message.deleted";
  message: ChatMessage;
}

export interface MessageReactionUpdatedEvent extends ReactionMutation {
  type: "message.reaction.updated";
}

export interface MessagePinUpdatedEvent extends PinMutation {
  type: "message.pin.updated";
}

export interface PresenceEvent {
  type: "presence.joined" | "presence.left";
  username: string;
  active_connections: number;
}

export interface RoomStateEvent {
  type: "room.archived" | "room.frozen" | "room.unfrozen";
  room: Room;
}

export interface MemberBannedEvent {
  type: "member.banned";
  subject: string;
  banned_until: string;
}

export interface PongEvent {
  type: "pong";
}

export interface TypingStartedEvent {
  type: "typing.started";
  username: string;
  expires_in: number;
}

export interface TypingStoppedEvent {
  type: "typing.stopped";
  username: string;
}

export interface ErrorEvent {
  type: "error";
  code: string;
  message: string;
}

export interface AuthRequiredEvent {
  type: "auth.required";
  message: string;
  example?: Record<string, unknown>;
}

export type RoomEvent =
  | ReadyEvent
  | HistoryEvent
  | MessageCreatedEvent
  | MessageUpdatedEvent
  | MessageDeletedEvent
  | MessagePinUpdatedEvent
  | MessageReactionUpdatedEvent
  | PresenceEvent
  | RoomStateEvent
  | MemberBannedEvent
  | PongEvent
  | TypingStartedEvent
  | TypingStoppedEvent
  | ErrorEvent
  | AuthRequiredEvent;

export type ConnectionState = "idle" | "connecting" | "connected" | "reconnecting" | "closed";

export interface ReconnectOptions {
  enabled?: boolean;
  initialDelayMs?: number;
  maxDelayMs?: number;
  maxAttempts?: number;
  jitter?: number;
  /** Reset the retry budget only after an activated connection stays open this long. Default 10000 ms. */
  stableConnectionMs?: number;
}

export interface RoomSessionOptions {
  username?: string;
  /** Deadline per attempt, including credentials, ready/history and activation pong. Default 10000 ms. */
  handshakeTimeoutMs?: number;
  reconnect?: ReconnectOptions;
  onListenerError?: (error: unknown) => void;
}

export interface WebSocketMessageEventLike {
  data: unknown;
}

export interface WebSocketCloseEventLike {
  code: number;
  reason: string;
}

export interface WebSocketLike {
  readonly readyState: number;
  onopen: (() => void) | null;
  onmessage: ((event: WebSocketMessageEventLike) => void) | null;
  onclose: ((event: WebSocketCloseEventLike) => void) | null;
  onerror: (() => void) | null;
  send(data: string): void;
  close(code?: number, reason?: string): void;
}

export type WebSocketFactory = (url: string) => WebSocketLike;
