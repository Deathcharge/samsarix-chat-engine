// Copyright (c) 2026 Samsarix LLC
// SPDX-License-Identifier: MPL-2.0

import { SamsarixApiError } from "./errors.js";
import { normalizeMessageMetadata } from "./metadata.js";
import { RoomSession } from "./room-session.js";
import type {
  ChatMessage,
  Credential,
  CredentialProvider,
  MemberModeration,
  MemberModerationUpdate,
  MessageCreate,
  MessagePage,
  PinMutation,
  ReadState,
  ReactionMutation,
  Room,
  RoomCreate,
  RoomSessionOptions,
  RoomUpdate,
  WebSocketFactory,
} from "./types.js";

export interface SamsarixClientOptions {
  baseUrl: string;
  credential: CredentialProvider;
  fetch?: typeof globalThis.fetch;
  webSocketFactory?: WebSocketFactory;
}

interface ErrorEnvelope {
  error?: { code?: unknown; message?: unknown; details?: unknown };
}

export class SamsarixChatClient {
  readonly baseUrl: string;
  readonly webSocketFactory?: WebSocketFactory;
  private readonly credentialProvider: CredentialProvider;
  private readonly fetchImplementation: typeof globalThis.fetch;
  private pendingCredential: Promise<Credential> | undefined;

  constructor(options: SamsarixClientOptions) {
    this.baseUrl = normalizeBaseUrl(options.baseUrl);
    this.credentialProvider = options.credential;
    const fetchImplementation = options.fetch ?? globalThis.fetch;
    if (fetchImplementation === undefined) {
      throw new TypeError("A Fetch API implementation is required");
    }
    this.fetchImplementation = fetchImplementation;
    if (options.webSocketFactory !== undefined) {
      this.webSocketFactory = options.webSocketFactory;
    }
  }

  async credential(): Promise<Credential> {
    if (typeof this.credentialProvider !== "function") {
      validateCredential(this.credentialProvider);
      return this.credentialProvider;
    }
    if (this.pendingCredential === undefined) {
      const provider = this.credentialProvider;
      this.pendingCredential = Promise.resolve()
        .then(() => provider())
        .then((value) => {
          validateCredential(value);
          return value;
        })
        .finally(() => {
          this.pendingCredential = undefined;
        });
    }
    return this.pendingCredential;
  }

  async createRoom(payload: RoomCreate): Promise<Room> {
    return this.request<Room>("/v1/rooms", { method: "POST", body: payload });
  }

  async listRooms(limit = 100): Promise<Room[]> {
    return this.request<Room[]>(`/v1/rooms?limit=${boundedInteger(limit, 1, 100, "limit")}`);
  }

  async getRoom(roomId: string): Promise<Room> {
    return this.request<Room>(`/v1/rooms/${encodeURIComponent(roomId)}`);
  }

  async updateRoom(roomId: string, payload: RoomUpdate): Promise<Room> {
    return this.request<Room>(`/v1/rooms/${encodeURIComponent(roomId)}`, { method: "PATCH", body: payload });
  }

  async deleteRoom(roomId: string): Promise<void> {
    await this.request<void>(`/v1/rooms/${encodeURIComponent(roomId)}`, {
      method: "DELETE",
      headers: { "X-Confirm-Room-Delete": roomId },
    });
  }

  async exportRoom(roomId: string): Promise<Response> {
    const credential = await this.credential();
    const headers = new Headers({ Accept: "application/x-ndjson" });
    this.applyAuthHeaders(headers, credential);
    const response = await this.fetchImplementation(
      `${this.baseUrl}/v1/rooms/${encodeURIComponent(roomId)}/export`,
      { headers },
    );
    if (!response.ok) {
      throw await apiError(response);
    }
    return response;
  }

  async listMessages(roomId: string, options: { limit?: number; before?: string } = {}): Promise<MessagePage> {
    const query = new URLSearchParams();
    query.set("limit", String(boundedInteger(options.limit ?? 50, 1, 100, "limit")));
    if (options.before !== undefined) {
      query.set("before", options.before);
    }
    return this.request<MessagePage>(`/v1/rooms/${encodeURIComponent(roomId)}/messages?${query}`);
  }

  async listPinnedMessages(
    roomId: string,
    options: { limit?: number; before?: string } = {},
  ): Promise<MessagePage> {
    const query = new URLSearchParams();
    query.set("limit", String(boundedInteger(options.limit ?? 50, 1, 100, "limit")));
    if (options.before !== undefined) {
      query.set("before", options.before);
    }
    return this.request<MessagePage>(`/v1/rooms/${encodeURIComponent(roomId)}/messages/pins?${query}`);
  }

  async listReplies(
    roomId: string,
    parentMessageId: string,
    options: { limit?: number; before?: string } = {},
  ): Promise<MessagePage> {
    const query = new URLSearchParams();
    query.set("limit", String(boundedInteger(options.limit ?? 50, 1, 100, "limit")));
    if (options.before !== undefined) {
      query.set("before", options.before);
    }
    return this.request<MessagePage>(
      `/v1/rooms/${encodeURIComponent(roomId)}/messages/${encodeURIComponent(parentMessageId)}/replies?${query}`,
    );
  }

  async searchMessages(
    roomId: string,
    search: string,
    options: { limit?: number; before?: string } = {},
  ): Promise<MessagePage> {
    const normalized = search.trim().normalize("NFKC");
    if ([...normalized].length < 2) {
      throw new RangeError("search must contain at least 2 NFKC-normalized characters");
    }
    const query = new URLSearchParams({
      q: normalized,
      limit: String(boundedInteger(options.limit ?? 50, 1, 100, "limit")),
    });
    if (options.before !== undefined) {
      query.set("before", options.before);
    }
    return this.request<MessagePage>(`/v1/rooms/${encodeURIComponent(roomId)}/messages/search?${query}`);
  }

  async createMessage(roomId: string, payload: MessageCreate, idempotencyKey?: string): Promise<ChatMessage> {
    const body =
      payload.metadata === undefined
        ? payload
        : { ...payload, metadata: normalizeMessageMetadata(payload.metadata) };
    return this.request<ChatMessage>(`/v1/rooms/${encodeURIComponent(roomId)}/messages`, {
      method: "POST",
      body,
      ...(idempotencyKey === undefined ? {} : { headers: { "Idempotency-Key": idempotencyKey } }),
    });
  }

  async getReadState(roomId: string): Promise<ReadState> {
    return this.request<ReadState>(`/v1/rooms/${encodeURIComponent(roomId)}/read-state`);
  }

  async markRead(roomId: string, messageId?: string): Promise<ReadState> {
    return this.request<ReadState>(`/v1/rooms/${encodeURIComponent(roomId)}/read-state`, {
      method: "PUT",
      body: messageId === undefined ? {} : { message_id: messageId },
    });
  }

  async clearReadState(roomId: string): Promise<void> {
    await this.request<void>(`/v1/rooms/${encodeURIComponent(roomId)}/read-state`, { method: "DELETE" });
  }

  async updateMessage(
    roomId: string,
    messageId: string,
    content: string,
    metadata?: import("./types.js").MessageMetadata,
  ): Promise<ChatMessage> {
    return this.request<ChatMessage>(
      `/v1/rooms/${encodeURIComponent(roomId)}/messages/${encodeURIComponent(messageId)}`,
      {
        method: "PATCH",
        body: {
          content,
          ...(metadata === undefined ? {} : { metadata: normalizeMessageMetadata(metadata) }),
        },
      },
    );
  }

  async deleteMessage(roomId: string, messageId: string): Promise<void> {
    await this.request<void>(
      `/v1/rooms/${encodeURIComponent(roomId)}/messages/${encodeURIComponent(messageId)}`,
      { method: "DELETE" },
    );
  }

  async addReaction(roomId: string, messageId: string, key: string, reactor?: string): Promise<ReactionMutation> {
    return this.mutateReaction("PUT", roomId, messageId, key, reactor);
  }

  async removeReaction(roomId: string, messageId: string, key: string, reactor?: string): Promise<ReactionMutation> {
    return this.mutateReaction("DELETE", roomId, messageId, key, reactor);
  }

  async pinMessage(roomId: string, messageId: string, pinner?: string): Promise<PinMutation> {
    return this.mutatePin("PUT", roomId, messageId, pinner);
  }

  async unpinMessage(roomId: string, messageId: string, pinner?: string): Promise<PinMutation> {
    return this.mutatePin("DELETE", roomId, messageId, pinner);
  }

  async updateMemberModeration(
    roomId: string,
    subject: string,
    payload: MemberModerationUpdate,
  ): Promise<MemberModeration> {
    return this.request<MemberModeration>(
      `/v1/rooms/${encodeURIComponent(roomId)}/members/${encodeURIComponent(subject)}/moderation`,
      { method: "PATCH", body: payload },
    );
  }

  roomSession(roomId: string, options: RoomSessionOptions = {}): RoomSession {
    return new RoomSession(this, roomId, options);
  }

  private async mutateReaction(
    method: "PUT" | "DELETE",
    roomId: string,
    messageId: string,
    key: string,
    reactor?: string,
  ): Promise<ReactionMutation> {
    if (!/^[a-z0-9][a-z0-9_+\-]{0,29}$/.test(key)) {
      throw new RangeError("reaction key must be 1-30 lowercase ASCII key characters");
    }
    if (reactor !== undefined && (reactor.trim().length === 0 || reactor.length > 64)) {
      throw new RangeError("reactor must be between 1 and 64 non-blank characters");
    }
    return this.request<ReactionMutation>(
      `/v1/rooms/${encodeURIComponent(roomId)}/messages/${encodeURIComponent(messageId)}/reactions/${encodeURIComponent(key)}`,
      { method, body: reactor === undefined ? {} : { reactor } },
    );
  }

  private async mutatePin(
    method: "PUT" | "DELETE",
    roomId: string,
    messageId: string,
    pinner?: string,
  ): Promise<PinMutation> {
    if (pinner !== undefined && (pinner.trim().length === 0 || pinner.length > 64)) {
      throw new RangeError("pinner must be between 1 and 64 non-blank characters");
    }
    return this.request<PinMutation>(
      `/v1/rooms/${encodeURIComponent(roomId)}/messages/${encodeURIComponent(messageId)}/pin`,
      { method, body: pinner === undefined ? {} : { pinner } },
    );
  }

  private async request<T>(
    path: string,
    options: { method?: string; body?: unknown; headers?: Record<string, string> } = {},
  ): Promise<T> {
    const credential = await this.credential();
    const headers = new Headers(options.headers);
    headers.set("Accept", "application/json");
    this.applyAuthHeaders(headers, credential);
    let body: string | undefined;
    if (options.body !== undefined) {
      headers.set("Content-Type", "application/json");
      body = JSON.stringify(options.body);
    }
    const response = await this.fetchImplementation(`${this.baseUrl}${path}`, {
      method: options.method ?? "GET",
      headers,
      ...(body === undefined ? {} : { body }),
    });
    if (!response.ok) {
      throw await apiError(response);
    }
    if (response.status === 204) {
      return undefined as T;
    }
    return (await response.json()) as T;
  }

  private applyAuthHeaders(headers: Headers, credential: Credential): void {
    if ("token" in credential) {
      headers.set("Authorization", `Bearer ${credential.token}`);
    } else {
      headers.set("X-API-Key", credential.apiKey);
    }
  }
}

function normalizeBaseUrl(value: string): string {
  const url = new URL(value);
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw new TypeError("baseUrl must use http or https");
  }
  if (url.username || url.password || url.search || url.hash) {
    throw new TypeError("baseUrl must not contain credentials, a query, or a fragment");
  }
  return url.href.replace(/\/$/, "");
}

function validateCredential(value: Credential): void {
  if (typeof value !== "object" || value === null || ("token" in value) === ("apiKey" in value)) {
    throw new TypeError("credential must contain exactly one of token or apiKey");
  }
  if ("token" in value) {
    if (typeof value.token !== "string" || value.token.length === 0) {
      throw new TypeError("token must be a non-empty string");
    }
  } else if (typeof value.apiKey !== "string" || value.apiKey.length === 0) {
    throw new TypeError("apiKey must be a non-empty string");
  }
}

function boundedInteger(value: number, minimum: number, maximum: number, name: string): number {
  if (!Number.isInteger(value) || value < minimum || value > maximum) {
    throw new RangeError(`${name} must be an integer between ${minimum} and ${maximum}`);
  }
  return value;
}

async function apiError(response: Response): Promise<SamsarixApiError> {
  let envelope: ErrorEnvelope = {};
  try {
    envelope = (await response.json()) as ErrorEnvelope;
  } catch {
    // Preserve the status even when an intermediary returned a non-JSON body.
  }
  const code = typeof envelope.error?.code === "string" ? envelope.error.code : "http_error";
  const message = typeof envelope.error?.message === "string" ? envelope.error.message : `HTTP ${response.status}`;
  return new SamsarixApiError(response.status, code, message, envelope.error?.details);
}
