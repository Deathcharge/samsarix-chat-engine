# Application workflows

Samsarix Chat Engine 0.10 provides the small amount of application state needed to build a credible one-to-one or small-team support inbox: per-user read cursors, current unread counts, ephemeral typing signals, and authorized per-case message retrieval. The host application still owns accounts, customer records, assignment, notifications, and the user interface.

## Support-room journey

A practical integration creates one private room per support case, issues short-lived room tokens to the customer and assigned agents, and stores the case-to-room mapping in the host application's database. A customer can post and disconnect; an agent later sees the unread count, reads through a message, replies, and marks the latest reply as read.

```ts
import { SamsarixChatClient } from "@samsarix/chat-client";

const chat = new SamsarixChatClient({
  baseUrl: "https://chat.example.com",
  credential: async () => ({ token: await issueSupportRoomToken() }),
});

const before = await chat.getReadState("support-case-42");
const messages = await chat.listMessages("support-case-42");
const paymentContext = await chat.searchMessages("support-case-42", "payment failed");
await chat.markRead("support-case-42", messages.items.at(-1)?.id);

const session = chat.roomSession("support-case-42");
const typingTimers = new Map<string, ReturnType<typeof setTimeout>>();

session.onEvent((event) => {
  if (event.type === "typing.started") {
    clearTimeout(typingTimers.get(event.username));
    showTyping(event.username);
    typingTimers.set(
      event.username,
      setTimeout(() => {
        typingTimers.delete(event.username);
        hideTyping(event.username);
      }, event.expires_in * 1000),
    );
  }
  if (event.type === "typing.stopped") {
    clearTimeout(typingTimers.get(event.username));
    typingTimers.delete(event.username);
    hideTyping(event.username);
  }
});
await session.connect();
session.setTyping(true);
```

The runnable [support workflow example](../examples/03_support_workflow.py) demonstrates the complete HTTP path with separate customer and agent identities. With a server running and an operator key plus signing secret configured, issue two tokens:

```bash
export SAMSARIX_CHAT_CUSTOMER_TOKEN="$(samsarix-chat token issue --subject customer-42 --room support-demo --permission room:read --permission room:write --expires-in 3600)"
export SAMSARIX_CHAT_AGENT_TOKEN="$(samsarix-chat token issue --subject agent-7 --room support-demo --permission room:read --permission room:write --expires-in 3600)"
python examples/03_support_workflow.py
```

On PowerShell, assign the two command results to `$env:SAMSARIX_CHAT_CUSTOMER_TOKEN` and `$env:SAMSARIX_CHAT_AGENT_TOKEN` instead. The example also requires `SAMSARIX_CHAT_API_KEY` so it can create the room.

## Read-state contract

`GET /v1/rooms/{room_id}/read-state` returns the signed subject's cursor and a currently derived unread count. `PUT` accepts `{"message_id":"..."}` or `{}` to advance through a specific message or the latest room position. `DELETE` removes only the caller's stored cursor.

- Read state requires a signed application-user token with `room:read`; the shared operator key and unauthenticated local identity are rejected because they do not identify a stable end user.
- A read cursor is persisted only after an explicit `PUT`. Before then, the response has null cursor fields and counts all non-deleted messages from other senders.
- Cursors are monotonic. A late device cannot move a user backward by submitting an older message.
- Messages whose authenticated author is the same signed subject and deleted-message tombstones do not count as unread. Operator/local display names never impersonate that identity for unread accounting.
- The cursor keeps its chronological position if count- or age-based retention later removes the referenced message.
- Each room is capped by `SAMSARIX_CHAT_MAX_READ_STATES_PER_ROOM`. Users can erase their own row, and deleting a room cascades its read-state rows.
- Read-state changes are intentionally excluded from the administrative audit stream and room-message export: they are high-volume, user-specific interaction metadata rather than administrative actions.

The returned `unread_count` is computed at request time. Samsarix does not push unread-count events or aggregate counts across rooms; clients should refresh after reconnect, message activity, or marking a room read.

## Support-case search contract

Agents and customers use the same `room:read` token boundary as history; a token for one case cannot search another. Search covers only current, retained message content in that room, so an edit removes the old wording and a tombstone removes the body from results. It performs Unicode-normalized substring matching without relevance ranking or highlights, and returns the normal chronological cursor page.

The scan is bounded by `SAMSARIX_CHAT_MAX_STORED_MESSAGES_PER_ROOM` and independently limited by `SAMSARIX_CHAT_SEARCHES_PER_MINUTE`. This is suitable for finding an order number, error phrase, or prior answer inside a bounded support case. Because a GET query can appear in reverse-proxy access logs, operators should govern those logs like room content and users should not search for credentials or secrets. A product needing fuzzy relevance, global discovery, analytics, or very large histories should export authorized events to a separately governed search system instead of treating this endpoint as an index.

## Typing contract

A connected WebSocket client with `room:write` sends `{"type":"typing","active":true}` and later `false`. Other room connections receive `typing.started` with `username` and `expires_in`, then `typing.stopped`.

- Typing signals are never written to SQLite, exported, or added to the audit trail.
- Repeated `active:true` commands refresh the deadline without repeatedly broadcasting `typing.started`.
- The server emits a stop transition after `SAMSARIX_CHAT_TYPING_TIMEOUT` seconds, on a successful message publish, or when the connection closes.
- A separate limiter keyed by signed subject or unauthenticated/operator client address, `SAMSARIX_CHAT_TYPING_EVENTS_PER_MINUTE`, prevents typing traffic from consuming the message-publish allowance.
- Delivery is best-effort and at-most-once. UIs must use the advertised expiry as a backstop and must not infer durable presence, activity history, or message intent from typing signals.

## Host application events

For offline assignment, notifications, CRM synchronization, or case timelines, version 0.9 can send selected committed message/moderation events to one host-application endpoint. The host should verify the signed raw request, durably deduplicate `webhook-id`, enqueue its own work, and acknowledge quickly. It must not assume ordered or exactly-once delivery. See [Reliable application webhooks](WEBHOOKS.md) for the complete sender/receiver and recovery contract.

## Product and privacy boundary

The host application should use opaque, stable internal account IDs as token subjects rather than email addresses. Read timestamps and typing activity can reveal engagement patterns, so expose them only to participants with a legitimate room relationship and document their use in the host product's privacy notice. Samsarix deliberately does not provide per-message read receipts, cross-room user activity, or durable typing history.

This shape follows common chat expectations without copying an entire hosted platform: Stream documents initial and event-driven unread state and channel-filtered search, while Sendbird documents channel-scoped unread counts, typing indicators, message search, and the need to restrict relational user data to relevant channel members. See the [Stream unread guide](https://getstream.io/chat/docs/javascript/unread/), [Stream message search](https://getstream.io/chat/docs/php/search/), [Sendbird channel overview](https://sendbird.com/docs/chat/sdk/v4/javascript/channel/overview-channel), and [Sendbird message-search overview](https://docs.sendbird.com/docs/chat/platform-api/v3/message/message-search/message-search-overview).
