# Application workflows

Samsarix Chat Engine provides the small amount of application state needed to build a credible one-to-one or small-team support inbox: per-user read cursors, bounded cross-room unread snapshots, ephemeral typing signals, and authorized per-case message retrieval. The host application still owns accounts, customer records, assignment, notifications, and the user interface.

## Support-room journey

A practical integration creates one private room per support case, issues short-lived room tokens to the customer and assigned agents, and stores the case-to-room mapping in the host application's database. A customer can post and disconnect; an agent later sees the unread count, reads through a message, adds a contextual one-depth reply or an `ack`/`resolved` reaction, pins the accepted resolution, and marks the latest reply as read.

```ts
import { SamsarixChatClient } from "@samsarix/chat-client";

const chat = new SamsarixChatClient({
  baseUrl: "https://chat.example.com",
  credential: async () => ({ token: await issueSupportRoomToken() }),
});

const inbox = await chat.queryReadStates(["support-case-42", "support-case-43"]);
const participants = await chat.queryReadReceipts("support-case-42", ["customer-42", "agent-7"]);
const before = inbox.items.find((item) => item.room_id === "support-case-42");
await chat.createMessage("support-case-42", {
  content: "Payment failed after the upgrade",
  client_message_id: crypto.randomUUID(),
  metadata: { "ticket.id": "SUP-42", "ticket.channel": "in_product" },
  mentioned_subjects: ["agent-7"],
  attachments: [{
    id: "support-upload-SUP-42-trace",
    name: "payment-trace.txt",
    media_type: "text/plain",
    size_bytes: 1842,
  }],
});
const messages = await chat.listMessages("support-case-42");
const paymentContext = await chat.searchMessages("support-case-42", "payment failed");
const original = paymentContext.items.at(-1);
if (original) {
  await chat.addReaction("support-case-42", original.id, "ack");
  await chat.pinMessage("support-case-42", original.id);
  const existingReplies = await chat.listReplies("support-case-42", original.id);
  console.log("thread", existingReplies.items);
}
await chat.markRead("support-case-42", messages.items.at(-1)?.id);

const session = chat.roomSession("support-case-42");
const typingTimers = new Map<string, ReturnType<typeof setTimeout>>();

session.onEvent((event) => {
  if (event.type === "read.updated") {
    updateParticipantReceipt(event.receipt);
  }
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
replaceParticipantReceipts(
  (await chat.queryReadReceipts("support-case-42", ["customer-42", "agent-7"])).items,
);
session.setTyping(true);
if (original) session.sendReply(original.id, "I am checking that transaction now", crypto.randomUUID());
```

Threads are a presentation aid, not a separate authorization or delivery boundary. Only a non-deleted top-level message can receive new replies, nesting is rejected, and every reply still appears in chronological room history, search, exports, webhooks, and ordinary `message.created` events. Existing replies remain readable if their parent is later tombstoned. If physical count/age retention removes the parent, surviving replies are promoted by clearing their parent ID.

Reactions are low-noise state signals, not replacement messages or workflow authority. Use a small product-owned vocabulary such as `ack`, `resolved`, `needs_attention`, or `helpful`; show the server's grouped counts and replace a message from each `message.reaction.updated` event. The host application still decides whether a reaction should transition a ticket or notify a human. Each actor/key pair is unique, distinct keys are capped at 20 per message, and tombstoning a message removes its reaction actors and counts.

Participant receipts are an optional acknowledgement affordance, not proof of human attention. Supply the small visible participant set from the host's case assignment/membership model and grant `room:read-receipts` only to clients covered by the product's privacy policy. Query the snapshot after every connection or reconnection, then replace one subject from each `read.updated` event. Samsarix does not enumerate members or retain per-message receipt history.

Pins are shared room curation, not private bookmarks or workflow authority. Give `room:pin` only to agents, teachers, moderators, or incident leads that may elevate an accepted answer, runbook, decision, guideline, or announcement for everyone with room read access. Pin mutations also require `room:read`, are metadata-audited, and emit `message.pin.updated`; refresh the newest-first pin list after reconnect or concurrent changes. Tombstoning clears the pin. The host application still decides whether a pinned resolution should close a case or trigger another side effect.

Application message metadata connects the room transcript to host-owned records without making chat storage the workflow database. Use bounded scalar references such as `ticket.id`, `assignment.id`, `incident.severity`, `runbook`, or `action`; keep customer records, access decisions, arbitrary nested data, and executable UI configuration in the host application. Every participant with room read access and every selected message webhook receiver can see the metadata. Treat values as untrusted display/integration data, and use the signed token—not metadata—for authorization. Tombstoning clears the object along with message content.

Attachment references connect a message to host-owned evidence or resources without turning Samsarix into a blob store. Authenticate and scan the upload before creating the message, store only a stable opaque object ID plus bounded display facts, and resolve that ID through a fresh room/user authorization check whenever a client downloads it. Never put a signed URL, storage credential, local path, or authorization decision in the descriptor. Tombstoning clears references from the chat record but does not delete host-owned bytes; the host must handle orphan cleanup, retention, and storage/egress cost. See [Host-owned attachment references](ATTACHMENTS.md) for the complete contract.

Mentions are explicit host-account targets, not parsed display names or proof of membership. A customer can include the assigned agent's opaque subject, or an agent can target an escalation/on-call subject; the host consumes a deduplicated message webhook, re-checks room membership and notification preferences, and performs any email/push/work-queue delivery under its own budgets. Samsarix preserves at most ten targets through the transcript and clears them on tombstone deletion. Do not put email addresses, device tokens, or provider credentials in `mentioned_subjects`. See [Host-resolved message mentions](MENTIONS.md).

The runnable [support workflow example](../examples/03_support_workflow.py) demonstrates the complete HTTP path with separate customer and agent identities. With a server running and an operator key plus signing secret configured, issue two tokens:

```bash
export SAMSARIX_CHAT_CUSTOMER_TOKEN="$(samsarix-chat token issue --subject customer-42 --room support-demo --permission room:read --permission room:write --expires-in 3600)"
export SAMSARIX_CHAT_AGENT_TOKEN="$(samsarix-chat token issue --subject agent-7 --room support-demo --permission room:read --permission room:write --permission room:pin --expires-in 3600)"
python examples/03_support_workflow.py
```

On PowerShell, assign the two command results to `$env:SAMSARIX_CHAT_CUSTOMER_TOKEN` and `$env:SAMSARIX_CHAT_AGENT_TOKEN` instead. The example also requires `SAMSARIX_CHAT_API_KEY` so it can create the room.

## Read-state and inbox-query contract

`GET /v1/rooms/{room_id}/read-state` returns the signed subject's cursor and a currently derived unread count. `PUT` accepts `{"message_id":"..."}` or `{}` to advance through a specific message or the latest room position. `DELETE` removes only the caller's stored cursor.

- Read state requires a signed application-user token with `room:read`; the shared operator key and unauthenticated local identity are rejected because they do not identify a stable end user.
- A read cursor is persisted only after an explicit `PUT`. Before then, the response has null cursor fields and counts all non-deleted messages from other senders.
- Cursors are monotonic. A late device cannot move a user backward by submitting an older message.
- Messages whose authenticated author is the same signed subject and deleted-message tombstones do not count as unread. Operator/local display names never impersonate that identity for unread accounting.
- The cursor keeps its chronological position if count- or age-based retention later removes the referenced message.
- Each room is capped by `SAMSARIX_CHAT_MAX_READ_STATES_PER_ROOM`. Users can erase their own row, and deleting a room cascades its read-state rows.
- Read-state changes are intentionally excluded from the administrative audit stream and room-message export: they are high-volume, user-specific interaction metadata rather than administrative actions.

The returned `unread_count` is computed at request time. `POST /v1/read-states/query` accepts 1–100 unique room IDs already granted by the signed token and obtains every room's cursor/count plus latest visible message ID/time in one storage statement. It also returns `total_unread_count` and `unread_room_count`. The result follows input order and contains no message body or sender, so the host can merge it with its own case/member/assignment list and lazily fetch one selected transcript. Any unauthorized, missing, or actively banned room fails the whole query; no partial result can reveal another room. The query has its own per-subject `SAMSARIX_CHAT_READ_STATE_QUERIES_PER_MINUTE` allowance.

Samsarix does not push aggregate unread-count events or discover a subject's room membership. Clients should refresh the batch after reconnect, relevant message activity, or marking a room read. Hosts with more than 100 candidate rooms should page their own membership list and issue bounded batches.

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

The host application should use opaque, stable internal account IDs as token subjects rather than email addresses. Read timestamps and typing activity can reveal engagement patterns, so expose them only to participants with a legitimate room relationship and document their use in the host product's privacy notice. Samsarix deliberately does not provide per-message read receipts, server-owned room membership discovery, or durable typing history.

This shape follows common chat expectations without copying an entire hosted platform: Stream documents channel lists ordered or filtered by unread state, Sendbird exposes per-channel and total unread counts, and Twilio pairs user-conversation unread counts with a last-read cursor. Samsarix instead batches only host-supplied, token-authorized room IDs because membership remains outside the engine. See [Stream channel queries](https://getstream.io/chat/docs/javascript/query-channels/), [Sendbird unread totals](https://sendbird.com/docs/chat/platform-api/v3/user/managing-unread-count/get-number-of-unread-items), and [Twilio user conversations](https://www.twilio.com/docs/conversations/api/user-conversation-resource).
