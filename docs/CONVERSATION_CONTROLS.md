# Conversation controls

Version 0.6 adds the smallest moderation surface that supports real embedded-community, support-room, classroom, and live-event workflows without turning Samsarix Chat Engine into an identity provider.

## Why these controls

The contract follows established chat primitives rather than inventing Samsarix-specific semantics:

- Sendbird defines mute as allowing a member to read while preventing messages, ban as removal and blocked re-entry, and freeze as operator-only communication.
- Discord permits authors to edit their own messages and requires elevated permission to delete another user's message.
- Ably exposes send, update, delete, and receive as core room-message operations and supports scoped edit/delete authorization.

References: [Sendbird moderation overview](https://docs.sendbird.com/docs/chat/platform-api/v3/moderation/moderation-overview), [Discord message resource](https://docs.discord.com/developers/resources/message), and [Ably room messages](https://ably.com/docs/chat/rooms/messages).

These primitives cover several concrete use cases:

- A support agent freezes an incident room while publishing authoritative updates.
- A teacher mutes a disruptive participant without hiding lesson history.
- A community moderator bans one account and immediately closes its active sockets.
- An author corrects a mistake, while a moderator removes abusive content without collapsing message order.

## Identity boundary

Member controls target the stable `sub` in a signed room token. Samsarix does not accept a display name as a moderation identity. The host application remains responsible for registration, account recovery, and deciding which of its users maps to each subject.

The deployment API key, local loopback operator, and signed `admin` tokens bypass member controls. Treat those credentials as privileged operational secrets.

## Freeze a room

```bash
curl -X PATCH http://127.0.0.1:8000/v1/rooms/general \
  -H "X-API-Key: $SAMSARIX_CHAT_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"frozen":true}'
```

Existing read sessions remain connected and receive `room.frozen`. Member publishes and edits return `room_frozen`; administrators can still post announcements and remove content. Unfreeze with `{"frozen":false}`. Archiving remains stronger: it closes every room socket and blocks ordinary writes until reopen.

## Mute, ban, and clear controls

Mute `customer-42` for 15 minutes:

```bash
curl -X PATCH http://127.0.0.1:8000/v1/rooms/general/members/customer-42/moderation \
  -H "X-API-Key: $SAMSARIX_CHAT_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"muted_for_seconds":900}'
```

Ban the subject for one day:

```bash
curl -X PATCH http://127.0.0.1:8000/v1/rooms/general/members/customer-42/moderation \
  -H "X-API-Key: $SAMSARIX_CHAT_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"banned_for_seconds":86400}'
```

Clear both controls:

```bash
curl -X PATCH http://127.0.0.1:8000/v1/rooms/general/members/customer-42/moderation \
  -H "X-API-Key: $SAMSARIX_CHAT_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"muted_for_seconds":0,"banned_for_seconds":0}'
```

Durations are evaluated in UTC and capped at 31,536,000 seconds. Supplying only one field preserves the other. Expired controls no longer affect access; clearing every control removes the subject's moderation record.

## Edit and delete messages

An author uses their bearer token; an operator can use the API key for any message:

```bash
curl -X PATCH http://127.0.0.1:8000/v1/rooms/general/messages/MESSAGE_ID \
  -H "Authorization: Bearer $ROOM_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"content":"Corrected message"}'

curl -X DELETE http://127.0.0.1:8000/v1/rooms/general/messages/MESSAGE_ID \
  -H "Authorization: Bearer $ROOM_TOKEN"
```

Edits overwrite the current content and set `edited_at`; this release intentionally does not retain revision history. Delete replaces content with an empty string and sets `deleted_at`. Keeping the metadata-only tombstone preserves conversation order and lets reconnecting clients converge even if they missed the live event.

## Audit and privacy behavior

The administrative audit records room freeze changes, member-control expiry metadata, and message IDs for update/delete operations. It never copies message content or credentials. A delete removes content from the live database, but operators must separately apply their retention and deletion obligations to prior backups and exported files.

Moderation is an application control, not a legal-compliance system. Document moderator authority, appeals, retention, and user notice for the jurisdiction and audience where the engine is deployed.
