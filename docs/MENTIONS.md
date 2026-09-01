# Host-resolved message mentions

Samsarix stores explicit message mention targets so a host application can highlight a participant, route an escalation, or decide whether to send its own notification. Mentions are identity references, not parsed display text and not a notification service.

HTTP and WebSocket message creation accept an ordered `mentioned_subjects` array:

```json
{
  "content": "Please review the failed payment",
  "mentioned_subjects": ["agent-7", "billing-oncall"]
}
```

The array may contain at most ten unique strings. Every subject is 1–64 characters, case-sensitive, and must not have surrounding whitespace. Order is preserved. The API does not extract `@names` from message content, normalize identifiers, expand roles or groups, or accept duplicate targets. Mentions do not satisfy the requirement for text or an attachment.

## Identity and authorization boundary

The host application owns accounts, room membership, assignments, aliases, roles, notification preferences, and device/email endpoints. It should submit the same opaque stable IDs it uses as signed-token subjects. Samsarix cannot prove that a target belongs to the room because it deliberately has no membership directory. A writer authorized for `room:write` may therefore store any syntactically valid target; clients must not treat a mention as proof of membership, access, role, or identity.

Use the existing signed `message.created` and `message.updated` webhooks to enqueue host-owned notification work. Verify and durably deduplicate the webhook first, then resolve each current target through the host database, re-check room access and preferences, and pass only the minimum necessary content to an approved delivery provider. Samsarix does not register devices, call APNs/FCM, send email, retry provider notifications, or record notification delivery receipts.

## Edits, events, and deletion

Authors and administrators can replace mentions while editing content by supplying `mentioned_subjects`; omitting it or sending `null` preserves the current array, while `[]` clears it. Idempotent create replay returns the original committed message and does not apply later targets.

History, replies, search results, pins, room export schema 8, realtime message events, and selected message webhooks carry the complete current array. Consumers replace cached messages by ID instead of independently adding/removing targets. Tombstone deletion clears mentions from the message, scrubs them from retained PostgreSQL message events, and removes or erases prior webhook bodies under the existing deletion boundary. Already delivered or concurrently in-flight copies remain the receiver's responsibility.

Mention targets are stable identifiers and may still be personal data. They are plaintext in the chat database, exports, backups, realtime frames, and configured message webhooks. Prefer opaque internal IDs over email addresses or display names, apply the same retention/access controls as other message metadata, and do not put notification tokens or contact details in the array.

## Cost and abuse controls

The ten-target limit bounds message, event, export, and webhook amplification. It does not limit work a receiver chooses to fan out after delivery. Host notification workers should enforce per-user/per-room budgets, preference checks, deduplication, quiet hours where applicable, and provider retry/cost limits. Muting a Samsarix writer blocks new writes; it does not retract already delivered host notifications.
