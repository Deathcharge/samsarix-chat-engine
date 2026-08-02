# Reliable application webhooks

Samsarix Chat Engine 0.9 can notify one host-application endpoint after committed message and moderation changes. The feature is opt-in and uses a transactional SQLite outbox: the chat mutation and its webhook row commit together, then a background worker delivers the row. A successful chat response does not wait for the receiver.

This is **at-least-once**, not exactly-once, delivery. A process can stop after the receiver accepts a request but before SQLite records the acknowledgement, so receivers must deduplicate by `webhook-id`. Delivery order is not guaranteed across retries; reconcile current state through the authenticated HTTP API when order matters.

## Configure an endpoint

Generate a Standard Webhooks symmetric secret containing 32 random bytes:

```bash
python -c "import base64,secrets; print('whsec_'+base64.b64encode(secrets.token_bytes(32)).decode())"
```

Set the destination and secret only in the server environment:

```text
SAMSARIX_CHAT_WEBHOOK_URL=https://app.example.com/webhooks/samsarix-chat
SAMSARIX_CHAT_WEBHOOK_SIGNING_SECRET=whsec_...
SAMSARIX_CHAT_WEBHOOK_EVENTS=message.created,message.updated,message.deleted,member.moderation.updated
```

The URL must use HTTPS and may contain a path, but not credentials, a query, or a fragment. Plain HTTP is accepted only for `localhost`, `127.0.0.1`, or `::1` development receivers. Remote targets that resolve to loopback, private, link-local, reserved, or other non-public addresses are blocked unless a trusted self-hosted operator explicitly sets `SAMSARIX_CHAT_WEBHOOK_ALLOW_PRIVATE_TARGETS=true`. DNS can change between validation and connection, so production deployments should also restrict the chat process's network egress; application-level checks are not a substitute for an egress firewall.

The current secret signs first. During a rotation, set `SAMSARIX_CHAT_WEBHOOK_PREVIOUS_SIGNING_SECRET` to the old `whsec_` value and the primary variable to the new value. Deliveries then carry both signatures. Remove the previous secret after receivers trust the new one. Secrets must decode to 24–64 random bytes and must be unique to this endpoint.

## Event contract

Every request is an HTTP `POST` with `Content-Type: application/json`, no redirect following, platform-default TLS certificate verification, and these [Standard Webhooks](https://github.com/standard-webhooks/standard-webhooks/blob/main/spec/standard-webhooks.md) headers:

- `webhook-id`: stable for the event and every retry/manual replay;
- `webhook-timestamp`: Unix seconds for this delivery attempt;
- `webhook-signature`: one or more space-separated `v1,<base64-hmac>` signatures.

The minified UTF-8 body has a stable envelope:

```json
{
  "id": "wh_...",
  "type": "message.created",
  "timestamp": "2026-08-02T12:00:00+00:00",
  "data": {
    "room_id": "support-123",
    "message": {
      "id": "...",
      "room_id": "support-123",
      "sender": "customer-42",
      "content": "I need help",
      "created_at": "2026-08-02T12:00:00+00:00",
      "client_message_id": null,
      "edited_at": null,
      "deleted_at": null
    }
  }
}
```

`message.updated` and `message.deleted` add `data.actor`; the deleted message is the committed empty-content tombstone. `member.moderation.updated` contains `data.actor` plus `data.moderation` with the room, subject, nullable mute/ban expiries, and update time. Event selection happens before outbox insertion, so unselected events consume no delivery storage.

Webhook payloads contain message content and stable subject/display identifiers. Configure a destination only when that transfer fits the deployment's privacy policy, retention rules, and user disclosures. Payload copies remain in the bounded SQLite outbox until pruning or resource deletion; the operations API intentionally returns metadata only.

## Verify before processing

Receivers must use the unmodified request bytes. For each `v1` value, compute HMAC-SHA256 over:

```text
webhook-id.webhook-timestamp.raw-body
```

Decode the `whsec_` suffix from base64, compare signatures with a constant-time function, reject timestamps outside a small tolerance such as five minutes, and atomically record `webhook-id` before applying side effects. Keep the deduplication record for at least as long as the sender can retry or an operator can replay; a durable business-event table is preferable to a short cache for important workflows.

```python
import base64
import hashlib
import hmac
import time


def verify(raw_body: bytes, headers: dict[str, str], secret: str) -> str:
    delivery_id = headers["webhook-id"]
    timestamp = int(headers["webhook-timestamp"])
    if abs(time.time() - timestamp) > 300:
        raise ValueError("stale webhook")
    key = base64.b64decode(secret.removeprefix("whsec_"), validate=True)
    signed = delivery_id.encode("ascii") + b"." + str(timestamp).encode("ascii") + b"." + raw_body
    expected = base64.b64encode(hmac.new(key, signed, hashlib.sha256).digest()).decode("ascii")
    candidates = [part.removeprefix("v1,") for part in headers["webhook-signature"].split() if part.startswith("v1,")]
    if not any(hmac.compare_digest(expected, candidate) for candidate in candidates):
        raise ValueError("invalid webhook signature")
    return delivery_id
```

Return any `2xx` only after durably accepting the event. All other statuses, connection failures, and timeouts are failures. `3xx` responses are never followed, and `410 Gone` terminally fails that delivery. `Retry-After` on an error is honored up to 24 hours.

## Retry and recovery

The default request timeout is 10 seconds. The default nine-attempt schedule starts immediately, then retries at approximately 5 seconds, 5 minutes, 30 minutes, 2 hours, 5 hours, 10 hours, 14 hours, and 20 hours, with deterministic ±20% jitter. Operators can configure 1–20 attempts and a 0.1–30 second timeout. Attempts after the documented schedule are at roughly 24-hour intervals.

Inspect delivery metadata:

```bash
curl --fail-with-body \
  -H "X-API-Key: $SAMSARIX_CHAT_API_KEY" \
  'http://127.0.0.1:8000/v1/admin/webhook-deliveries?status=failed&limit=50'
```

The response includes event type, room, attempt timestamps/count, final status code, a sanitized error code, and `replayable`. It never includes the payload, destination URL, secret, or receiver response body. Replay one delivery with the same `webhook-id`:

```bash
curl --fail-with-body -X POST \
  -H "X-API-Key: $SAMSARIX_CHAT_API_KEY" \
  http://127.0.0.1:8000/v1/admin/webhook-deliveries/wh_.../retry
```

Replay resets the displayed attempt history for that delivery. Deleting a message or room, or removing a message through configured age/count retention, cancels its pending prior payloads and scrubs payload bytes from completed/terminal rows while retaining metadata; attempting to replay such a row returns `409 webhook_payload_unavailable`. The deletion tombstone event itself can still deliver while the room exists. Deleting the room cancels that remaining pending row as part of the same transaction.

Deletion cannot recall data the receiver already accepted, and a delivery already claimed by the worker may finish while deletion is in progress. Downstream erasure therefore remains the receiver operator's responsibility.

The outbox retains at most `SAMSARIX_CHAT_MAX_WEBHOOK_DELIVERIES` rows. Completed/terminal rows are pruned oldest-first when new events arrive. If every retained row is still pending and the cap is reached, the originating message or moderation transaction returns `507 webhook_capacity_reached` and rolls back rather than committing without its promised event.

Backups include the outbox. Restoring an earlier backup can redeliver events whose acknowledgements occurred after that snapshot; receiver idempotency is therefore required for both ordinary retries and disaster recovery.
