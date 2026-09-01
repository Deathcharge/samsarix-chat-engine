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
SAMSARIX_CHAT_WEBHOOK_EVENTS=message.created,message.updated,message.deleted,message.reaction.updated,message.pin.updated,member.moderation.updated
```

The URL must use HTTPS and may contain a path, but not credentials, a query, or a fragment. Plain HTTP is accepted only for `localhost`, `127.0.0.1`, or `::1` development receivers. Each attempt resolves the destination once, rejects any disallowed address, and pins the selected address for the connection while preserving the original hostname for TLS verification and `Host`. Remote targets that resolve to loopback, private, link-local, reserved, or other non-public addresses are blocked unless a trusted self-hosted operator explicitly sets `SAMSARIX_CHAT_WEBHOOK_ALLOW_PRIVATE_TARGETS=true`. Production deployments should still restrict the chat process's network egress; application-level checks are not a substitute for an egress firewall or protection from deployment-level routing changes.

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
      "parent_message_id": null,
      "reactions": [],
      "pinned_at": null,
      "pinned_by": null,
      "metadata": {"ticket.id":"SUP-42"},
      "attachments": [{
        "id": "support-upload-SUP-42-trace",
        "name": "payment-trace.txt",
        "media_type": "text/plain",
        "size_bytes": 1842,
        "sha256": null
      }],
      "mentioned_subjects": ["agent-7"],
      "edited_at": null,
      "deleted_at": null
    }
  }
}
```

For a threaded reply, `data.message.parent_message_id` is the top-level message ID; top-level messages use null. Application `metadata` is the same untrusted bounded scalar object returned by history. `attachments` contains the same bounded host-owned descriptors and never file bytes or a download URL. `mentioned_subjects` contains up to ten untrusted host-resolved IDs; receivers may use them as notification candidates only after their own membership and preference checks. `message.updated` and `message.deleted` add `data.actor`; the deleted message is the committed empty-content, metadata-free, attachment-free, mention-free, reaction-free, unpinned tombstone. `message.reaction.updated` contains the complete current message plus `key`, `reactor`, `present`, `changed`, and `updated_at`. `message.pin.updated` contains the complete current message plus `pinner`, `pinned`, `changed`, and `updated_at`. Only real state changes enqueue either mutation event. `member.moderation.updated` contains `data.actor` plus `data.moderation` with the room, subject, nullable mute/ban expiries, and update time. Event selection happens before outbox insertion, so unselected events consume no delivery storage.

Webhook payloads contain message content, attachment descriptors, mention targets, and stable subject/display identifiers. Names, media types, sizes, digests, and target IDs can themselves be sensitive even though no file bytes, access URLs, contact addresses, or device tokens are included. Configure a destination only when that transfer fits the deployment's privacy policy, retention rules, and user disclosures. Payload copies remain in the bounded SQLite outbox until pruning or resource deletion; the operations API intentionally returns metadata only.

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

The default total network-attempt budget is 10 seconds, configurable from 0.1–30 seconds. One monotonic deadline covers the caller's DNS wait, TCP connect, TLS handshake, request writes and response headers. Receiving occasional headers does not reset it. Expiry records a sanitized `timeout` failure and interrupts the owned socket; a late result cannot replace that outcome. Database claim/outcome operations have their own backend budgets, so this is not a bound on the complete dispatcher iteration or whole-process shutdown, nor a hard-real-time scheduling guarantee.

Python cannot forcibly interrupt native DNS resolution. Each dispatcher therefore owns at most one daemon transport worker and one outstanding job, without using the application's default thread executor. If DNS is still blocked after timeout or cancellation, the dispatcher takes no more claims until that job returns; its expired result cannot create a connection. This bounds outstanding work and allows process exit, but a permanently stuck resolver requires operator diagnosis/restart to restore delivery. The lingering job can retain its one claimed body/signing closure until return or process exit. A completed idle worker releases those references. Stop or cancellation interrupts owned TCP/TLS sockets without waiting for native DNS, leaves an unfinished claim unrecorded, and prevents new claims. Embedders that call `process_due_once()` directly must call `stop()` when finished; cancellation of `run()` also stops its transport. A stopped dispatcher is not restartable.

The default nine-attempt schedule starts immediately, then retries at approximately 5 seconds, 5 minutes, 30 minutes, 2 hours, 5 hours, 10 hours, 14 hours, and 20 hours, with deterministic ±20% jitter. Operators can configure 1–20 attempts. Attempts after the documented schedule are at roughly 24-hour intervals.

Cancellation interrupts the connection with socket shutdown; the transport owner releases its descriptor only after native I/O unwinds. This prevents descriptor reuse while an older operation still owns it. The cancellation path does not manipulate OpenSSL state from another thread. Outstanding transport capacity remains occupied until owner cleanup completes.

The PostgreSQL preview uses a database-clock 60-second claim lease. A crashed worker leaves its claim behind; another worker can reclaim the pending payload after expiry. The delivery ID and raw payload remain stable, while the retry has a fresh signing timestamp. An attempt sent before a crash but never recorded is absent from `attempt_count`, so that counter is not a count of all receiver requests or business effects. An old external request may outlive its database lease; exclusive live database ownership does not promise one physical request in flight. Durable receiver deduplication is required.

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
