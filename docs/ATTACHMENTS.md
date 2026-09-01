# Host-owned attachment references

Samsarix can attach bounded file descriptors to a message without accepting, serving, fetching, scanning, or deleting file bytes. This is intended for support logs/screenshots, classroom material, incident evidence, and other application-owned artifacts. The host application remains the file service and authorization authority.

## Contract

HTTP and WebSocket message creation accept up to five ordered `attachments`:

```json
{
  "content": "Console evidence",
  "attachments": [
    {
      "id": "upload:SUP-42:console-log",
      "name": "console-log.txt",
      "media_type": "text/plain",
      "size_bytes": 1842,
      "sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    }
  ]
}
```

`content` may be empty when at least one attachment is present. Each descriptor has:

- `id`: a host-owned opaque identifier, 1–128 portable ASCII characters matching `^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`; IDs must be unique within the message;
- `name`: an untrusted display name of 1–255 Unicode characters with ASCII controls rejected;
- `media_type`: a lowercase `type/subtype` label of at most 127 characters;
- `size_bytes`: a non-negative JavaScript-safe integer supplied by the host;
- `sha256`: an optional lowercase 64-hex digest. Omitted values are returned as `null`; a digest can detect a mismatched download but does not prove the file is safe.

The canonical descriptor array is capped at 8192 UTF-8 JSON bytes. Unknown fields, URLs, nested custom data, and more than five descriptors are rejected. Use bounded message `metadata` for scalar application context.

References are immutable after creation. A normal content/metadata edit preserves them. History, replies, search results, pins, realtime events, selected signed webhooks, and export schema 7 return the same descriptors. Tombstone deletion clears them; retention and room deletion remove them with the message. PostgreSQL also scrubs them from retained message-event payloads, and terminal webhook bodies follow the existing deletion scrub policy.

## Recommended host workflow

1. Authenticate and authorize the user in the host application before offering an upload.
2. Upload to a separate object/file service under host-enforced type, size, quota, filename, scanning, and retention policy.
3. Commit a host record that binds an opaque attachment ID to the uploader, intended room, object key, verified size/type/digest, and lifecycle state.
4. Send the Samsarix message using only that stable opaque ID and display metadata.
5. When a reader opens the attachment, reauthorize their current room access in the host application, resolve the ID, and issue or proxy a short-lived download response.
6. Reconcile orphaned uploads and deleted/retained messages with a host-owned periodic job. Webhooks are useful signals but are delivered at least once and are not a garbage-collection database.

Do not persist a presigned object-store URL in `id`, `name`, `metadata`, or another descriptor field. Signed URLs are bearer credentials, may expire long before chat history, and can leak through exports, webhooks, backups, logs, or copied messages. Generate them only after current authorization at download time. Do not derive a filesystem path directly from `id` or `name`.

The engine never dereferences a reference, so accepting one does not establish that an object exists, that its claimed media type/size/digest is accurate, that it is malware-free, or that a reader may access it. Clients must treat names and media types as untrusted text, avoid automatic active-content rendering, and rely on the host download response for safe disposition headers.

## Deliberate exclusions

This milestone does not add multipart upload endpoints, SQLite/PostgreSQL blobs, an object-store SDK, thumbnail/link-preview fetching, content sniffing, antivirus, image transcoding, download URLs, file access control, automatic object deletion, per-room file galleries, or storage/egress claims. Those concerns belong to the host application or a separately operated file service and must be tested against its own policy and costs.

Primary design references checked on 2026-09-01: [Sendbird external file URLs and access-control responsibility](https://sendbird.com/docs/chat/platform-api/v3/message/messaging-basics/send-a-message), [Stream file uploads and signed access](https://getstream.io/chat/docs/node/file-uploads/), [OWASP file-upload controls and separated storage](https://cheatsheetseries.owasp.org/cheatsheets/File_Upload_Cheat_Sheet.html), [Google Cloud signed-URL bearer behavior](https://docs.cloud.google.com/storage/docs/access-control/signed-urls), and [Azure SAS security and cost guidance](https://learn.microsoft.com/en-us/azure/storage/common/storage-sas-overview).
