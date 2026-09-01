// Copyright (c) 2026 Samsarix LLC
// SPDX-License-Identifier: MPL-2.0

import type { AttachmentReference } from "./types.js";

const ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
const MEDIA_TYPE_PATTERN = /^[a-z0-9][a-z0-9!#$&^_.+\-]{0,62}\/[a-z0-9][a-z0-9!#$&^_.+\-]{0,62}$/;
const SHA256_PATTERN = /^[a-f0-9]{64}$/;
const MAX_SAFE_INTEGER = 9_007_199_254_740_991;
const MAX_COUNT = 5;
const MAX_BYTES = 8192;
const ALLOWED_KEYS = new Set(["id", "name", "media_type", "size_bytes", "sha256"]);

export function normalizeAttachmentReferences(value: readonly AttachmentReference[]): AttachmentReference[] {
  if (!Array.isArray(value)) throw new TypeError("attachments must be an array");
  if (value.length > MAX_COUNT) throw new RangeError(`attachments must contain at most ${MAX_COUNT} items`);
  const normalized = value.map((attachment) => normalizeAttachmentReference(attachment));
  const identifiers = new Set(normalized.map((attachment) => attachment.id));
  if (identifiers.size !== normalized.length) {
    throw new RangeError("attachment IDs must be unique within a message");
  }
  if (new TextEncoder().encode(JSON.stringify(normalized)).byteLength > MAX_BYTES) {
    throw new RangeError(`attachments must not exceed ${MAX_BYTES} UTF-8 JSON bytes`);
  }
  return normalized;
}

export function isAttachmentReferences(value: unknown): value is AttachmentReference[] {
  try {
    normalizeAttachmentReferences(value as AttachmentReference[]);
    return true;
  } catch {
    return false;
  }
}

function normalizeAttachmentReference(value: AttachmentReference): AttachmentReference {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new TypeError("each attachment must be an object");
  }
  if (Object.keys(value).some((key) => !ALLOWED_KEYS.has(key))) {
    throw new TypeError("attachments contain an unsupported field");
  }
  const id = typeof value.id === "string" ? value.id.trim() : "";
  const name = typeof value.name === "string" ? value.name.trim() : "";
  const mediaType = typeof value.media_type === "string" ? value.media_type.trim() : "";
  if (!ID_PATTERN.test(id)) throw new RangeError("attachment id must be 1-128 portable ASCII characters");
  if (name.length === 0 || [...name].length > 255 || /[\u0000-\u001f\u007f]/u.test(name)) {
    throw new RangeError("attachment name must be 1-255 characters without controls");
  }
  if (!MEDIA_TYPE_PATTERN.test(mediaType)) {
    throw new RangeError("attachment media_type must be a lowercase type/subtype");
  }
  if (!Number.isSafeInteger(value.size_bytes) || value.size_bytes < 0 || value.size_bytes > MAX_SAFE_INTEGER) {
    throw new RangeError("attachment size_bytes must be a non-negative JavaScript-safe integer");
  }
  if (value.sha256 !== undefined && value.sha256 !== null && !SHA256_PATTERN.test(value.sha256)) {
    throw new RangeError("attachment sha256 must be 64 lowercase hexadecimal characters");
  }
  return {
    id,
    name,
    media_type: mediaType,
    size_bytes: value.size_bytes,
    sha256: value.sha256 ?? null,
  };
}
