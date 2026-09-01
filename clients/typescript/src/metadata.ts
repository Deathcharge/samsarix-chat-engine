// Copyright (c) 2026 Samsarix LLC
// SPDX-License-Identifier: MPL-2.0

import type { MessageMetadata } from "./types.js";

const KEY_PATTERN = /^[a-z][a-z0-9_.-]{0,63}$/;
const MAX_KEYS = 20;
const MAX_BYTES = 4096;

export function normalizeMessageMetadata(value: MessageMetadata): MessageMetadata {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new TypeError("metadata must be an object");
  }
  const entries = Object.entries(value).sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0));
  if (entries.length > MAX_KEYS) {
    throw new RangeError(`metadata must contain at most ${MAX_KEYS} keys`);
  }
  const normalized: MessageMetadata = {};
  for (const [key, item] of entries) {
    if (!KEY_PATTERN.test(key)) {
      throw new RangeError("metadata keys must be 1-64 lowercase ASCII key characters");
    }
    if (
      item !== null &&
      typeof item !== "string" &&
      typeof item !== "boolean" &&
      typeof item !== "number"
    ) {
      throw new TypeError("metadata values must be JSON scalars");
    }
    if (typeof item === "number") {
      if (!Number.isFinite(item)) throw new RangeError("metadata numbers must be finite");
      if (Number.isInteger(item) && !Number.isSafeInteger(item)) {
        throw new RangeError("metadata integers must be exactly representable by JavaScript");
      }
    }
    normalized[key] = item;
  }
  if (new TextEncoder().encode(JSON.stringify(normalized)).byteLength > MAX_BYTES) {
    throw new RangeError(`metadata must not exceed ${MAX_BYTES} UTF-8 JSON bytes`);
  }
  return normalized;
}

export function isMessageMetadata(value: unknown): value is MessageMetadata {
  try {
    normalizeMessageMetadata(value as MessageMetadata);
    return true;
  } catch {
    return false;
  }
}
