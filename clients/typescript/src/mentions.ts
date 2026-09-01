// Copyright (c) 2026 Samsarix LLC
// SPDX-License-Identifier: MPL-2.0

const MAX_MENTIONED_SUBJECTS = 10;

export function normalizeMentionedSubjects(value: readonly string[]): string[] {
  if (!Array.isArray(value)) throw new TypeError("mentionedSubjects must be an array");
  if (value.length > MAX_MENTIONED_SUBJECTS) {
    throw new RangeError(`mentionedSubjects must contain at most ${MAX_MENTIONED_SUBJECTS} items`);
  }
  const subjects = [...value];
  for (const subject of subjects) {
    if (typeof subject !== "string") throw new TypeError("mentionedSubjects must contain strings");
    if (subject.length < 1 || subject.length > 64) {
      throw new RangeError("mentioned subjects must be between 1 and 64 characters");
    }
    if (subject !== subject.trim()) {
      throw new TypeError("mentioned subjects must not have surrounding whitespace");
    }
  }
  if (new Set(subjects).size !== subjects.length) {
    throw new TypeError("mentionedSubjects must not contain duplicates");
  }
  return subjects;
}

export function isMentionedSubjects(value: unknown): value is string[] {
  try {
    normalizeMentionedSubjects(value as readonly string[]);
    return true;
  } catch {
    return false;
  }
}
