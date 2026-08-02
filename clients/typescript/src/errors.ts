// Copyright (c) 2026 Samsarix LLC
// SPDX-License-Identifier: MPL-2.0

export class SamsarixApiError extends Error {
  readonly status: number;
  readonly code: string;
  readonly details?: unknown;

  constructor(status: number, code: string, message: string, details?: unknown) {
    super(message);
    this.name = "SamsarixApiError";
    this.status = status;
    this.code = code;
    if (details !== undefined) {
      this.details = details;
    }
  }
}

export class SamsarixConnectionError extends Error {
  readonly code?: number;

  constructor(message: string, code?: number) {
    super(message);
    this.name = "SamsarixConnectionError";
    if (code !== undefined) {
      this.code = code;
    }
  }
}
