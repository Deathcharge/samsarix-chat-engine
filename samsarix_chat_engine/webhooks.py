# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Durable Standard Webhooks delivery with bounded retries and transport policy."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import ipaddress
import logging
import math
import socket
import ssl
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import HTTPRedirectHandler, Request, build_opener

from .store import ChatStore, PendingWebhook, WebhookDeliveryNotFoundError

logger = logging.getLogger(__name__)
_RETRY_SCHEDULE_SECONDS = (5.0, 300.0, 1_800.0, 7_200.0, 18_000.0, 36_000.0, 50_400.0, 72_000.0)


class WebhookTargetError(ValueError):
    """Raised when a configured destination resolves outside the allowed network policy."""


class _NoRedirects(HTTPRedirectHandler):
    def redirect_request(
        self,
        req: Request,
        fp: object,
        code: int,
        msg: str,
        headers: object,
        newurl: str,
    ) -> None:
        return None


@dataclass(frozen=True, slots=True)
class WebhookAttemptResult:
    """Sanitized result retained by the durable outbox."""

    status_code: int | None
    error: str | None
    retry_after_seconds: float | None = None

    @property
    def delivered(self) -> bool:
        return self.status_code is not None and 200 <= self.status_code < 300


def sign_webhook(delivery_id: str, timestamp: int, payload: bytes, secrets: tuple[bytes, ...]) -> str:
    """Create the Standard Webhooks v1 signature list for an exact payload."""

    signed = delivery_id.encode("ascii") + b"." + str(timestamp).encode("ascii") + b"." + payload
    return " ".join(
        f"v1,{base64.b64encode(hmac.new(secret, signed, hashlib.sha256).digest()).decode('ascii')}"
        for secret in secrets
    )


def _validate_target(url: str, *, allow_private_targets: bool) -> None:
    parsed = urlparse(url)
    try:
        hostname = parsed.hostname
        port = parsed.port
    except ValueError as exc:
        raise WebhookTargetError("invalid_target") from exc
    loopback = hostname is not None and hostname.lower() in {"localhost", "127.0.0.1", "::1"}
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or hostname is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or (parsed.scheme == "http" and not loopback)
    ):
        raise WebhookTargetError("invalid_target")
    if loopback:
        return
    try:
        addresses = socket.getaddrinfo(hostname, port or 443, type=socket.SOCK_STREAM)
    except OSError as exc:
        raise WebhookTargetError("dns_error") from exc
    if not addresses:
        raise WebhookTargetError("dns_error")
    if allow_private_targets:
        return
    for address in addresses:
        try:
            resolved = ipaddress.ip_address(address[4][0])
        except ValueError as exc:
            raise WebhookTargetError("dns_error") from exc
        if not resolved.is_global:
            raise WebhookTargetError("private_target_blocked")


def _retry_after_seconds(value: str | None, now: datetime) -> float | None:
    if value is None:
        return None
    try:
        seconds = float(value)
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(value)
        except (TypeError, ValueError, OverflowError):
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=timezone.utc)
        seconds = (retry_at - now).total_seconds()
    if not math.isfinite(seconds):
        return None
    if seconds < 0:
        return 0.0
    return min(seconds, 86_400.0)


def _send_request(
    *,
    url: str,
    delivery: PendingWebhook,
    secrets: tuple[bytes, ...],
    timeout: float,
    attempted_at: datetime,
    allow_private_targets: bool,
) -> WebhookAttemptResult:
    """Send one non-redirecting request using the platform TLS trust store."""

    try:
        _validate_target(url, allow_private_targets=allow_private_targets)
    except WebhookTargetError as exc:
        return WebhookAttemptResult(status_code=None, error=str(exc))
    timestamp = int(attempted_at.timestamp())
    request = Request(  # noqa: S310 - target is validated and redirects are disabled
        url,
        data=delivery.payload,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "User-Agent": "Samsarix-Chat-Webhook/0.9",
            "webhook-id": delivery.delivery.id,
            "webhook-timestamp": str(timestamp),
            "webhook-signature": sign_webhook(delivery.delivery.id, timestamp, delivery.payload, secrets),
        },
    )
    opener = build_opener(_NoRedirects)
    try:
        with opener.open(request, timeout=timeout) as response:  # noqa: S310 - validated above
            status = response.status
        if 200 <= status < 300:
            return WebhookAttemptResult(status_code=status, error=None)
        return WebhookAttemptResult(status_code=status, error=f"http_status_{status}")
    except HTTPError as exc:
        now = datetime.now(timezone.utc)
        result = WebhookAttemptResult(
            status_code=exc.code,
            error=f"http_status_{exc.code}",
            retry_after_seconds=_retry_after_seconds(exc.headers.get("Retry-After"), now),
        )
        exc.close()
        return result
    except TimeoutError:
        return WebhookAttemptResult(status_code=None, error="timeout")
    except ssl.SSLError:
        return WebhookAttemptResult(status_code=None, error="tls_error")
    except (ConnectionError, URLError, OSError):
        return WebhookAttemptResult(status_code=None, error="connection_error")


class WebhookDispatcher:
    """Poll and deliver a persistent outbox with restart-safe at-least-once semantics."""

    def __init__(
        self,
        store: ChatStore,
        *,
        url: str,
        secrets: tuple[bytes, ...],
        timeout: float,
        max_attempts: int,
        allow_private_targets: bool,
        poll_interval: float = 0.5,
    ) -> None:
        self.store = store
        self.url = url
        self.secrets = secrets
        self.timeout = timeout
        self.max_attempts = max_attempts
        self.allow_private_targets = allow_private_targets
        self.poll_interval = poll_interval
        self._stop = asyncio.Event()
        self._wake = asyncio.Event()

    def wake(self) -> None:
        """Prompt the worker after a new commit or operator replay."""

        self._wake.set()

    def stop(self) -> None:
        """Request graceful worker shutdown."""

        self._stop.set()
        self._wake.set()

    async def run(self) -> None:
        """Deliver due rows until application shutdown without dropping the worker on one failure."""

        while not self._stop.is_set():
            try:
                processed = await self.process_due_once()
            except Exception:  # noqa: BLE001 - worker isolation; details go to operator logs
                logger.exception("Webhook worker iteration failed")
                processed = False
            if processed:
                continue
            self._wake.clear()
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self.poll_interval)
            except TimeoutError:
                pass

    async def process_due_once(self, *, now: datetime | None = None) -> bool:
        """Attempt one due delivery and return whether a row was processed."""

        attempted_at = now or datetime.now(timezone.utc)
        pending = await self.store.next_webhook_delivery(attempted_at)
        if pending is None or self._stop.is_set():
            return False
        result = await asyncio.to_thread(
            _send_request,
            url=self.url,
            delivery=pending,
            secrets=self.secrets,
            timeout=self.timeout,
            attempted_at=attempted_at,
            allow_private_targets=self.allow_private_targets,
        )
        attempt_number = pending.delivery.attempt_count + 1
        terminal = not result.delivered and (attempt_number >= self.max_attempts or result.status_code == 410)
        next_attempt_at = None
        if not result.delivered and not terminal:
            delay = self._retry_delay(pending.delivery.id, attempt_number)
            if result.retry_after_seconds is not None:
                delay = max(delay, result.retry_after_seconds)
            next_attempt_at = attempted_at + timedelta(seconds=delay)
        try:
            await self.store.record_webhook_attempt(
                pending.delivery.id,
                attempted_at=attempted_at,
                status_code=result.status_code,
                error=result.error,
                next_attempt_at=next_attempt_at,
                delivered=result.delivered,
                failed=terminal,
            )
        except WebhookDeliveryNotFoundError:
            logger.info(
                "Webhook delivery state changed before outcome recording", extra={"delivery_id": pending.delivery.id}
            )
        if terminal:
            logger.error(
                "Webhook delivery exhausted retries",
                extra={"delivery_id": pending.delivery.id, "event_type": pending.delivery.event_type},
            )
        return True

    @staticmethod
    def _retry_delay(delivery_id: str, attempt_number: int) -> float:
        index = min(attempt_number - 1, len(_RETRY_SCHEDULE_SECONDS) - 1)
        base = _RETRY_SCHEDULE_SECONDS[index] if attempt_number <= len(_RETRY_SCHEDULE_SECONDS) else 86_400.0
        digest = hashlib.sha256(f"{delivery_id}:{attempt_number}".encode()).digest()
        jitter = (int.from_bytes(digest[:2], "big") / 65_535.0 - 0.5) * 0.4
        return max(1.0, base * (1.0 + jitter))
