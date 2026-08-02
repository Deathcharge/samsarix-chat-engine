# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Durable Standard Webhooks delivery with bounded retries and transport policy."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import http.client
import ipaddress
import logging
import math
import socket
import ssl
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import quote, urlparse

from .store import ChatStorage, PendingWebhook, WebhookDeliveryNotFoundError

logger = logging.getLogger(__name__)
_RETRY_SCHEDULE_SECONDS = (5.0, 300.0, 1_800.0, 7_200.0, 18_000.0, 36_000.0, 50_400.0, 72_000.0)


class WebhookTargetError(ValueError):
    """Raised when a configured destination resolves outside the allowed network policy."""


@dataclass(frozen=True, slots=True)
class _ResolvedWebhookTarget:
    scheme: str
    hostname: str
    address: str
    port: int
    path: str
    host_header: str


class _PinnedHTTPSConnection(http.client.HTTPConnection):
    """Connect to one validated address while retaining the hostname for TLS verification."""

    def __init__(self, hostname: str, address: str, port: int, timeout: float) -> None:
        super().__init__(address, port, timeout=timeout)
        self._server_hostname = hostname
        self._ssl_context = ssl.create_default_context()

    def connect(self) -> None:
        super().connect()
        if self.sock is None:
            raise ConnectionError("HTTPS connection did not create a socket")
        self.sock = self._ssl_context.wrap_socket(self.sock, server_hostname=self._server_hostname)


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


def _resolve_target(url: str, *, allow_private_targets: bool) -> _ResolvedWebhookTarget:
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
    try:
        ascii_hostname = hostname.encode("idna").decode("ascii")
    except UnicodeError as exc:
        raise WebhookTargetError("invalid_target") from exc
    resolved_port = port or (443 if parsed.scheme == "https" else 80)
    try:
        addresses = socket.getaddrinfo(ascii_hostname, resolved_port, type=socket.SOCK_STREAM)
    except OSError as exc:
        raise WebhookTargetError("dns_error") from exc
    if not addresses:
        raise WebhookTargetError("dns_error")
    resolved_addresses: list[str] = []
    for address in addresses:
        try:
            resolved = ipaddress.ip_address(address[4][0])
        except ValueError as exc:
            raise WebhookTargetError("dns_error") from exc
        if not loopback and not allow_private_targets and not resolved.is_global:
            raise WebhookTargetError("private_target_blocked")
        normalized = str(resolved)
        if normalized not in resolved_addresses:
            resolved_addresses.append(normalized)
    host_header = f"[{ascii_hostname}]" if ":" in ascii_hostname else ascii_hostname
    default_port = 443 if parsed.scheme == "https" else 80
    if resolved_port != default_port:
        host_header = f"{host_header}:{resolved_port}"
    return _ResolvedWebhookTarget(
        scheme=parsed.scheme,
        hostname=ascii_hostname,
        address=resolved_addresses[0],
        port=resolved_port,
        path=quote(parsed.path or "/", safe="/%:@!$&'()*+,;=-._~"),
        host_header=host_header,
    )


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
        target = _resolve_target(url, allow_private_targets=allow_private_targets)
    except WebhookTargetError as exc:
        return WebhookAttemptResult(status_code=None, error=str(exc))
    timestamp = int(attempted_at.timestamp())
    headers = {
        "Content-Type": "application/json",
        "Host": target.host_header,
        "User-Agent": "Samsarix-Chat-Webhook/0.9",
        "webhook-id": delivery.delivery.id,
        "webhook-timestamp": str(timestamp),
        "webhook-signature": sign_webhook(delivery.delivery.id, timestamp, delivery.payload, secrets),
    }
    connection: http.client.HTTPConnection
    if target.scheme == "https":
        connection = _PinnedHTTPSConnection(target.hostname, target.address, target.port, timeout)
    else:
        connection = http.client.HTTPConnection(target.address, target.port, timeout=timeout)
    try:
        connection.request("POST", target.path, body=delivery.payload, headers=headers)
        response = connection.getresponse()
        try:
            status = response.status
            retry_after = response.getheader("Retry-After")
        finally:
            response.close()
        if 200 <= status < 300:
            return WebhookAttemptResult(status_code=status, error=None)
        now = datetime.now(timezone.utc)
        return WebhookAttemptResult(
            status_code=status,
            error=f"http_status_{status}",
            retry_after_seconds=_retry_after_seconds(retry_after, now),
        )
    except TimeoutError:
        return WebhookAttemptResult(status_code=None, error="timeout")
    except ssl.SSLError:
        return WebhookAttemptResult(status_code=None, error="tls_error")
    except (ConnectionError, http.client.HTTPException, OSError):
        return WebhookAttemptResult(status_code=None, error="connection_error")
    finally:
        connection.close()


class WebhookDispatcher:
    """Poll and deliver a persistent outbox with restart-safe at-least-once semantics."""

    def __init__(
        self,
        store: ChatStorage,
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
            except asyncio.TimeoutError:
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
