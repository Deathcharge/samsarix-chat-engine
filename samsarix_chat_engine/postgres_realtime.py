# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Durable PostgreSQL event-log relay for one process's local WebSockets."""

from __future__ import annotations

import asyncio
import logging
import uuid
from typing import TYPE_CHECKING, Any, Protocol

from pydantic import ValidationError

from .models import MemberModeration
from .postgres import (
    InstanceLeaseError,
    PostgresFoundation,
    PostgresUnavailableError,
    RealtimeEvent,
)

if TYPE_CHECKING:
    from .websocket_manager import ConnectionManager

logger = logging.getLogger(__name__)

_BROADCAST_EVENT_TYPES = frozenset(
    {
        "message.created",
        "message.updated",
        "message.deleted",
        "room.frozen",
        "room.unfrozen",
    }
)


class RealtimeTarget(Protocol):
    """Local socket operations needed by the durable relay."""

    async def broadcast(self, room_id: str, event: dict[str, Any]) -> None: ...

    async def close_room(
        self,
        room_id: str,
        event: dict[str, Any],
        *,
        code: int = 4409,
        reason: str = "Room archived",
    ) -> None: ...

    async def close_member(
        self,
        room_id: str,
        subject: str,
        event: dict[str, Any],
        *,
        code: int = 4403,
        reason: str = "Room access revoked",
    ) -> int: ...

    async def close_all(self) -> None: ...


class PostgresRealtimeRelay:
    """Replay committed PostgreSQL events to one process and advance only after dispatch."""

    def __init__(
        self,
        foundation: PostgresFoundation,
        target: RealtimeTarget,
        *,
        instance_id: str | None = None,
        lease_seconds: int = 30,
        poll_interval_seconds: float = 0.25,
        batch_size: int = 100,
    ) -> None:
        if not 3 <= lease_seconds <= 300:
            raise ValueError("realtime relay lease must be between 3 and 300 seconds")
        if not 0.01 <= poll_interval_seconds <= 5.0:
            raise ValueError("realtime relay poll interval must be between 0.01 and 5 seconds")
        if not 1 <= batch_size <= 1_000:
            raise ValueError("realtime relay batch size must be between 1 and 1000")
        self.foundation = foundation
        self.target = target
        self.instance_id = instance_id or f"relay-{uuid.uuid4().hex}"
        if not 1 <= len(self.instance_id) <= 128:
            raise ValueError("realtime relay instance ID must be between 1 and 128 characters")
        self.lease_seconds = lease_seconds
        self.poll_interval_seconds = poll_interval_seconds
        self.batch_size = batch_size
        self._cursor: int | None = None
        self._next_heartbeat = 0.0
        self._stop = asyncio.Event()
        self._fenced = False

    async def initialize(self) -> int:
        """Register or renew this process cursor and return its acknowledged sequence."""

        self._cursor = await self.foundation.register_instance(
            self.instance_id,
            lease_seconds=self.lease_seconds,
        )
        self._next_heartbeat = asyncio.get_running_loop().time() + self.lease_seconds / 3
        self._fenced = False
        return self._cursor

    async def process_once(self) -> int:
        """Dispatch one ordered batch and acknowledge it only after every local action succeeds."""

        if self._cursor is None:
            raise RuntimeError("realtime relay is not initialized")
        events = await self.foundation.read_events(self.instance_id, limit=self.batch_size)
        for event in events:
            await self._dispatch(event)
        if events:
            self._cursor = await self.foundation.acknowledge_events(
                self.instance_id,
                through_sequence=events[-1].sequence,
            )
        return len(events)

    async def heartbeat(self) -> None:
        """Renew this process lease before it can expire."""

        if self._cursor is None:
            raise RuntimeError("realtime relay is not initialized")
        await self.foundation.heartbeat_instance(self.instance_id, lease_seconds=self.lease_seconds)
        self._next_heartbeat = asyncio.get_running_loop().time() + self.lease_seconds / 3

    async def run(self) -> None:
        """Poll forever, fencing sockets and resuming from the cursor after a lease/database loss."""

        while not self._stop.is_set():
            try:
                if self._cursor is None:
                    await self.initialize()
                if asyncio.get_running_loop().time() >= self._next_heartbeat:
                    await self.heartbeat()
                processed = await self.process_once()
                if processed == self.batch_size:
                    continue
            except (InstanceLeaseError, PostgresUnavailableError) as exc:
                logger.warning("Fencing local sockets after PostgreSQL relay interruption: %s", type(exc).__name__)
                await self._fence()
                self._cursor = None
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.poll_interval_seconds)
            except asyncio.TimeoutError:
                pass

    def stop(self) -> None:
        """Request graceful relay shutdown without closing sockets itself."""

        self._stop.set()

    async def _fence(self) -> None:
        if not self._fenced:
            self._fenced = True
            await self.target.close_all()

    async def _dispatch(self, event: RealtimeEvent) -> None:
        if event.event_type in _BROADCAST_EVENT_TYPES:
            await self.target.broadcast(event.room_id, event.payload)
            return
        if event.event_type == "room.archived":
            await self.target.close_room(event.room_id, event.payload)
            return
        if event.event_type != "member.moderation.updated":
            return
        raw_moderation = event.payload.get("moderation")
        try:
            moderation = MemberModeration.model_validate(raw_moderation)
        except ValidationError:
            return
        if moderation.banned_until is None or moderation.banned_until <= event.created_at:
            return
        await self.target.close_member(
            event.room_id,
            moderation.subject,
            {
                "type": "member.banned",
                "subject": moderation.subject,
                "banned_until": moderation.banned_until.isoformat(),
            },
        )


def _target_contract(target: ConnectionManager) -> RealtimeTarget:
    """Make static analysis prove that the local connection manager is relay-compatible."""

    return target
