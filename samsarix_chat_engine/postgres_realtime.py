# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Durable PostgreSQL event-log relay for one process's local WebSockets."""

from __future__ import annotations

import asyncio
import logging
import uuid
from typing import TYPE_CHECKING, Any, Protocol
from uuid import UUID

from pydantic import ValidationError

from .models import MemberModeration
from .postgres import (
    EventLogGapError,
    EventLogLagError,
    InstanceLeaseError,
    PostgresFoundation,
    PostgresFoundationError,
    RealtimeEvent,
    _validate_lag_limits,
)

if TYPE_CHECKING:
    from .websocket_manager import ConnectionManager

logger = logging.getLogger(__name__)

_BROADCAST_EVENT_TYPES = frozenset(
    {
        "message.created",
        "message.updated",
        "message.deleted",
        "message.pin.updated",
        "message.reaction.updated",
        "room.frozen",
        "room.unfrozen",
        "presence.joined",
        "presence.left",
        "typing.started",
        "typing.stopped",
    }
)


class RealtimeTarget(Protocol):
    """Local socket operations needed by the durable relay."""

    async def broadcast(
        self,
        room_id: str,
        event: dict[str, Any],
        *,
        exclude_connection_id: str | None = None,
        event_sequence: int | None = None,
    ) -> None: ...

    async def close_room(
        self,
        room_id: str,
        event: dict[str, Any],
        *,
        code: int = 4409,
        reason: str = "Room archived",
        event_sequence: int | None = None,
    ) -> None: ...

    async def close_member(
        self,
        room_id: str,
        subject: str,
        event: dict[str, Any],
        *,
        code: int = 4403,
        reason: str = "Room access revoked",
        event_sequence: int | None = None,
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
        max_pending_events: int = 10_000,
        max_event_age_seconds: int = 30,
    ) -> None:
        if not 3 <= lease_seconds <= 300:
            raise ValueError("realtime relay lease must be between 3 and 300 seconds")
        if not 0.01 <= poll_interval_seconds <= 5.0:
            raise ValueError("realtime relay poll interval must be between 0.01 and 5 seconds")
        if not 1 <= batch_size <= 1_000:
            raise ValueError("realtime relay batch size must be between 1 and 1000")
        _validate_lag_limits(max_pending_events, max_event_age_seconds)
        if max_pending_events is None or max_event_age_seconds is None:
            raise ValueError("realtime relay lag limits cannot be disabled")
        self.foundation = foundation
        self.target = target
        self.instance_id = instance_id if instance_id is not None else f"relay-{uuid.uuid4().hex}"
        if not 1 <= len(self.instance_id) <= 128:
            raise ValueError("realtime relay instance ID must be between 1 and 128 characters")
        self.lease_seconds = lease_seconds
        self.poll_interval_seconds = poll_interval_seconds
        self.batch_size = batch_size
        self.max_pending_events = max_pending_events
        self.max_event_age_seconds = max_event_age_seconds
        self._cursor: int | None = None
        self._generation: UUID | None = None
        self._next_heartbeat = 0.0
        self._lease_deadline = 0.0
        self._admission_epoch = 0
        self._stop = asyncio.Event()
        self._fenced = False
        self._fence_required = False
        self._gap_recovery_required = False
        self._lag_recovery_required = False
        self._recovery_generation: UUID | None = None

    @property
    def ready(self) -> bool:
        """Require a locally unexpired claim and no pending socket fence or recovery."""

        return (
            self._cursor is not None
            and self._generation is not None
            and not self._fenced
            and not self._fence_required
            and not self._gap_recovery_required
            and not self._lag_recovery_required
            and self._recovery_generation is None
            and not self._stop.is_set()
            and asyncio.get_running_loop().time() < self._lease_deadline
        )

    @property
    def admission_token(self) -> tuple[UUID, int] | None:
        """Identify one uninterrupted local admission window, including same-claim recovery."""

        if not self.ready or self._generation is None:
            return None
        return self._generation, self._admission_epoch

    async def initialize(self) -> int:
        """Register or renew this process cursor and return its acknowledged sequence."""

        self._stop.clear()
        started = asyncio.get_running_loop().time()
        registration = await self.foundation.claim_instance(
            self.instance_id,
            lease_seconds=self.lease_seconds,
            generation=self._generation,
        )
        self._generation = registration.generation
        self._admission_epoch += 1
        self._cursor = registration.last_sequence
        self._lease_deadline = started + self.lease_seconds
        self._next_heartbeat = asyncio.get_running_loop().time() + self.lease_seconds / 3
        self._fenced = False
        self._fence_required = False
        return self._cursor

    async def process_once(self) -> int:
        """Dispatch one ordered batch and checkpoint its last successful local action."""

        if self._cursor is None:
            raise RuntimeError("realtime relay is not initialized")
        if self._generation is None:
            raise RuntimeError("realtime relay generation is not initialized")
        events = await self.foundation.read_events(
            self.instance_id,
            limit=self.batch_size,
            generation=self._generation,
            max_pending_events=self.max_pending_events,
            max_event_age_seconds=self.max_event_age_seconds,
        )
        dispatched_sequence: int | None = None
        try:
            for event in events:
                await self._dispatch(event)
                dispatched_sequence = event.sequence
        finally:
            if dispatched_sequence is not None:
                self._cursor = await self.foundation.acknowledge_events(
                    self.instance_id,
                    through_sequence=dispatched_sequence,
                    generation=self._generation,
                )
        return len(events)

    async def heartbeat(self) -> None:
        """Renew this process lease before it can expire."""

        if self._cursor is None:
            raise RuntimeError("realtime relay is not initialized")
        if self._generation is None:
            raise RuntimeError("realtime relay generation is not initialized")
        started = asyncio.get_running_loop().time()
        await self.foundation.heartbeat_claimed_instance(
            self.instance_id,
            generation=self._generation,
            lease_seconds=self.lease_seconds,
        )
        self._lease_deadline = started + self.lease_seconds
        self._next_heartbeat = asyncio.get_running_loop().time() + self.lease_seconds / 3

    async def run(self) -> None:
        """Poll forever, fencing sockets and resuming from the cursor after a lease/database loss."""

        while not self._stop.is_set():
            if (
                self._fence_required or self._gap_recovery_required or self._lag_recovery_required
            ) and not await self._fence():
                await self._wait_for_work()
                continue
            try:
                if self._gap_recovery_required or self._lag_recovery_required:
                    if self._generation is None:
                        raise RuntimeError("realtime relay generation is not initialized")
                    started = asyncio.get_running_loop().time()
                    if self._recovery_generation is None:
                        self._recovery_generation = uuid.uuid4()
                    registration = await self.foundation.resynchronize_claimed_instance(
                        self.instance_id,
                        generation=self._generation,
                        recovery_generation=self._recovery_generation,
                        lease_seconds=self.lease_seconds,
                    )
                    self._generation = registration.generation
                    self._admission_epoch += 1
                    self._cursor = registration.last_sequence
                    self._lease_deadline = started + self.lease_seconds
                    self._next_heartbeat = asyncio.get_running_loop().time() + self.lease_seconds / 3
                    self._gap_recovery_required = False
                    self._lag_recovery_required = False
                    self._recovery_generation = None
                    self._fenced = False
                if self._cursor is None:
                    await self.initialize()
                if asyncio.get_running_loop().time() >= self._next_heartbeat:
                    await self.heartbeat()
                processed = await self.process_once()
                if processed == self.batch_size:
                    continue
            except EventLogLagError as exc:
                logger.warning("Fencing local sockets after PostgreSQL relay lag exceeded its %s limit", exc.reason)
                self._cursor = None
                self._lag_recovery_required = True
                self._fence_required = True
                await self._fence()
            except EventLogGapError:
                logger.warning("Fencing local sockets after a retained PostgreSQL event-log gap")
                self._cursor = None
                self._gap_recovery_required = True
                self._fence_required = True
                await self._fence()
            except InstanceLeaseError:
                # A different owner may have claimed the ID during an outage.
                # Do not retry a stale recovery UUID forever or overwrite it.
                logger.warning("Fencing local sockets after the PostgreSQL instance claim became unusable")
                self._cursor = None
                self._gap_recovery_required = False
                self._lag_recovery_required = False
                self._recovery_generation = None
                self._fence_required = True
                await self._fence()
            except PostgresFoundationError as exc:
                logger.warning("Fencing local sockets after PostgreSQL relay interruption: %s", type(exc).__name__)
                self._cursor = None
                self._fence_required = True
                await self._fence()
            except Exception as exc:
                logger.error("Local realtime dispatch failed; retrying from the durable cursor: %s", type(exc).__name__)
            await self._wait_for_work()

    def stop(self) -> None:
        """Request graceful relay shutdown without closing sockets itself."""

        self._stop.set()

    async def release(self) -> bool:
        """Expire this exact process generation after its relay loop stops."""

        if self._generation is None:
            return False
        return await self.foundation.release_instance(
            self.instance_id,
            generation=self._generation,
        )

    async def _wait_for_work(self) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=self.poll_interval_seconds)
        except asyncio.TimeoutError:
            pass

    async def _fence(self) -> bool:
        if self._fenced:
            self._fence_required = False
            return True
        self._fence_required = True
        self._admission_epoch += 1
        try:
            await self.target.close_all()
        except Exception as exc:
            logger.error("Local socket fencing failed and will be retried: %s", type(exc).__name__)
            return False
        self._fenced = True
        self._fence_required = False
        return True

    async def _dispatch(self, event: RealtimeEvent) -> None:
        if event.event_type in _BROADCAST_EVENT_TYPES:
            payload = dict(event.payload)
            origin_connection_id = payload.pop("origin_connection_id", None)
            if event.event_type == "message.created":
                payload.setdefault("idempotent_replay", False)
            await self.target.broadcast(
                event.room_id,
                payload,
                exclude_connection_id=(str(origin_connection_id) if origin_connection_id is not None else None),
                event_sequence=event.sequence,
            )
            return
        if event.event_type == "room.archived":
            await self.target.close_room(event.room_id, event.payload, event_sequence=event.sequence)
            return
        if event.event_type != "member.moderation.updated":
            return
        raw_moderation = event.payload.get("moderation")
        try:
            moderation = MemberModeration.model_validate(raw_moderation)
        except ValidationError:
            logger.warning(
                "Discarding malformed moderation event for room %s at sequence %d",
                event.room_id,
                event.sequence,
            )
            return
        if moderation.banned_until is None:
            return
        if moderation.banned_until.tzinfo is None or moderation.banned_until.utcoffset() is None:
            logger.warning(
                "Discarding timezone-naive moderation event for room %s at sequence %d",
                event.room_id,
                event.sequence,
            )
            return
        if moderation.banned_until <= event.created_at:
            return
        await self.target.close_member(
            event.room_id,
            moderation.subject,
            {
                "type": "member.banned",
                "subject": moderation.subject,
                "banned_until": moderation.banned_until.isoformat(),
            },
            event_sequence=event.sequence,
        )


def _target_contract(target: ConnectionManager) -> RealtimeTarget:
    """Make static analysis prove that the local connection manager is relay-compatible."""

    return target
