"""
Helix Collective — WebSocket Manager
websocket_manager.py — Real-time UCF state broadcasting with Redis pub/sub

Supports cross-instance broadcasting via Redis pub/sub so that messages
published on Railway instance A are relayed to clients connected to instance B.

Author: Andrew John Ward (Architect)
"""

import asyncio
import contextlib
import json
import logging
import os
from datetime import UTC, datetime
from typing import Any

from fastapi import WebSocket, WebSocketDisconnect

logger = logging.getLogger(__name__)

# Maximum concurrent WebSocket connections per instance.
# Prevents resource exhaustion from malicious/accidental connection floods.
MAX_CONNECTIONS = int(os.getenv("WS_MAX_CONNECTIONS", "2000"))

# Maximum concurrent WebSocket connections per user.
# Prevents a single user from hogging all connection slots.
MAX_CONNECTIONS_PER_USER = int(os.getenv("WS_MAX_PER_USER", "10"))

# Redis pub/sub channel for cross-instance broadcasting
_BROADCAST_CHANNEL = "ws:broadcast"


class ConnectionManager:
    """
    Manages WebSocket connections for real-time UCF broadcasting.

    Features:
    - Connection limit to prevent resource exhaustion
    - Redis pub/sub for cross-instance broadcasting
    - Automatic cleanup of dead connections
    - Heartbeat mechanism for connection health
    """

    def __init__(self):
        self.active_connections: set[WebSocket] = set()
        self.connection_metadata: dict[WebSocket, dict[str, Any]] = {}
        self._lock = asyncio.Lock()
        self._redis = None
        self._pubsub = None
        self._listener_task: asyncio.Task | None = None

    async def initialize_redis(self):
        """Set up Redis pub/sub for cross-instance broadcasting.

        Call this once during app lifespan startup.
        """
        redis_url = os.getenv("REDIS_URL")
        if not redis_url:
            logger.warning("REDIS_URL not set — WebSocket broadcasts are instance-local only")
            return

        try:
            import redis.asyncio as aioredis

            self._redis = aioredis.from_url(redis_url, decode_responses=True)
            self._pubsub = self._redis.pubsub()
            await self._pubsub.subscribe(_BROADCAST_CHANNEL)
            from apps.backend.services.background_tasks import create_tracked_task

            self._listener_task = create_tracked_task(self._redis_listener(), name="ws-redis-listener")
            logger.info("Redis pub/sub initialized for WebSocket cross-instance broadcasting")
        except Exception as e:
            logger.warning("Redis pub/sub init failed — broadcasts are instance-local only: %s", e)
            self._redis = None
            self._pubsub = None

    async def shutdown(self):
        """Clean up Redis resources. Call during app lifespan shutdown."""
        if self._listener_task:
            self._listener_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._listener_task
        if self._pubsub:
            await self._pubsub.unsubscribe(_BROADCAST_CHANNEL)
            await self._pubsub.close()
        if self._redis:
            await self._redis.close()

    async def _redis_listener(self):
        """Listen for broadcasts from other instances and relay to local clients."""
        try:
            pubsub = self._pubsub
            if pubsub is None:
                return

            async for raw_message in pubsub.listen():
                if raw_message["type"] != "message":
                    continue
                try:
                    message = json.loads(raw_message["data"])
                    # Relay to local clients (skip re-publishing to avoid loops)
                    await self._broadcast_local(message)
                except (json.JSONDecodeError, TypeError) as e:
                    logger.debug("Ignoring malformed Redis pub/sub message: %s", e)
        except asyncio.CancelledError:
            logger.debug("Redis pub/sub listener task cancelled")
        except Exception as e:
            logger.error("Redis pub/sub listener crashed: %s", e)

    async def _verify_ws_token(self, websocket: WebSocket) -> dict | None:
        """Verify JWT from the shared WebSocket auth helper.

        This keeps WebSocket manager auth aligned with revocation and session
        invalidation checks in the canonical auth stack.
        """
        from apps.backend.core.unified_auth import verify_token_from_websocket

        return await verify_token_from_websocket(websocket)

    async def connect(
        self,
        websocket: WebSocket,
        client_id: str | None = None,
        require_auth: bool = False,
    ):
        """Accept new WebSocket connection with optional JWT authentication.

        Args:
            websocket: The WebSocket connection.
            client_id: Optional client identifier.
            require_auth: If True, reject unauthenticated connections with
                code 4001. Routes that already verify tokens before calling
                connect() can leave this False (default) to avoid double-auth.
        """
        if len(self.active_connections) >= MAX_CONNECTIONS:
            await websocket.close(code=1013, reason="Server at capacity")
            logger.warning("WebSocket connection rejected — at capacity (%s)", MAX_CONNECTIONS)
            return

        # Authenticate the connection
        user_payload = await self._verify_ws_token(websocket)
        if require_auth and user_payload is None:
            await websocket.close(code=4001, reason="Authentication required")
            logger.warning("WebSocket connection rejected — no valid token")
            return

        # Per-user connection limit
        user_id = (user_payload or {}).get("sub") or (user_payload or {}).get("user_id")
        if user_id:
            user_conn_count = sum(1 for meta in self.connection_metadata.values() if meta.get("user_id") == user_id)
            if user_conn_count >= MAX_CONNECTIONS_PER_USER:
                await websocket.close(code=4002, reason="Too many connections for this user")
                logger.warning("WebSocket rejected — user %s at per-user limit (%s)", user_id, MAX_CONNECTIONS_PER_USER)
                return

        await websocket.accept()
        async with self._lock:
            self.active_connections.add(websocket)

            self.connection_metadata[websocket] = {
                "client_id": client_id or f"client_{id(websocket)}",
                "user_id": user_id,
                "connected_at": datetime.now(UTC).isoformat(),
                "message_count": 0,
            }

        logger.info(
            "WebSocket client connected: %s (active: %s)",
            self.connection_metadata[websocket]["client_id"],
            len(self.active_connections),
        )

        await self.send_personal_message(
            {
                "type": "connection",
                "status": "connected",
                "client_id": self.connection_metadata[websocket]["client_id"],
                "active_clients": len(self.active_connections),
                "timestamp": datetime.now(UTC).isoformat(),
            },
            websocket,
        )

    async def disconnect(self, websocket: WebSocket):
        """Remove WebSocket connection and cleanup metadata."""
        if websocket in self.active_connections:
            client_id = self.connection_metadata.get(websocket, {}).get("client_id", "unknown")
            async with self._lock:
                self.active_connections.remove(websocket)
                self.connection_metadata.pop(websocket, None)
            logger.info("WebSocket client disconnected: %s (active: %s)", client_id, len(self.active_connections))

    async def send_personal_message(self, message: dict[str, Any], websocket: WebSocket):
        """Send message to specific client."""
        try:
            await websocket.send_json(message)
            async with self._lock:
                if websocket in self.connection_metadata:
                    self.connection_metadata[websocket]["message_count"] += 1
        except Exception as e:
            logger.error("Error sending personal message: %s", e)
            await self.disconnect(websocket)

    async def _broadcast_local(self, message: dict[str, Any]):
        """Broadcast to local connections only (no Redis publish)."""
        if not self.active_connections:
            return

        disconnected = []
        for connection in self.active_connections.copy():
            try:
                await connection.send_json(message)
                async with self._lock:
                    if connection in self.connection_metadata:
                        self.connection_metadata[connection]["message_count"] += 1
            except Exception as e:
                logger.error("Error broadcasting to client: %s", e)
                disconnected.append(connection)

        for conn in disconnected:
            await self.disconnect(conn)

    async def broadcast(self, message: dict[str, Any], message_type: str = "ucf_update"):
        """
        Broadcast message to all connected clients across all instances.

        If Redis is available, publishes to the broadcast channel so other
        instances relay to their local clients. Falls back to local-only.
        """
        if self._redis:
            try:
                await self._redis.publish(_BROADCAST_CHANNEL, json.dumps(message))
                return  # The Redis listener will handle local delivery too
            except Exception as e:
                logger.warning("Redis publish failed, falling back to local broadcast: %s", e)

        await self._broadcast_local(message)

    async def broadcast_ucf_state(self, ucf_state: dict[str, float]):
        """Broadcast UCF state update to all clients."""
        await self.broadcast(ucf_state, message_type="ucf_update")
        logger.debug("Broadcasted UCF state to %s local clients", len(self.active_connections))

    async def broadcast_agent_status(self, agent_status: dict[str, Any]):
        """Broadcast agent status update to all clients."""
        await self.broadcast(agent_status, message_type="agent_status")
        logger.debug("Broadcasted agent status to %s local clients", len(self.active_connections))

    async def broadcast_event(self, event: dict[str, Any]):
        """Broadcast system event to all clients."""
        await self.broadcast(event, message_type="event")
        logger.info("Broadcasted event to %s local clients", len(self.active_connections))

    def get_connection_stats(self) -> dict[str, Any]:
        """Return statistics about active connections."""
        return {
            "active_connections": len(self.active_connections),
            "max_connections": MAX_CONNECTIONS,
            "redis_connected": self._redis is not None,
            "total_messages_sent": sum(meta["message_count"] for meta in self.connection_metadata.values()),
            "clients": [
                {
                    "client_id": meta["client_id"],
                    "connected_at": meta["connected_at"],
                    "messages_sent": meta["message_count"],
                }
                for meta in self.connection_metadata.values()
            ],
        }


# Global connection manager instance
manager = ConnectionManager()


async def heartbeat_task(websocket: WebSocket, interval: int = 30):
    """
    Send periodic heartbeat pings to keep connection alive.
    Runs as a continuous loop — detects and cleans up dead connections.

    Args:
        websocket: WebSocket connection
        interval: Seconds between heartbeats
    """
    try:
        while True:
            await asyncio.sleep(interval)
            try:
                await asyncio.wait_for(
                    websocket.send_json(
                        {
                            "type": "heartbeat",
                            "timestamp": datetime.now(UTC).isoformat(),
                        }
                    ),
                    timeout=5.0,
                )
            except TimeoutError:
                logger.warning("Heartbeat send timed out — closing stale connection")
                break
    except WebSocketDisconnect:
        logger.debug("Heartbeat stopped: client disconnected")
    except Exception as e:
        logger.debug("Heartbeat stopped: %s", e)
    finally:
        # Ensure the connection is removed from the manager
        try:
            await manager.disconnect(websocket)
        except Exception as e:
            logger.debug("WebSocket disconnect cleanup failed: %s", e)
