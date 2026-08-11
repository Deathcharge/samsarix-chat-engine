# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""FastAPI application factory and the complete room-chat protocol."""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import time
import uuid
from collections import defaultdict, deque
from collections.abc import AsyncIterator, Iterator, Mapping
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Annotated, Any, Literal, Protocol
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, FastAPI, Header, Path, Query, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.security import APIKeyHeader, HTTPAuthorizationCredentials, HTTPBearer
from pydantic import TypeAdapter, ValidationError
from starlette.background import BackgroundTask
from starlette.types import ASGIApp, Receive, Scope, Send
from starlette.types import Message as ASGIMessage

if TYPE_CHECKING:
    from .postgres_runtime import PostgresApplicationRuntime

from .auth import (
    AccessTokenService,
    AccessTokenVerifier,
    AuthenticationError,
    JWKSAccessTokenVerifier,
    Permission,
    Principal,
    credentials_match,
)
from .config import ConfigurationError, Settings, decode_webhook_secret
from .models import (
    AuditEventPage,
    MemberModeration,
    MemberModerationUpdate,
    Message,
    MessageCreate,
    MessagePage,
    MessageUpdate,
    ReadState,
    ReadStateUpdate,
    RetentionResult,
    Room,
    RoomCreate,
    RoomUpdate,
    WebhookDelivery,
    WebhookDeliveryPage,
    WebSocketAuth,
    WebSocketMessage,
    WebSocketPing,
    WebSocketTyping,
)
from .store import (
    ChatStorage,
    ChatStore,
    DatabaseLifecycleLock,
    InvalidAuditCursorError,
    InvalidCursorError,
    InvalidSearchQueryError,
    InvalidWebhookCursorError,
    MemberBannedError,
    MemberMutedError,
    MessageDeletedError,
    MessageNotFoundError,
    MessageOwnershipError,
    ReadStateCapacityError,
    RetentionNotConfiguredError,
    RoomAlreadyExistsError,
    RoomArchivedError,
    RoomCapacityError,
    RoomFrozenError,
    RoomNotArchivedError,
    RoomNotFoundError,
    WebhookCapacityError,
    WebhookDeliveryNotFoundError,
    WebhookPayloadUnavailableError,
    normalize_search_query,
)
from .webhooks import WebhookDispatcher
from .websocket_manager import ConnectionManager

logger = logging.getLogger(__name__)
_WS_COMMAND: TypeAdapter[WebSocketMessage | WebSocketPing | WebSocketTyping] = TypeAdapter(
    WebSocketMessage | WebSocketPing | WebSocketTyping
)
_WS_AUTH: TypeAdapter[WebSocketAuth] = TypeAdapter(WebSocketAuth)
_API_KEY_SCHEME = APIKeyHeader(name="X-API-Key", scheme_name="OperatorKey", auto_error=False)
_BEARER_SCHEME = HTTPBearer(scheme_name="AccessToken", auto_error=False)


class APIError(Exception):
    """Internal exception mapped to the stable public error envelope."""

    def __init__(self, status_code: int, code: str, message: str, *, headers: dict[str, str] | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.headers = headers or {}


class MessageRateLimiter:
    """Bounded in-memory sliding-window limiter for one service instance."""

    def __init__(self, limit: int, *, window_seconds: float = 60.0, max_keys: int = 10_000) -> None:
        self.limit = limit
        self.window_seconds = window_seconds
        self.max_keys = max_keys
        self._events: dict[str, deque[float]] = defaultdict(deque)
        self._lock = asyncio.Lock()

    async def allow(self, key: str) -> bool:
        now = time.monotonic()
        cutoff = now - self.window_seconds
        async with self._lock:
            events = self._events[key]
            while events and events[0] <= cutoff:
                events.popleft()
            if len(events) >= self.limit:
                return False
            events.append(now)
            if len(self._events) > self.max_keys:
                self._discard_inactive(cutoff)
            return True

    def _discard_inactive(self, cutoff: float) -> None:
        inactive = [key for key, events in self._events.items() if not events or events[-1] <= cutoff]
        for key in inactive:
            self._events.pop(key, None)
        while len(self._events) > self.max_keys:
            self._events.pop(next(iter(self._events)))


class RateLimiter(Protocol):
    """Shared request-path contract for local and PostgreSQL limiters."""

    async def allow(self, key: str) -> bool: ...


class RequestBodyTooLarge(Exception):
    """Internal control-flow exception for streamed/chunked oversized bodies."""


class RequestBodyLimitMiddleware:
    """Reject oversized HTTP bodies before Pydantic retains their contents."""

    def __init__(self, app: ASGIApp, max_body_bytes: int) -> None:
        self.app = app
        self.max_body_bytes = max_body_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        headers = dict(scope.get("headers", ()))
        content_length = headers.get(b"content-length")
        if content_length is not None:
            try:
                if int(content_length) > self.max_body_bytes:
                    await self._reject(scope, receive, send)
                    return
            except ValueError:
                pass

        received = 0

        async def limited_receive() -> ASGIMessage:
            nonlocal received
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > self.max_body_bytes:
                    raise RequestBodyTooLarge
            return message

        try:
            await self.app(scope, limited_receive, send)
        except RequestBodyTooLarge:
            await self._reject(scope, receive, send)

    @staticmethod
    async def _reject(scope: Scope, receive: Receive, send: Send) -> None:
        response = JSONResponse(
            status_code=413,
            content=_error_payload("request_too_large", "Request body is too large"),
        )
        await response(scope, receive, send)


def _bearer_from_headers(headers: Mapping[str, str]) -> str | None:
    authorization = headers.get("authorization", "")
    scheme, _, value = authorization.partition(" ")
    if scheme.lower() == "bearer" and value:
        return value
    return None


def _authenticate_credentials(
    *,
    api_key: str | None,
    bearer: str | None,
    settings: Settings,
    token_service: AccessTokenVerifier | None,
) -> Principal:
    if settings.api_key is None and token_service is None:
        return Principal.local_operator()
    operator_credential = api_key or bearer
    if credentials_match(operator_credential, settings.api_key):
        return Principal.api_key_operator()
    if bearer is not None and token_service is not None:
        return token_service.verify(bearer)
    raise AuthenticationError("invalid credential")


def _client_key(request: Request) -> str:
    return request.client.host if request.client else "unknown"


def _audit_actor(principal: Principal) -> str:
    if principal.subject is not None:
        return principal.subject
    return "operator-api-key" if principal.authentication == "api_key" else "local-operator"


APIKeyCredential = Annotated[str | None, Depends(_API_KEY_SCHEME)]
BearerCredential = Annotated[HTTPAuthorizationCredentials | None, Depends(_BEARER_SCHEME)]


async def _request_principal(
    request: Request,
    api_key: APIKeyCredential,
    bearer: BearerCredential,
) -> Principal:
    settings: Settings = request.app.state.settings
    try:
        return _authenticate_credentials(
            api_key=api_key,
            bearer=bearer.credentials if bearer is not None else None,
            settings=settings,
            token_service=request.app.state.token_service,
        )
    except AuthenticationError as exc:
        raise APIError(401, "authentication_required", "A valid API key or access token is required") from exc


PrincipalDependency = Annotated[Principal, Depends(_request_principal)]


def _authorize(principal: Principal, permission: Permission, room_id: str | None = None) -> None:
    if not principal.allows(permission, room_id):
        raise APIError(403, "authorization_denied", "This credential does not grant the required access")


def _stable_subject(principal: Principal) -> str:
    if principal.subject is None:
        raise APIError(403, "stable_subject_required", "A signed access token is required for persistent read state")
    return principal.subject


async def _enforce_member_access(
    store: ChatStorage,
    principal: Principal,
    room_id: str,
    *,
    write: bool,
) -> None:
    """Apply room moderation to stable token subjects; operators always bypass it."""

    if principal.is_admin or principal.subject is None:
        return
    moderation = await store.get_member_moderation(room_id, principal.subject)
    if moderation is None:
        return
    now = datetime.now(timezone.utc)
    if moderation.banned_until is not None and moderation.banned_until > now:
        raise APIError(403, "room_banned", "This account is banned from the room")
    if write and moderation.muted_until is not None and moderation.muted_until > now:
        raise APIError(403, "room_muted", "This account is muted in the room")


def _error_payload(code: str, message: str, **details: Any) -> dict[str, Any]:
    error: dict[str, Any] = {"code": code, "message": message}
    if details:
        error["details"] = details
    return {"error": error}


def _event(event_type: str, **payload: Any) -> dict[str, Any]:
    return {"type": event_type, **payload}


_MESSAGE_WRITE_ERRORS: tuple[tuple[type[Exception], str, str, int | None, str | None], ...] = (
    (RoomArchivedError, "room_archived", "Archived rooms are read-only", 4409, "Room archived"),
    (
        RoomFrozenError,
        "room_frozen",
        "Only administrators may publish while the room is frozen",
        None,
        None,
    ),
    (MemberBannedError, "room_banned", "This account is banned from the room", 4403, "Room access revoked"),
    (MemberMutedError, "room_muted", "This account is muted in the room", None, None),
    (WebhookCapacityError, "webhook_capacity_reached", "Webhook delivery capacity reached", None, None),
    (RoomNotFoundError, "room_not_found", "Room not found", 4404, "Room not found"),
)


def _message_write_error(exc: Exception) -> tuple[str, str, int | None, str | None]:
    """Resolve a known persistence rejection into one WebSocket action."""

    for error_type, code, message, close_code, reason in _MESSAGE_WRITE_ERRORS:
        if isinstance(exc, error_type):
            return code, message, close_code, reason
    raise TypeError(f"unsupported message write error: {type(exc).__name__}")


def _websocket_origin_allowed(websocket: WebSocket, settings: Settings) -> bool:
    origin = websocket.headers.get("origin")
    if origin is None:
        return True
    normalized = origin.rstrip("/")
    if settings.allowed_origins:
        return normalized in settings.allowed_origins
    try:
        hostname = urlparse(normalized).hostname
    except ValueError:
        return False
    return hostname in {"localhost", "127.0.0.1", "::1"}


async def _authenticate_websocket(
    websocket: WebSocket,
    settings: Settings,
    token_service: AccessTokenVerifier | None,
) -> Principal | None:
    header_api_key = websocket.headers.get("x-api-key")
    header_bearer = _bearer_from_headers(websocket.headers)
    if settings.api_key is None and token_service is None:
        return Principal.local_operator()
    if header_api_key is not None or header_bearer is not None:
        try:
            return _authenticate_credentials(
                api_key=header_api_key,
                bearer=header_bearer,
                settings=settings,
                token_service=token_service,
            )
        except AuthenticationError:
            await websocket.send_json(_event("error", code="authentication_failed", message="Authentication failed"))
            await websocket.close(code=4401, reason="Authentication required")
            return None

    await websocket.send_json(
        _event(
            "auth.required",
            message="Send an auth command before any chat commands",
            example={"type": "auth", "token": "..."},
        )
    )
    try:
        raw = await asyncio.wait_for(
            websocket.receive_text(),
            timeout=settings.websocket_auth_timeout_seconds,
        )
        command = _WS_AUTH.validate_json(raw)
    except (asyncio.TimeoutError, ValidationError, ValueError, WebSocketDisconnect):
        await websocket.send_json(_event("error", code="authentication_failed", message="Authentication failed"))
        await websocket.close(code=4401, reason="Authentication required")
        return None
    try:
        return _authenticate_credentials(
            api_key=command.api_key,
            bearer=command.token,
            settings=settings,
            token_service=token_service,
        )
    except AuthenticationError:
        await websocket.send_json(_event("error", code="authentication_failed", message="Authentication failed"))
        await websocket.close(code=4401, reason="Authentication required")
        return None


def create_app(settings: Settings | None = None) -> FastAPI:
    """Create an isolated chat service using explicit or environment settings."""

    resolved = settings or Settings.from_env()
    manager = ConnectionManager(
        max_connections=resolved.max_connections,
        max_per_room=resolved.max_connections_per_room,
        send_timeout=resolved.websocket_send_timeout_seconds,
    )
    postgres_runtime: PostgresApplicationRuntime | None = None
    lifecycle_lock: DatabaseLifecycleLock | None = None
    limiter: RateLimiter
    search_limiter: RateLimiter
    typing_limiter: RateLimiter
    if resolved.storage_backend == "postgres":
        try:
            from .postgres_runtime import PostgresApplicationRuntime
        except ModuleNotFoundError as exc:
            raise ConfigurationError(
                "PostgreSQL storage requires the samsarix-chat-engine[postgres] installation extra"
            ) from exc
        postgres_url = resolved.postgres_url
        postgres_instance_id = resolved.postgres_instance_id
        if postgres_url is None or postgres_instance_id is None:
            raise ConfigurationError("PostgreSQL storage configuration is incomplete")
        postgres_runtime = PostgresApplicationRuntime(
            postgres_url,
            manager,
            instance_id=postgres_instance_id,
            max_rooms=resolved.max_rooms,
            max_stored_messages=resolved.max_stored_messages,
            max_stored_messages_per_room=resolved.max_stored_messages_per_room,
            max_read_states_per_room=resolved.max_read_states_per_room,
            message_retention_days=resolved.message_retention_days,
            max_audit_events=resolved.max_audit_events,
            webhook_events=resolved.webhook_events,
            max_webhook_deliveries=resolved.max_webhook_deliveries,
            max_connections=resolved.max_connections,
            max_connections_per_room=resolved.max_connections_per_room,
            messages_per_minute=resolved.messages_per_minute,
            searches_per_minute=resolved.searches_per_minute,
            typing_events_per_minute=resolved.typing_events_per_minute,
            typing_timeout_seconds=resolved.typing_timeout_seconds,
            min_pool_size=resolved.postgres_min_pool_size,
            max_pool_size=resolved.postgres_max_pool_size,
            pool_timeout_seconds=resolved.postgres_pool_timeout_seconds,
            lease_seconds=resolved.postgres_lease_seconds,
            relay_poll_interval_seconds=resolved.postgres_relay_poll_seconds,
            maintenance_interval_seconds=resolved.postgres_maintenance_interval_seconds,
            max_rate_buckets=resolved.postgres_max_rate_buckets,
            max_realtime_events=resolved.postgres_max_realtime_events,
            realtime_event_max_age_seconds=resolved.postgres_realtime_event_max_age_seconds,
        )
        store: ChatStorage = postgres_runtime.store
        limiter = postgres_runtime.message_limiter
        search_limiter = postgres_runtime.search_limiter
        typing_limiter = postgres_runtime.typing_limiter
    else:
        store = ChatStore(
            resolved.database_path,
            max_rooms=resolved.max_rooms,
            max_stored_messages=resolved.max_stored_messages,
            max_stored_messages_per_room=resolved.max_stored_messages_per_room,
            max_read_states_per_room=resolved.max_read_states_per_room,
            message_retention_days=resolved.message_retention_days,
            max_audit_events=resolved.max_audit_events,
            webhook_events=resolved.webhook_events,
            max_webhook_deliveries=resolved.max_webhook_deliveries,
        )
        lifecycle_lock = DatabaseLifecycleLock(resolved.database_path)
        limiter = MessageRateLimiter(resolved.messages_per_minute)
        search_limiter = MessageRateLimiter(resolved.searches_per_minute)
        typing_limiter = MessageRateLimiter(resolved.typing_events_per_minute)
    token_service: AccessTokenVerifier | None = (
        AccessTokenService(
            resolved.token_signing_secret,
            issuer=resolved.token_issuer,
            audience=resolved.token_audience,
            max_lifetime_seconds=resolved.token_max_lifetime_seconds,
            clock_skew_seconds=resolved.token_clock_skew_seconds,
        )
        if resolved.token_signing_secret is not None
        else None
    )
    if resolved.token_verification_jwks_path is not None:
        token_service = JWKSAccessTokenVerifier.from_file(
            resolved.token_verification_jwks_path,
            issuer=resolved.token_issuer,
            audience=resolved.token_audience,
            max_lifetime_seconds=resolved.token_max_lifetime_seconds,
            clock_skew_seconds=resolved.token_clock_skew_seconds,
        )
    webhook_dispatcher = None
    if resolved.webhook_url is not None and resolved.webhook_signing_secret is not None:
        secrets = [decode_webhook_secret(resolved.webhook_signing_secret)]
        if resolved.webhook_previous_signing_secret is not None:
            secrets.append(decode_webhook_secret(resolved.webhook_previous_signing_secret))
        webhook_dispatcher = WebhookDispatcher(
            store,
            url=resolved.webhook_url,
            secrets=tuple(secrets),
            timeout=resolved.webhook_timeout_seconds,
            max_attempts=resolved.webhook_max_attempts,
            allow_private_targets=resolved.webhook_allow_private_targets,
        )

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        webhook_task: asyncio.Task[None] | None = None
        if lifecycle_lock is not None:
            lifecycle_lock.acquire()
        try:
            if postgres_runtime is not None:
                await postgres_runtime.open()
            else:
                await store.initialize()
            application.state.settings = resolved
            application.state.store = store
            application.state.connections = manager
            application.state.message_limiter = limiter
            application.state.search_limiter = search_limiter
            application.state.typing_limiter = typing_limiter
            application.state.token_service = token_service
            application.state.webhook_dispatcher = webhook_dispatcher
            application.state.postgres_runtime = postgres_runtime
            if webhook_dispatcher is not None:
                webhook_task = asyncio.create_task(webhook_dispatcher.run(), name="samsarix-webhook-dispatcher")
            yield
        finally:
            try:
                if webhook_dispatcher is not None:
                    webhook_dispatcher.stop()
                if webhook_task is not None:
                    await webhook_task
            finally:
                try:
                    await manager.close_all()
                finally:
                    try:
                        if postgres_runtime is not None:
                            await postgres_runtime.close()
                        else:
                            await store.close()
                    finally:
                        if lifecycle_lock is not None:
                            lifecycle_lock.release()

    application = FastAPI(
        title="Samsarix Chat Engine",
        version="0.12.0",
        summary="A small persisted room-chat service with WebSocket delivery",
        lifespan=lifespan,
    )
    application.state.settings = resolved
    application.state.store = store
    application.state.connections = manager
    application.state.message_limiter = limiter
    application.state.search_limiter = search_limiter
    application.state.typing_limiter = typing_limiter
    application.state.token_service = token_service
    application.state.webhook_dispatcher = webhook_dispatcher
    application.state.postgres_runtime = postgres_runtime

    application.add_middleware(
        RequestBodyLimitMiddleware,
        max_body_bytes=max(16_384, resolved.max_message_chars * 12 + 8_192),
    )

    if resolved.allowed_origins:
        application.add_middleware(
            CORSMiddleware,
            allow_origins=list(resolved.allowed_origins),
            allow_credentials=False,
            allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
            allow_headers=[
                "Authorization",
                "Content-Type",
                "Idempotency-Key",
                "X-API-Key",
                "X-Confirm-Room-Delete",
            ],
        )

    @application.middleware("http")
    async def response_headers(request: Request, call_next: Any) -> Response:
        request_id = uuid.uuid4().hex
        response: Response = await call_next(request)
        response.headers.setdefault("Cache-Control", "no-store")
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers["X-Request-ID"] = request_id
        return response

    @application.exception_handler(APIError)
    async def handle_api_error(_request: Request, exc: APIError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content=_error_payload(exc.code, exc.message),
            headers=exc.headers,
        )

    @application.exception_handler(RequestValidationError)
    async def handle_validation_error(_request: Request, exc: RequestValidationError) -> JSONResponse:
        issues = [
            {"location": list(issue["loc"]), "message": issue["msg"], "type": issue["type"]} for issue in exc.errors()
        ]
        return JSONResponse(
            status_code=422,
            content=_error_payload("invalid_request", "Request validation failed", issues=issues),
        )

    @application.exception_handler(sqlite3.Error)
    async def handle_database_error(_request: Request, exc: sqlite3.Error) -> JSONResponse:
        logger.error("SQLite operation failed: %s", type(exc).__name__)
        return JSONResponse(
            status_code=503,
            content=_error_payload("storage_unavailable", "Chat storage is temporarily unavailable"),
        )

    if postgres_runtime is not None:
        from .postgres import PostgresFoundationError

        @application.exception_handler(PostgresFoundationError)
        async def handle_postgres_error(_request: Request, exc: PostgresFoundationError) -> JSONResponse:
            logger.error("PostgreSQL operation failed: %s", type(exc).__name__)
            return JSONResponse(
                status_code=503,
                content=_error_payload("storage_unavailable", "Chat storage is temporarily unavailable"),
            )

    @application.get("/", include_in_schema=False)
    async def index() -> dict[str, Any]:
        return {
            "name": "Samsarix Chat Engine",
            "version": "0.12.0",
            "status": "ok",
            "docs": "/docs",
            "health": "/healthz",
        }

    @application.get("/healthz", tags=["operations"])
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @application.get("/readyz", tags=["operations"])
    async def readiness() -> JSONResponse:
        ready = await postgres_runtime.check_ready() if postgres_runtime is not None else await store.check_ready()
        return JSONResponse(status_code=200 if ready else 503, content={"status": "ready" if ready else "not_ready"})

    router = APIRouter(prefix="/v1")

    @router.post("/rooms", response_model=Room, status_code=201, tags=["rooms"])
    async def create_room(payload: RoomCreate, response: Response, principal: PrincipalDependency) -> Room:
        _authorize(principal, "admin")
        try:
            room = await store.create_room(payload, actor=_audit_actor(principal))
        except RoomAlreadyExistsError as exc:
            raise APIError(409, "room_already_exists", "A room with that ID already exists") from exc
        except RoomCapacityError as exc:
            raise APIError(507, "room_capacity_reached", "The configured room capacity has been reached") from exc
        response.headers["Location"] = f"/v1/rooms/{room.id}"
        return room

    @router.get("/rooms", response_model=list[Room], tags=["rooms"])
    async def list_rooms(principal: PrincipalDependency, limit: int = Query(default=100, ge=1, le=100)) -> list[Room]:
        _authorize(principal, "admin")
        return await store.list_rooms(limit=limit)

    @router.get("/rooms/{room_id}", response_model=Room, tags=["rooms"])
    async def get_room(room_id: str, principal: PrincipalDependency) -> Room:
        _authorize(principal, "room:read", room_id)
        room = await store.get_room(room_id)
        if room is None:
            raise APIError(404, "room_not_found", "Room not found")
        await _enforce_member_access(store, principal, room_id, write=False)
        return room

    @router.patch("/rooms/{room_id}", response_model=Room, tags=["rooms"])
    async def update_room(room_id: str, payload: RoomUpdate, principal: PrincipalDependency) -> Room:
        _authorize(principal, "admin")
        try:
            room, changes = await store.set_room_state(
                room_id,
                archived=payload.archived,
                frozen=payload.frozen,
                actor=_audit_actor(principal),
            )
        except RoomNotFoundError as exc:
            raise APIError(404, "room_not_found", "Room not found") from exc
        if postgres_runtime is None and "archived" in changes and payload.archived:
            await manager.close_room(
                room_id,
                _event("room.archived", room=room.model_dump(mode="json")),
            )
        elif postgres_runtime is None and "frozen" in changes:
            await manager.broadcast(
                room_id,
                _event("room.frozen" if payload.frozen else "room.unfrozen", room=room.model_dump(mode="json")),
            )
        return room

    @router.delete("/rooms/{room_id}", status_code=204, tags=["rooms"])
    async def delete_room(
        room_id: str,
        principal: PrincipalDependency,
        confirmation: str | None = Header(default=None, alias="X-Confirm-Room-Delete", max_length=64),
    ) -> Response:
        _authorize(principal, "admin")
        if confirmation != room_id:
            raise APIError(
                400,
                "deletion_confirmation_required",
                "Repeat the room ID in X-Confirm-Room-Delete",
            )
        try:
            await store.delete_room(room_id, actor=_audit_actor(principal))
        except RoomNotFoundError as exc:
            raise APIError(404, "room_not_found", "Room not found") from exc
        except RoomNotArchivedError as exc:
            raise APIError(409, "room_not_archived", "Archive the room before deleting it") from exc
        return Response(status_code=204)

    @router.get("/rooms/{room_id}/export", tags=["rooms"])
    async def export_room(room_id: str, principal: PrincipalDependency) -> StreamingResponse:
        _authorize(principal, "admin")
        room = await store.get_room(room_id)
        if room is None:
            raise APIError(404, "room_not_found", "Room not found")
        exported_at = datetime.now(timezone.utc)
        try:
            messages = await store.prepare_export(room_id, actor=_audit_actor(principal))
        except RoomNotFoundError as exc:
            raise APIError(404, "room_not_found", "Room not found") from exc

        def export_lines() -> Iterator[str]:
            metadata = {
                "type": "samsarix.room_export",
                "schema_version": 2,
                "exported_at": exported_at.isoformat(),
                "room": room.model_dump(mode="json"),
            }
            yield json.dumps(metadata, separators=(",", ":"), sort_keys=True) + "\n"
            for message in messages:
                yield (
                    json.dumps(
                        {"type": "message", "message": message.model_dump(mode="json")},
                        separators=(",", ":"),
                        sort_keys=True,
                    )
                    + "\n"
                )

        return StreamingResponse(
            export_lines(),
            media_type="application/x-ndjson",
            headers={"Content-Disposition": f'attachment; filename="{room_id}-messages.ndjson"'},
            background=BackgroundTask(messages.close),
        )

    @router.get("/rooms/{room_id}/messages", response_model=MessagePage, tags=["messages"])
    async def list_messages(
        room_id: str,
        principal: PrincipalDependency,
        limit: int = Query(default=50, ge=1, le=100),
        before: str | None = Query(default=None, min_length=1, max_length=128),
    ) -> MessagePage:
        _authorize(principal, "room:read", room_id)
        await _enforce_member_access(store, principal, room_id, write=False)
        try:
            items, next_before = await store.list_messages(room_id, limit=limit, before=before)
        except RoomNotFoundError as exc:
            raise APIError(404, "room_not_found", "Room not found") from exc
        except InvalidCursorError as exc:
            raise APIError(400, "invalid_cursor", "The message cursor is not valid for this room") from exc
        return MessagePage(items=items, next_before=next_before)

    @router.get("/rooms/{room_id}/messages/search", response_model=MessagePage, tags=["messages"])
    async def search_messages(
        request: Request,
        room_id: str,
        principal: PrincipalDependency,
        q: str = Query(min_length=1, max_length=100),
        limit: int = Query(default=50, ge=1, le=100),
        before: str | None = Query(default=None, min_length=1, max_length=128),
    ) -> MessagePage:
        _authorize(principal, "room:read", room_id)
        await _enforce_member_access(store, principal, room_id, write=False)
        try:
            normalized_query = normalize_search_query(q)
        except InvalidSearchQueryError as exc:
            raise APIError(
                422,
                "invalid_search_query",
                "Search queries must contain 2 to 100 normalized characters",
            ) from exc
        rate_subject = principal.subject or _client_key(request)
        if not await search_limiter.allow(f"search:{rate_subject}"):
            raise APIError(
                429,
                "search_rate_limit_exceeded",
                "Message search rate limit exceeded",
                headers={"Retry-After": "60"},
            )
        try:
            items, next_before = await store.search_messages(room_id, normalized_query, limit=limit, before=before)
        except RoomNotFoundError as exc:
            raise APIError(404, "room_not_found", "Room not found") from exc
        except InvalidCursorError as exc:
            raise APIError(400, "invalid_cursor", "The message cursor is not valid for this room") from exc
        return MessagePage(items=items, next_before=next_before)

    @router.get("/rooms/{room_id}/read-state", response_model=ReadState, tags=["messages"])
    async def get_read_state(room_id: str, principal: PrincipalDependency) -> ReadState:
        _authorize(principal, "room:read", room_id)
        await _enforce_member_access(store, principal, room_id, write=False)
        subject = _stable_subject(principal)
        try:
            return await store.get_read_state(room_id, subject)
        except RoomNotFoundError as exc:
            raise APIError(404, "room_not_found", "Room not found") from exc

    @router.put("/rooms/{room_id}/read-state", response_model=ReadState, tags=["messages"])
    async def mark_read(room_id: str, payload: ReadStateUpdate, principal: PrincipalDependency) -> ReadState:
        _authorize(principal, "room:read", room_id)
        await _enforce_member_access(store, principal, room_id, write=False)
        subject = _stable_subject(principal)
        try:
            return await store.mark_read(room_id, subject, payload.message_id)
        except RoomNotFoundError as exc:
            raise APIError(404, "room_not_found", "Room not found") from exc
        except MessageNotFoundError as exc:
            raise APIError(404, "message_not_found", "Message not found in this room") from exc
        except ReadStateCapacityError as exc:
            raise APIError(507, "read_state_capacity_reached", "The room read-state capacity has been reached") from exc

    @router.delete("/rooms/{room_id}/read-state", status_code=204, tags=["messages"])
    async def clear_read_state(room_id: str, principal: PrincipalDependency) -> Response:
        _authorize(principal, "room:read", room_id)
        await _enforce_member_access(store, principal, room_id, write=False)
        subject = _stable_subject(principal)
        try:
            await store.clear_read_state(room_id, subject)
        except RoomNotFoundError as exc:
            raise APIError(404, "room_not_found", "Room not found") from exc
        return Response(status_code=204)

    @router.post("/rooms/{room_id}/messages", response_model=Message, tags=["messages"])
    async def create_message(
        room_id: str,
        payload: MessageCreate,
        response: Response,
        request: Request,
        principal: PrincipalDependency,
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key", min_length=1, max_length=128),
    ) -> Message:
        _authorize(principal, "room:write", room_id)
        await _enforce_member_access(store, principal, room_id, write=True)
        sender = payload.sender
        if principal.subject is not None:
            if sender is not None and sender != principal.subject:
                raise APIError(403, "identity_mismatch", "Message sender must match the authenticated subject")
            sender = principal.subject
        if sender is None:
            raise APIError(422, "sender_required", "sender is required when using operator or local access")
        if len(payload.content) > resolved.max_message_chars:
            raise APIError(
                413,
                "message_too_large",
                f"Message content exceeds the {resolved.max_message_chars}-character limit",
            )
        if idempotency_key and payload.client_message_id and idempotency_key != payload.client_message_id:
            raise APIError(
                400,
                "idempotency_conflict",
                "Idempotency-Key and client_message_id must match when both are supplied",
            )
        rate_subject = principal.subject or _client_key(request)
        if not await limiter.allow(f"http:{rate_subject}"):
            raise APIError(
                429,
                "rate_limit_exceeded",
                "Message rate limit exceeded",
                headers={"Retry-After": "60"},
            )
        try:
            message, created = await store.create_message(
                room_id=room_id,
                sender=sender,
                content=payload.content,
                client_message_id=idempotency_key or payload.client_message_id,
                allow_frozen=principal.is_admin,
                member_subject=None if principal.is_admin else principal.subject,
                author_subject=principal.subject,
            )
        except RoomNotFoundError as exc:
            raise APIError(404, "room_not_found", "Room not found") from exc
        except RoomArchivedError as exc:
            raise APIError(409, "room_archived", "Archived rooms are read-only") from exc
        except RoomFrozenError as exc:
            raise APIError(409, "room_frozen", "Only administrators may publish while the room is frozen") from exc
        except MemberBannedError as exc:
            raise APIError(403, "room_banned", "This account is banned from the room") from exc
        except MemberMutedError as exc:
            raise APIError(403, "room_muted", "This account is muted in the room") from exc
        except WebhookCapacityError as exc:
            raise APIError(507, "webhook_capacity_reached", "Webhook delivery capacity reached") from exc
        response.status_code = 201 if created else 200
        event = _event("message.created", message=message.model_dump(mode="json"))
        if created:
            if webhook_dispatcher is not None:
                webhook_dispatcher.wake()
            if postgres_runtime is None:
                await manager.broadcast(room_id, event)
        return message

    @router.patch("/rooms/{room_id}/messages/{message_id}", response_model=Message, tags=["messages"])
    async def update_message(
        room_id: str,
        message_id: str,
        payload: MessageUpdate,
        principal: PrincipalDependency,
    ) -> Message:
        _authorize(principal, "room:write", room_id)
        await _enforce_member_access(store, principal, room_id, write=True)
        if len(payload.content) > resolved.max_message_chars:
            raise APIError(
                413,
                "message_too_large",
                f"Message content exceeds the {resolved.max_message_chars}-character limit",
            )
        try:
            message = await store.update_message(
                room_id=room_id,
                message_id=message_id,
                actor=_audit_actor(principal),
                content=payload.content,
                is_admin=principal.is_admin,
                member_subject=None if principal.is_admin else principal.subject,
            )
        except RoomNotFoundError as exc:
            raise APIError(404, "room_not_found", "Room not found") from exc
        except MessageNotFoundError as exc:
            raise APIError(404, "message_not_found", "Message not found") from exc
        except MessageOwnershipError as exc:
            raise APIError(
                403,
                "message_not_owned",
                "Only the author or an administrator may edit this message",
            ) from exc
        except MessageDeletedError as exc:
            raise APIError(409, "message_deleted", "Deleted messages cannot be edited") from exc
        except RoomArchivedError as exc:
            raise APIError(409, "room_archived", "Archived rooms are read-only") from exc
        except RoomFrozenError as exc:
            raise APIError(409, "room_frozen", "Only administrators may edit while the room is frozen") from exc
        except MemberBannedError as exc:
            raise APIError(403, "room_banned", "This account is banned from the room") from exc
        except MemberMutedError as exc:
            raise APIError(403, "room_muted", "This account is muted in the room") from exc
        except WebhookCapacityError as exc:
            raise APIError(507, "webhook_capacity_reached", "Webhook delivery capacity reached") from exc
        if webhook_dispatcher is not None:
            webhook_dispatcher.wake()
        if postgres_runtime is None:
            await manager.broadcast(room_id, _event("message.updated", message=message.model_dump(mode="json")))
        return message

    @router.delete("/rooms/{room_id}/messages/{message_id}", status_code=204, tags=["messages"])
    async def delete_message(
        room_id: str,
        message_id: str,
        principal: PrincipalDependency,
    ) -> Response:
        _authorize(principal, "room:write", room_id)
        await _enforce_member_access(store, principal, room_id, write=True)
        try:
            message, deleted = await store.delete_message(
                room_id=room_id,
                message_id=message_id,
                actor=_audit_actor(principal),
                is_admin=principal.is_admin,
                member_subject=None if principal.is_admin else principal.subject,
            )
        except RoomNotFoundError as exc:
            raise APIError(404, "room_not_found", "Room not found") from exc
        except MessageNotFoundError as exc:
            raise APIError(404, "message_not_found", "Message not found") from exc
        except MessageOwnershipError as exc:
            raise APIError(
                403,
                "message_not_owned",
                "Only the author or an administrator may delete this message",
            ) from exc
        except RoomArchivedError as exc:
            raise APIError(409, "room_archived", "Archived rooms are read-only") from exc
        except RoomFrozenError as exc:
            raise APIError(409, "room_frozen", "Only administrators may delete while the room is frozen") from exc
        except MemberBannedError as exc:
            raise APIError(403, "room_banned", "This account is banned from the room") from exc
        except MemberMutedError as exc:
            raise APIError(403, "room_muted", "This account is muted in the room") from exc
        except WebhookCapacityError as exc:
            raise APIError(507, "webhook_capacity_reached", "Webhook delivery capacity reached") from exc
        if deleted:
            if webhook_dispatcher is not None:
                webhook_dispatcher.wake()
            if postgres_runtime is None:
                await manager.broadcast(room_id, _event("message.deleted", message=message.model_dump(mode="json")))
        return Response(status_code=204)

    @router.patch(
        "/rooms/{room_id}/members/{subject}/moderation",
        response_model=MemberModeration,
        tags=["moderation"],
    )
    async def update_member_moderation(
        room_id: str,
        subject: Annotated[str, Path(min_length=1, max_length=64)],
        payload: MemberModerationUpdate,
        principal: PrincipalDependency,
    ) -> MemberModeration:
        _authorize(principal, "admin")
        if subject != subject.strip():
            raise APIError(422, "invalid_request", "Moderation subject must not have surrounding whitespace")
        try:
            moderation = await store.set_member_moderation(
                room_id,
                subject,
                payload,
                actor=_audit_actor(principal),
            )
        except RoomNotFoundError as exc:
            raise APIError(404, "room_not_found", "Room not found") from exc
        except WebhookCapacityError as exc:
            raise APIError(507, "webhook_capacity_reached", "Webhook delivery capacity reached") from exc
        if webhook_dispatcher is not None:
            webhook_dispatcher.wake()
        if (
            postgres_runtime is None
            and moderation.banned_until is not None
            and moderation.banned_until > datetime.now(timezone.utc)
        ):
            closed = await manager.close_member(
                room_id,
                subject,
                _event("member.banned", subject=subject, banned_until=moderation.banned_until.isoformat()),
            )
            if closed:
                await manager.broadcast(
                    room_id,
                    _event(
                        "presence.left",
                        username=subject,
                        active_connections=manager.room_connections(room_id),
                    ),
                )
        return moderation

    @router.get("/stats", tags=["operations"])
    async def stats(principal: PrincipalDependency) -> dict[str, int]:
        _authorize(principal, "admin")
        active_connections = (
            await postgres_runtime.total_connection_count()
            if postgres_runtime is not None
            else manager.active_connections
        )
        return {"active_connections": active_connections}

    @router.get("/admin/audit-events", response_model=AuditEventPage, tags=["operations"])
    async def list_audit_events(
        principal: PrincipalDependency,
        limit: int = Query(default=50, ge=1, le=100),
        before: str | None = Query(default=None, min_length=1, max_length=128),
    ) -> AuditEventPage:
        _authorize(principal, "admin")
        try:
            items, next_before = await store.list_audit_events(limit=limit, before=before)
        except InvalidAuditCursorError as exc:
            raise APIError(400, "invalid_cursor", "The audit cursor is not valid") from exc
        return AuditEventPage(items=items, next_before=next_before)

    @router.get(
        "/admin/webhook-deliveries",
        response_model=WebhookDeliveryPage,
        tags=["operations"],
    )
    async def list_webhook_deliveries(
        principal: PrincipalDependency,
        limit: int = Query(default=50, ge=1, le=100),
        before: str | None = Query(default=None, min_length=1, max_length=128),
        status: Literal["pending", "delivered", "failed"] | None = Query(default=None),
    ) -> WebhookDeliveryPage:
        _authorize(principal, "admin")
        try:
            items, next_before = await store.list_webhook_deliveries(
                limit=limit,
                before=before,
                status=status,
            )
        except InvalidWebhookCursorError as exc:
            raise APIError(400, "invalid_cursor", "The webhook delivery cursor is not valid") from exc
        return WebhookDeliveryPage(items=items, next_before=next_before)

    @router.post(
        "/admin/webhook-deliveries/{delivery_id}/retry",
        response_model=WebhookDelivery,
        status_code=202,
        tags=["operations"],
    )
    async def retry_webhook_delivery(
        delivery_id: Annotated[str, Path(min_length=1, max_length=128)],
        principal: PrincipalDependency,
    ) -> WebhookDelivery:
        _authorize(principal, "admin")
        if webhook_dispatcher is None:
            raise APIError(409, "webhook_not_configured", "Configure a webhook destination before replaying delivery")
        try:
            delivery = await store.retry_webhook_delivery(delivery_id)
        except WebhookDeliveryNotFoundError as exc:
            raise APIError(404, "webhook_delivery_not_found", "Webhook delivery not found") from exc
        except WebhookPayloadUnavailableError as exc:
            raise APIError(409, "webhook_payload_unavailable", "Deletion removed this delivery's replay body") from exc
        webhook_dispatcher.wake()
        return delivery

    @router.post("/admin/retention/run", response_model=RetentionResult, tags=["operations"])
    async def run_retention(principal: PrincipalDependency) -> RetentionResult:
        _authorize(principal, "admin")
        try:
            deleted_messages, cutoff = await store.run_retention(actor=_audit_actor(principal))
        except RetentionNotConfiguredError as exc:
            raise APIError(409, "retention_not_configured", "Message retention is not configured") from exc
        return RetentionResult(deleted_messages=deleted_messages, cutoff=cutoff)

    application.include_router(router)

    @application.websocket("/v1/rooms/{room_id}/ws")
    async def room_websocket(
        websocket: WebSocket,
        room_id: str,
        username: str | None = Query(default=None, min_length=1, max_length=64),
    ) -> None:
        if not _websocket_origin_allowed(websocket, resolved):
            await websocket.close(code=4403, reason="Origin not allowed")
            return
        await websocket.accept()
        principal = await _authenticate_websocket(websocket, resolved, token_service)
        if principal is None:
            return

        if not principal.allows("room:read", room_id):
            await websocket.send_json(_event("error", code="authorization_denied", message="Room access denied"))
            await websocket.close(code=4403, reason="Room access denied")
            return
        if principal.subject is not None:
            if username is not None and username != principal.subject:
                await websocket.send_json(_event("error", code="identity_mismatch", message="Username mismatch"))
                await websocket.close(code=4403, reason="Username mismatch")
                return
            username = principal.subject
        if username is None:
            await websocket.send_json(_event("error", code="username_required", message="Username is required"))
            await websocket.close(code=1008, reason="Username required")
            return

        room = await store.get_room(room_id)
        if room is None:
            await websocket.send_json(_event("error", code="room_not_found", message="Room not found"))
            await websocket.close(code=4404, reason="Room not found")
            return
        moderation = await store.get_member_moderation(room_id, principal.subject) if principal.subject else None
        if (
            not principal.is_admin
            and moderation is not None
            and moderation.banned_until is not None
            and moderation.banned_until > datetime.now(timezone.utc)
        ):
            await websocket.send_json(
                _event("error", code="room_banned", message="This account is banned from the room")
            )
            await websocket.close(code=4403, reason="Room access revoked")
            return
        if room.archived_at is not None:
            await websocket.send_json(_event("error", code="room_archived", message="Archived rooms are read-only"))
            await websocket.close(code=4409, reason="Room archived")
            return
        connection_id = f"socket-{uuid.uuid4().hex}"
        if postgres_runtime is not None:
            lease = await postgres_runtime.acquire_connection(
                connection_id=connection_id,
                room_id=room_id,
                username=username,
                subject=principal.subject,
            )
            admitted = lease is not None
        else:
            admitted = True
        registered = admitted and await manager.register(
            websocket,
            room_id,
            username,
            principal.subject,
            connection_id=connection_id,
            broadcast_ready=False,
        )
        if not registered:
            if admitted and postgres_runtime is not None:
                await postgres_runtime.release_connection(connection_id)
            await websocket.send_json(
                _event("error", code="connection_capacity_reached", message="Connection capacity reached")
            )
            await websocket.close(code=1013, reason="Try again later")
            return

        room = await store.get_room(room_id)
        moderation = await store.get_member_moderation(room_id, principal.subject) if principal.subject else None
        now = datetime.now(timezone.utc)
        banned = (
            not principal.is_admin
            and moderation is not None
            and moderation.banned_until is not None
            and moderation.banned_until > now
        )
        if room is None or room.archived_at is not None or banned:
            code = "room_not_found" if room is None else "room_banned" if banned else "room_archived"
            status_message = (
                "Room not found"
                if room is None
                else "This account is banned from the room"
                if banned
                else "Archived rooms are read-only"
            )
            await manager.send(websocket, _event("error", code=code, message=status_message))
            await manager.close(
                websocket,
                code=4404 if room is None else 4403 if banned else 4409,
                reason=status_message,
            )
            if postgres_runtime is not None:
                await postgres_runtime.release_connection(connection_id)
            return

        history, next_before = await store.list_messages(room_id, limit=50)
        if postgres_runtime is not None:
            active_connections = (await postgres_runtime.connection_counts(room_id)).room
        else:
            active_connections = manager.room_connections(room_id)
        ready_sent = await manager.send(
            websocket,
            _event(
                "ready",
                room=room.model_dump(mode="json"),
                username=username,
                active_connections=active_connections,
                max_message_chars=resolved.max_message_chars,
            ),
        )
        if not ready_sent:
            if postgres_runtime is not None:
                await postgres_runtime.release_connection(connection_id)
            return
        history_sent = await manager.send(
            websocket,
            _event(
                "history",
                items=[message.model_dump(mode="json") for message in history],
                next_before=next_before,
            ),
        )
        if not history_sent or not await manager.activate(websocket):
            if postgres_runtime is not None:
                await postgres_runtime.release_connection(connection_id)
            return
        if postgres_runtime is None:
            await manager.broadcast(
                room_id,
                _event("presence.joined", username=username, active_connections=active_connections),
                exclude=websocket,
            )

        typing_active = False
        typing_deadline = 0.0
        typing_task: asyncio.Task[None] | None = None
        connection_heartbeat_task: asyncio.Task[None] | None = None

        async def maintain_connection_lease() -> None:
            if postgres_runtime is None:
                raise RuntimeError("PostgreSQL runtime is unavailable")
            try:
                while True:
                    await asyncio.sleep(resolved.postgres_lease_seconds / 3)
                    await postgres_runtime.renew_connection(connection_id)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Closing WebSocket after PostgreSQL lease loss: %s", type(exc).__name__)
                await manager.send(
                    websocket,
                    _event("error", code="storage_unavailable", message="Chat storage is temporarily unavailable"),
                )
                try:
                    await manager.close(websocket, code=1012, reason="Storage unavailable")
                except Exception as close_exc:
                    logger.debug("WebSocket was already closed after lease loss: %s", type(close_exc).__name__)

        if postgres_runtime is not None:
            connection_heartbeat_task = asyncio.create_task(
                maintain_connection_lease(),
                name=f"samsarix-connection-{connection_id}",
            )

        async def expire_typing() -> None:
            nonlocal typing_active, typing_task
            try:
                while typing_active:
                    remaining = typing_deadline - time.monotonic()
                    if remaining > 0:
                        await asyncio.sleep(remaining)
                        continue
                    typing_active = False
                    await manager.broadcast(
                        room_id,
                        _event("typing.stopped", username=username),
                        exclude=websocket,
                    )
            except asyncio.CancelledError:
                pass
            finally:
                typing_task = None

        async def set_typing(active: bool) -> None:
            nonlocal typing_active, typing_deadline, typing_task
            if postgres_runtime is not None:
                await postgres_runtime.set_typing(connection_id, active)
                typing_active = active
                return
            if active:
                typing_deadline = time.monotonic() + resolved.typing_timeout_seconds
                if not typing_active:
                    typing_active = True
                    await manager.broadcast(
                        room_id,
                        _event("typing.started", username=username, expires_in=resolved.typing_timeout_seconds),
                        exclude=websocket,
                    )
                if typing_task is None:
                    typing_task = asyncio.create_task(expire_typing())
            elif typing_active:
                typing_active = False
                await manager.broadcast(
                    room_id,
                    _event("typing.stopped", username=username),
                    exclude=websocket,
                )

        invalid_commands = 0
        try:
            while True:
                packet = await websocket.receive()
                if packet["type"] == "websocket.disconnect":
                    raise WebSocketDisconnect(packet.get("code", 1000))
                raw = packet.get("text")
                if raw is None:
                    invalid_commands += 1
                    await manager.send(
                        websocket,
                        _event("error", code="text_required", message="Only JSON text frames are supported"),
                    )
                    if invalid_commands >= 3:
                        await manager.close(websocket, code=1003, reason="Text frames required")
                        break
                    continue
                if len(raw.encode("utf-8")) > resolved.websocket_max_bytes:
                    await manager.send(
                        websocket,
                        _event("error", code="frame_too_large", message="WebSocket command is too large"),
                    )
                    await manager.close(websocket, code=1009, reason="Message too large")
                    break
                try:
                    command = _WS_COMMAND.validate_json(raw)
                except (ValidationError, json.JSONDecodeError):
                    invalid_commands += 1
                    await manager.send(
                        websocket,
                        _event("error", code="invalid_command", message="Expected a message, ping, or typing command"),
                    )
                    if invalid_commands >= 3:
                        await manager.close(websocket, code=1008, reason="Too many invalid commands")
                        break
                    continue

                invalid_commands = 0
                if isinstance(command, WebSocketPing):
                    await manager.send(websocket, _event("pong"))
                    continue
                if isinstance(command, WebSocketTyping):
                    if not principal.allows("room:write", room_id):
                        await manager.send(
                            websocket,
                            _event("error", code="authorization_denied", message="Typing signals are not allowed"),
                        )
                        continue
                    current_room = await store.get_room(room_id)
                    if current_room is None:
                        await manager.send(websocket, _event("error", code="room_not_found", message="Room not found"))
                        await manager.close(websocket, code=4404, reason="Room not found")
                        break
                    if current_room.archived_at is not None:
                        await manager.send(
                            websocket,
                            _event("error", code="room_archived", message="Archived rooms are read-only"),
                        )
                        await manager.close(websocket, code=4409, reason="Room archived")
                        break
                    if current_room.frozen_at is not None and not principal.is_admin:
                        await manager.send(
                            websocket,
                            _event(
                                "error",
                                code="room_frozen",
                                message="Only administrators may send typing signals while the room is frozen",
                            ),
                        )
                        continue
                    moderation = (
                        await store.get_member_moderation(room_id, principal.subject) if principal.subject else None
                    )
                    now = datetime.now(timezone.utc)
                    if not principal.is_admin and moderation is not None:
                        if moderation.banned_until is not None and moderation.banned_until > now:
                            await manager.send(
                                websocket,
                                _event("error", code="room_banned", message="This account is banned from the room"),
                            )
                            await manager.close(websocket, code=4403, reason="Room access revoked")
                            break
                        if moderation.muted_until is not None and moderation.muted_until > now:
                            await manager.send(
                                websocket,
                                _event("error", code="room_muted", message="This account is muted in the room"),
                            )
                            continue
                    rate_subject = principal.subject or (websocket.client.host if websocket.client else "unknown")
                    if not await typing_limiter.allow(f"typing:{rate_subject}"):
                        await manager.send(
                            websocket,
                            _event("error", code="typing_rate_limit_exceeded", message="Typing signal limit exceeded"),
                        )
                        continue
                    await set_typing(command.active)
                    continue
                if not principal.allows("room:write", room_id):
                    await manager.send(
                        websocket,
                        _event("error", code="authorization_denied", message="Message publishing is not allowed"),
                    )
                    continue
                if len(command.content) > resolved.max_message_chars:
                    await manager.send(
                        websocket,
                        _event(
                            "error",
                            code="message_too_large",
                            message=f"Message content exceeds the {resolved.max_message_chars}-character limit",
                        ),
                    )
                    continue
                rate_subject = principal.subject or (websocket.client.host if websocket.client else "unknown")
                if not await limiter.allow(f"ws:{rate_subject}"):
                    await manager.send(
                        websocket,
                        _event("error", code="rate_limit_exceeded", message="Message rate limit exceeded"),
                    )
                    continue
                try:
                    message, created = await store.create_message(
                        room_id=room_id,
                        sender=username,
                        content=command.content,
                        client_message_id=command.client_message_id,
                        allow_frozen=principal.is_admin,
                        member_subject=None if principal.is_admin else principal.subject,
                        author_subject=principal.subject,
                    )
                except (
                    MemberBannedError,
                    MemberMutedError,
                    RoomArchivedError,
                    RoomFrozenError,
                    RoomNotFoundError,
                    WebhookCapacityError,
                ) as exc:
                    code, message_text, close_code, close_reason = _message_write_error(exc)
                    await manager.send(
                        websocket,
                        _event("error", code=code, message=message_text),
                    )
                    if close_code is None:
                        continue
                    await manager.close(websocket, code=close_code, reason=close_reason or message_text)
                    break
                message_event = _event(
                    "message.created",
                    message=message.model_dump(mode="json"),
                    idempotent_replay=not created,
                )
                await set_typing(False)
                if created:
                    if webhook_dispatcher is not None:
                        webhook_dispatcher.wake()
                    if postgres_runtime is None:
                        await manager.broadcast(room_id, message_event)
                else:
                    await manager.send(websocket, message_event)
        except (WebSocketDisconnect, asyncio.CancelledError):
            pass
        finally:
            if connection_heartbeat_task is not None:
                connection_heartbeat_task.cancel()
                await asyncio.gather(connection_heartbeat_task, return_exceptions=True)
            if typing_task is not None:
                typing_task.cancel()
            if typing_active and postgres_runtime is None:
                await manager.broadcast(room_id, _event("typing.stopped", username=username), exclude=websocket)
            metadata = await manager.unregister(websocket)
            if postgres_runtime is not None:
                await postgres_runtime.release_connection(connection_id)
            elif metadata:
                await manager.broadcast(
                    room_id,
                    _event("presence.left", username=username, active_connections=manager.room_connections(room_id)),
                )

    return application
