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
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from typing import Annotated, Any
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, FastAPI, Header, Query, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader, HTTPAuthorizationCredentials, HTTPBearer
from pydantic import TypeAdapter, ValidationError
from starlette.types import ASGIApp, Receive, Scope, Send
from starlette.types import Message as ASGIMessage

from .auth import AccessTokenService, AuthenticationError, Permission, Principal, credentials_match
from .config import Settings
from .models import (
    Message,
    MessageCreate,
    MessagePage,
    Room,
    RoomCreate,
    WebSocketAuth,
    WebSocketMessage,
    WebSocketPing,
)
from .store import (
    ChatStore,
    InvalidCursorError,
    RoomAlreadyExistsError,
    RoomCapacityError,
    RoomNotFoundError,
)
from .websocket_manager import ConnectionManager

logger = logging.getLogger(__name__)
_WS_COMMAND: TypeAdapter[WebSocketMessage | WebSocketPing] = TypeAdapter(WebSocketMessage | WebSocketPing)
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
    token_service: AccessTokenService | None,
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


def _error_payload(code: str, message: str, **details: Any) -> dict[str, Any]:
    error: dict[str, Any] = {"code": code, "message": message}
    if details:
        error["details"] = details
    return {"error": error}


def _event(event_type: str, **payload: Any) -> dict[str, Any]:
    return {"type": event_type, **payload}


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
    token_service: AccessTokenService | None,
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
    except (TimeoutError, ValidationError, ValueError, WebSocketDisconnect):
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
    store = ChatStore(
        resolved.database_path,
        max_rooms=resolved.max_rooms,
        max_stored_messages=resolved.max_stored_messages,
        max_stored_messages_per_room=resolved.max_stored_messages_per_room,
    )
    manager = ConnectionManager(
        max_connections=resolved.max_connections,
        max_per_room=resolved.max_connections_per_room,
        send_timeout=resolved.websocket_send_timeout_seconds,
    )
    limiter = MessageRateLimiter(resolved.messages_per_minute)
    token_service = (
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

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        await store.initialize()
        application.state.settings = resolved
        application.state.store = store
        application.state.connections = manager
        application.state.message_limiter = limiter
        application.state.token_service = token_service
        yield
        await manager.close_all()

    application = FastAPI(
        title="Samsarix Chat Engine",
        version="0.4.0",
        summary="A small persisted room-chat service with WebSocket delivery",
        lifespan=lifespan,
    )
    application.state.settings = resolved
    application.state.store = store
    application.state.connections = manager
    application.state.message_limiter = limiter
    application.state.token_service = token_service

    application.add_middleware(
        RequestBodyLimitMiddleware,
        max_body_bytes=max(16_384, resolved.max_message_chars * 12 + 8_192),
    )

    if resolved.allowed_origins:
        application.add_middleware(
            CORSMiddleware,
            allow_origins=list(resolved.allowed_origins),
            allow_credentials=False,
            allow_methods=["GET", "POST"],
            allow_headers=["Authorization", "Content-Type", "Idempotency-Key", "X-API-Key"],
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

    @application.get("/", include_in_schema=False)
    async def index() -> dict[str, Any]:
        return {
            "name": "Samsarix Chat Engine",
            "version": "0.4.0",
            "status": "ok",
            "docs": "/docs",
            "health": "/healthz",
        }

    @application.get("/healthz", tags=["operations"])
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @application.get("/readyz", tags=["operations"])
    async def readiness() -> JSONResponse:
        ready = await store.check_ready()
        return JSONResponse(status_code=200 if ready else 503, content={"status": "ready" if ready else "not_ready"})

    router = APIRouter(prefix="/v1")

    @router.post("/rooms", response_model=Room, status_code=201, tags=["rooms"])
    async def create_room(payload: RoomCreate, response: Response, principal: PrincipalDependency) -> Room:
        _authorize(principal, "admin")
        try:
            room = await store.create_room(payload)
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
        return room

    @router.get("/rooms/{room_id}/messages", response_model=MessagePage, tags=["messages"])
    async def list_messages(
        room_id: str,
        principal: PrincipalDependency,
        limit: int = Query(default=50, ge=1, le=100),
        before: str | None = Query(default=None, min_length=1, max_length=128),
    ) -> MessagePage:
        _authorize(principal, "room:read", room_id)
        try:
            items, next_before = await store.list_messages(room_id, limit=limit, before=before)
        except RoomNotFoundError as exc:
            raise APIError(404, "room_not_found", "Room not found") from exc
        except InvalidCursorError as exc:
            raise APIError(400, "invalid_cursor", "The message cursor is not valid for this room") from exc
        return MessagePage(items=items, next_before=next_before)

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
            )
        except RoomNotFoundError as exc:
            raise APIError(404, "room_not_found", "Room not found") from exc
        response.status_code = 201 if created else 200
        event = _event("message.created", message=message.model_dump(mode="json"))
        if created:
            await manager.broadcast(room_id, event)
        return message

    @router.get("/stats", tags=["operations"])
    async def stats(principal: PrincipalDependency) -> dict[str, int]:
        _authorize(principal, "admin")
        return {"active_connections": manager.active_connections}

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
        if not await manager.register(websocket, room_id, username):
            await websocket.send_json(
                _event("error", code="connection_capacity_reached", message="Connection capacity reached")
            )
            await websocket.close(code=1013, reason="Try again later")
            return

        history, next_before = await store.list_messages(room_id, limit=50)
        await manager.send(
            websocket,
            _event(
                "ready",
                room=room.model_dump(mode="json"),
                username=username,
                active_connections=manager.room_connections(room_id),
                max_message_chars=resolved.max_message_chars,
            ),
        )
        await manager.send(
            websocket,
            _event(
                "history",
                items=[message.model_dump(mode="json") for message in history],
                next_before=next_before,
            ),
        )
        await manager.broadcast(
            room_id,
            _event("presence.joined", username=username, active_connections=manager.room_connections(room_id)),
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
                        await websocket.close(code=1003, reason="Text frames required")
                        break
                    continue
                if len(raw.encode("utf-8")) > resolved.websocket_max_bytes:
                    await manager.send(
                        websocket,
                        _event("error", code="frame_too_large", message="WebSocket command is too large"),
                    )
                    await websocket.close(code=1009, reason="Message too large")
                    break
                try:
                    command = _WS_COMMAND.validate_json(raw)
                except (ValidationError, json.JSONDecodeError):
                    invalid_commands += 1
                    await manager.send(
                        websocket,
                        _event("error", code="invalid_command", message="Expected a message or ping command"),
                    )
                    if invalid_commands >= 3:
                        await websocket.close(code=1008, reason="Too many invalid commands")
                        break
                    continue

                invalid_commands = 0
                if isinstance(command, WebSocketPing):
                    await manager.send(websocket, _event("pong"))
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
                if not await limiter.allow(f"ws:{id(websocket)}"):
                    await manager.send(
                        websocket,
                        _event("error", code="rate_limit_exceeded", message="Message rate limit exceeded"),
                    )
                    continue
                message, created = await store.create_message(
                    room_id=room_id,
                    sender=username,
                    content=command.content,
                    client_message_id=command.client_message_id,
                )
                message_event = _event(
                    "message.created",
                    message=message.model_dump(mode="json"),
                    idempotent_replay=not created,
                )
                if created:
                    await manager.broadcast(room_id, message_event)
                else:
                    await manager.send(websocket, message_event)
        except (WebSocketDisconnect, asyncio.CancelledError):
            pass
        finally:
            metadata = await manager.unregister(websocket)
            if metadata:
                await manager.broadcast(
                    room_id,
                    _event("presence.left", username=username, active_connections=manager.room_connections(room_id)),
                )

    return application
