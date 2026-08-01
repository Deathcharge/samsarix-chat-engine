# Copyright (c) 2026 Samsarix LLC
# SPDX-License-Identifier: MPL-2.0
"""Dependency-light command-line entry point."""

from __future__ import annotations

import argparse
import ipaddress
import logging
import sys
import warnings
from collections.abc import Sequence
from pathlib import Path

from . import __version__
from .app import create_app
from .auth import AccessTokenService, Permission
from .config import ConfigurationError, Settings


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="samsarix-chat",
        description="Run the local-first Samsarix room chat service.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    subparsers = parser.add_subparsers(dest="command", required=True)
    serve = subparsers.add_parser("serve", help="start the HTTP and WebSocket service")
    serve.add_argument("--host", default="127.0.0.1", help="bind address (default: 127.0.0.1)")
    serve.add_argument("--port", type=int, default=8000, help="bind port (default: 8000)")
    serve.add_argument("--database", type=Path, help="SQLite path (overrides SAMSARIX_CHAT_DATABASE)")
    serve.add_argument(
        "--allow-insecure-public",
        action="store_true",
        help="allow a non-loopback bind without SAMSARIX_CHAT_API_KEY (unsafe)",
    )
    serve.add_argument(
        "--log-level",
        choices=("critical", "error", "warning", "info", "debug"),
        default="info",
    )
    token = subparsers.add_parser("token", help="manage short-lived application access tokens")
    token_commands = token.add_subparsers(dest="token_command", required=True)
    issue = token_commands.add_parser("issue", help="issue a signed access token")
    issue.add_argument("--subject", required=True, help="authenticated application user ID")
    issue.add_argument("--room", action="append", default=[], help="allowed room ID (repeatable)")
    issue.add_argument(
        "--permission",
        action="append",
        choices=("room:read", "room:write", "admin"),
        help="granted permission (repeatable; defaults to room read and write)",
    )
    issue.add_argument(
        "--expires-in",
        type=int,
        default=3_600,
        metavar="SECONDS",
        help="token lifetime in seconds (default: 3600)",
    )
    return parser


def _is_loopback_host(host: str) -> bool:
    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def main(argv: Sequence[str] | None = None) -> int:
    if argv is None and Path(sys.argv[0]).stem.casefold() == "helix-chat":
        warnings.warn(
            "helix-chat is deprecated; use samsarix-chat instead",
            FutureWarning,
            stacklevel=2,
        )
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        settings = Settings.from_env()
        if args.command == "serve" and args.database is not None:
            settings = settings.with_database_path(args.database)
    except ConfigurationError as exc:
        parser.error(str(exc))

    if args.command == "token":
        if settings.token_signing_secret is None:
            parser.error("SAMSARIX_CHAT_TOKEN_SIGNING_SECRET is required to issue tokens")
        permissions: list[Permission] = args.permission or ["room:read", "room:write"]
        service = AccessTokenService(
            settings.token_signing_secret,
            issuer=settings.token_issuer,
            audience=settings.token_audience,
            max_lifetime_seconds=settings.token_max_lifetime_seconds,
            clock_skew_seconds=settings.token_clock_skew_seconds,
        )
        try:
            issued = service.issue(
                args.subject,
                rooms=args.room,
                permissions=permissions,
                expires_in_seconds=args.expires_in,
            )
        except ValueError as exc:
            parser.error(str(exc))
        print(issued)
        return 0

    if not 1 <= args.port <= 65_535:
        parser.error("--port must be between 1 and 65535")
    authentication_configured = settings.api_key is not None or settings.token_signing_secret is not None
    if not _is_loopback_host(args.host) and not authentication_configured and not args.allow_insecure_public:
        parser.error(
            "refusing an unauthenticated public bind; configure an API key or token signing secret, "
            "or explicitly pass --allow-insecure-public"
        )

    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    if not _is_loopback_host(args.host) and not authentication_configured:
        logging.warning("Starting an unauthenticated service on a non-loopback interface")
    import uvicorn

    uvicorn.run(
        create_app(settings),
        host=args.host,
        port=args.port,
        log_level=args.log_level,
        ws_max_size=settings.websocket_max_bytes,
        timeout_graceful_shutdown=10,
    )
    return 0
