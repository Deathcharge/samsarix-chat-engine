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
        if args.database is not None:
            settings = settings.with_database_path(args.database)
    except ConfigurationError as exc:
        parser.error(str(exc))

    if not 1 <= args.port <= 65_535:
        parser.error("--port must be between 1 and 65535")
    if not _is_loopback_host(args.host) and settings.api_key is None and not args.allow_insecure_public:
        parser.error(
            "refusing an unauthenticated public bind; set SAMSARIX_CHAT_API_KEY or explicitly pass "
            "--allow-insecure-public"
        )

    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    if not _is_loopback_host(args.host) and settings.api_key is None:
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
