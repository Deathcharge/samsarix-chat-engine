"""Create a room, post a message, and read history through the HTTP API."""

from __future__ import annotations

import json
import os
from urllib.error import HTTPError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

BASE_URL = os.getenv("SAMSARIX_CHAT_URL", "http://127.0.0.1:8000")
API_KEY = os.getenv("SAMSARIX_CHAT_API_KEY")

if urlparse(BASE_URL).scheme not in {"http", "https"}:
    raise ValueError("SAMSARIX_CHAT_URL must use http or https")


def request(method: str, path: str, payload: dict[str, str] | None = None) -> tuple[int, object]:
    headers = {"Content-Type": "application/json"}
    if API_KEY:
        headers["X-API-Key"] = API_KEY
    body = json.dumps(payload).encode() if payload is not None else None
    outbound = Request(  # noqa: S310 -- scheme is restricted above
        f"{BASE_URL}{path}", data=body, headers=headers, method=method
    )
    try:
        with urlopen(outbound, timeout=5) as response:  # noqa: S310 -- scheme is restricted above
            return response.status, json.load(response)
    except HTTPError as exc:
        return exc.code, json.load(exc)


status, room = request("POST", "/v1/rooms", {"id": "general", "name": "General"})
if status not in {201, 409}:
    raise SystemExit(f"Could not create room: {status} {room}")

status, message = request(
    "POST",
    "/v1/rooms/general/messages",
    {"sender": "example", "content": "Hello from the REST example"},
)
if status != 201:
    raise SystemExit(f"Could not send message: {status} {message}")

_, history = request("GET", "/v1/rooms/general/messages")
print(json.dumps(history, indent=2))
