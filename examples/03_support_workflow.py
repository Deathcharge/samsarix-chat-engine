"""Exercise a two-party support-room journey with signed unread state."""

from __future__ import annotations

import json
import os
from urllib.error import HTTPError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

BASE_URL = os.getenv("SAMSARIX_CHAT_URL", "http://127.0.0.1:8000")
API_KEY = os.getenv("SAMSARIX_CHAT_API_KEY")
CUSTOMER_TOKEN = os.getenv("SAMSARIX_CHAT_CUSTOMER_TOKEN")
AGENT_TOKEN = os.getenv("SAMSARIX_CHAT_AGENT_TOKEN")
ROOM_ID = os.getenv("SAMSARIX_CHAT_ROOM", "support-demo")

if urlparse(BASE_URL).scheme not in {"http", "https"}:
    raise ValueError("SAMSARIX_CHAT_URL must use http or https")
if not API_KEY or not CUSTOMER_TOKEN or not AGENT_TOKEN:
    raise RuntimeError(
        "SAMSARIX_CHAT_API_KEY, SAMSARIX_CHAT_CUSTOMER_TOKEN, and SAMSARIX_CHAT_AGENT_TOKEN are required"
    )


def request(method: str, path: str, *, credential: str, payload: object | None = None) -> tuple[int, object | None]:
    headers = {"Accept": "application/json"}
    if credential == API_KEY:
        headers["X-API-Key"] = credential
    else:
        headers["Authorization"] = f"Bearer {credential}"
    body = None
    if payload is not None:
        headers["Content-Type"] = "application/json"
        body = json.dumps(payload).encode()
    outbound = Request(f"{BASE_URL}{path}", data=body, headers=headers, method=method)  # noqa: S310
    try:
        with urlopen(outbound, timeout=5) as response:  # noqa: S310 - configured http(s) target
            return response.status, json.load(response) if response.status != 204 else None
    except HTTPError as exc:
        return exc.code, json.load(exc)


status, result = request(
    "POST",
    "/v1/rooms",
    credential=API_KEY,
    payload={"id": ROOM_ID, "name": "Support demo"},
)
if status not in {201, 409}:
    raise SystemExit(f"Could not create support room: {status} {result}")

status, customer_message = request(
    "POST",
    f"/v1/rooms/{ROOM_ID}/messages",
    credential=CUSTOMER_TOKEN,
    payload={"content": "My deployment cannot reconnect."},
)
if status != 201 or not isinstance(customer_message, dict):
    raise SystemExit(f"Could not create customer message: {status} {customer_message}")

_, agent_unread = request("GET", f"/v1/rooms/{ROOM_ID}/read-state", credential=AGENT_TOKEN)
_, agent_read = request(
    "PUT",
    f"/v1/rooms/{ROOM_ID}/read-state",
    credential=AGENT_TOKEN,
    payload={"message_id": customer_message["id"]},
)
status, agent_message = request(
    "POST",
    f"/v1/rooms/{ROOM_ID}/messages",
    credential=AGENT_TOKEN,
    payload={"content": "I found the reconnect configuration issue."},
)
if status != 201 or not isinstance(agent_message, dict):
    raise SystemExit(f"Could not create agent reply: {status} {agent_message}")

_, customer_unread = request("GET", f"/v1/rooms/{ROOM_ID}/read-state", credential=CUSTOMER_TOKEN)
_, customer_read = request(
    "PUT",
    f"/v1/rooms/{ROOM_ID}/read-state",
    credential=CUSTOMER_TOKEN,
    payload={"message_id": agent_message["id"]},
)
print(
    json.dumps(
        {
            "agent_before": agent_unread,
            "agent_after": agent_read,
            "customer_before": customer_unread,
            "customer_after": customer_read,
        },
        indent=2,
    )
)
