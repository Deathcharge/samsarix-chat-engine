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

parsed_url = urlparse(BASE_URL)
if parsed_url.scheme not in {"http", "https"} or parsed_url.hostname is None:
    raise ValueError("SAMSARIX_CHAT_URL must use http or https")
if parsed_url.scheme == "http" and parsed_url.hostname not in {"localhost", "127.0.0.1", "::1"}:
    raise ValueError("SAMSARIX_CHAT_URL must use https for non-loopback targets")
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


def require_state(status: int, payload: object | None, *, action: str) -> dict[str, object]:
    if status != 200 or not isinstance(payload, dict):
        raise SystemExit(f"Could not {action}: {status} {payload}")
    return payload


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
    payload={
        "content": "My deployment cannot reconnect.",
        "metadata": {"ticket.id": "SUP-DEMO-1", "ticket.channel": "in_product"},
        # The host owns assignment and notification preferences; Samsarix
        # records only the assigned agent's stable subject for webhook routing.
        "mentioned_subjects": ["agent-7"],
        # The host application has already authenticated, scanned, stored, and
        # authorized this object. Samsarix stores only its opaque descriptor.
        "attachments": [
            {
                "id": "support-upload-SUP-DEMO-1-trace",
                "name": "reconnect-trace.txt",
                "media_type": "text/plain",
                "size_bytes": 1842,
            }
        ],
    },
)
if status != 201 or not isinstance(customer_message, dict):
    raise SystemExit(f"Could not create customer message: {status} {customer_message}")

status, result = request("GET", f"/v1/rooms/{ROOM_ID}/read-state", credential=AGENT_TOKEN)
agent_unread = require_state(status, result, action="read agent state")
status, result = request(
    "PUT",
    f"/v1/rooms/{ROOM_ID}/read-state",
    credential=AGENT_TOKEN,
    payload={"message_id": customer_message["id"]},
)
agent_read = require_state(status, result, action="mark agent state read")
status, agent_message = request(
    "POST",
    f"/v1/rooms/{ROOM_ID}/messages",
    credential=AGENT_TOKEN,
    payload={
        "content": "I found the reconnect configuration issue.",
        "metadata": {"ticket.id": "SUP-DEMO-1", "action": "provide_resolution"},
    },
)
if status != 201 or not isinstance(agent_message, dict):
    raise SystemExit(f"Could not create agent reply: {status} {agent_message}")

status, result = request("GET", f"/v1/rooms/{ROOM_ID}/read-state", credential=CUSTOMER_TOKEN)
customer_unread = require_state(status, result, action="read customer state")
status, result = request(
    "PUT",
    f"/v1/rooms/{ROOM_ID}/read-state",
    credential=CUSTOMER_TOKEN,
    payload={"message_id": agent_message["id"]},
)
customer_read = require_state(status, result, action="mark customer state read")
print(
    json.dumps(
        {
            "agent_before": agent_unread,
            "agent_after": agent_read,
            "customer_before": customer_unread,
            "customer_after": customer_read,
            "customer_attachment_ids": [attachment["id"] for attachment in customer_message.get("attachments", [])],
            "customer_mentions": customer_message.get("mentioned_subjects", []),
        },
        indent=2,
    )
)
