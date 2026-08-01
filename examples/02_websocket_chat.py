"""Connect, authenticate if configured, send a message, and print events."""

from __future__ import annotations

import asyncio
import json
import os

from websockets.asyncio.client import connect

ROOM_ID = os.getenv("SAMSARIX_CHAT_ROOM", "general")
USERNAME = os.getenv("SAMSARIX_CHAT_USERNAME", "example")
API_KEY = os.getenv("SAMSARIX_CHAT_API_KEY")
ACCESS_TOKEN = os.getenv("SAMSARIX_CHAT_ACCESS_TOKEN")
WS_URL = os.getenv("SAMSARIX_CHAT_WS_URL", "ws://127.0.0.1:8000")


async def chat() -> None:
    username_query = "" if ACCESS_TOKEN else f"?username={USERNAME}"
    uri = f"{WS_URL}/v1/rooms/{ROOM_ID}/ws{username_query}"
    async with connect(uri, max_size=16_384) as websocket:
        event = json.loads(await websocket.recv())
        if event["type"] == "auth.required":
            if ACCESS_TOKEN:
                await websocket.send(json.dumps({"type": "auth", "token": ACCESS_TOKEN}))
            elif API_KEY:
                await websocket.send(json.dumps({"type": "auth", "api_key": API_KEY}))
            else:
                raise RuntimeError("The service requires an API key or access token")
            event = json.loads(await websocket.recv())
        print(event)  # ready
        print(json.loads(await websocket.recv()))  # history
        await websocket.send(json.dumps({"type": "message", "content": "Hello over WebSocket"}))
        print(json.loads(await websocket.recv()))  # message.created


asyncio.run(chat())
