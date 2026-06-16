# Getting Started with Helix Chat Engine

## Installation

```bash
pip install helix-chat-engine
```

## Quick Start

```python
from helix_chat_engine import ChatServer, WebSocketManager

# Create server
server = ChatServer(host="localhost", port=8000)

# Create WebSocket manager
ws_manager = WebSocketManager()

# Start server
server.start()
```

## Basic Usage

### Create a Chat Room

```python
room = server.create_room(
    name="General",
    description="General discussion",
    max_users=100
)
```

### Send a Message

```python
message = server.send_message(
    room_id=room.id,
    user_id="user-1",
    content="Hello everyone!"
)
```

### Join a Room

```python
server.join_room(
    room_id=room.id,
    user_id="user-1"
)
```

## WebSocket Integration

```python
from fastapi import FastAPI, WebSocket

app = FastAPI()
manager = WebSocketManager()

@app.websocket("/ws/{room_id}/{user_id}")
async def websocket_endpoint(websocket: WebSocket, room_id: str, user_id: str):
    await manager.connect(websocket, room_id, user_id)
    try:
        while True:
            data = await websocket.receive_text()
            await manager.broadcast(room_id, data)
    finally:
        manager.disconnect(websocket, room_id, user_id)
```

## Common Patterns

### 1. Private Rooms

```python
room = server.create_room(
    name="Private",
    is_private=True,
    allowed_users=["user-1", "user-2"]
)
```

### 2. Message History

```python
history = server.get_room_messages(
    room_id=room.id,
    limit=50,
    offset=0
)
```

### 3. User Status

```python
server.set_user_status(
    user_id="user-1",
    status="online"  # or "away", "offline"
)
```

## Error Handling

```python
try:
    message = server.send_message(
        room_id=room.id,
        user_id="user-1",
        content="Hello"
    )
except RoomNotFoundError:
    print("Room not found")
except UserNotInRoomError:
    print("User not in room")
```

## Next Steps

- Read the [API Reference](API_REFERENCE.md)
- Check out [examples](../examples/)
- Review [architecture](ARCHITECTURE.md)
