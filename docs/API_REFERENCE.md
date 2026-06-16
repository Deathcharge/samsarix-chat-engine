# Helix Chat Engine API Reference

## ChatServer

Main class for managing chat functionality.

### Methods

#### create_room(name, description="", max_users=100, is_private=False)
Create a new chat room.

**Parameters:**
- `name` (str): Room name
- `description` (str): Room description
- `max_users` (int): Maximum users allowed
- `is_private` (bool): Whether room is private

**Returns:**
- `room` (Room): Created room object

#### send_message(room_id, user_id, content)
Send a message to a room.

**Parameters:**
- `room_id` (str): ID of room
- `user_id` (str): ID of sender
- `content` (str): Message content

**Returns:**
- `message` (Message): Created message object

#### join_room(room_id, user_id)
Join a chat room.

**Parameters:**
- `room_id` (str): ID of room
- `user_id` (str): ID of user

**Returns:**
- `success` (bool): Whether join succeeded

#### leave_room(room_id, user_id)
Leave a chat room.

**Parameters:**
- `room_id` (str): ID of room
- `user_id` (str): ID of user

**Returns:**
- `success` (bool): Whether leave succeeded

#### get_room_messages(room_id, limit=50, offset=0)
Get messages from a room.

**Parameters:**
- `room_id` (str): ID of room
- `limit` (int): Maximum messages to return
- `offset` (int): Offset for pagination

**Returns:**
- `messages` (list): List of messages

#### get_room_users(room_id)
Get users in a room.

**Parameters:**
- `room_id` (str): ID of room

**Returns:**
- `users` (list): List of user IDs

#### set_user_status(user_id, status)
Set user status.

**Parameters:**
- `user_id` (str): ID of user
- `status` (str): Status ("online", "away", "offline")

**Returns:**
- `success` (bool): Whether status was set

## WebSocketManager

Class for managing WebSocket connections.

### Methods

#### connect(websocket, room_id, user_id)
Connect a WebSocket.

**Parameters:**
- `websocket` (WebSocket): WebSocket connection
- `room_id` (str): Room ID
- `user_id` (str): User ID

#### disconnect(websocket, room_id, user_id)
Disconnect a WebSocket.

**Parameters:**
- `websocket` (WebSocket): WebSocket connection
- `room_id` (str): Room ID
- `user_id` (str): User ID

#### broadcast(room_id, message)
Broadcast message to room.

**Parameters:**
- `room_id` (str): Room ID
- `message` (str): Message to broadcast

#### send_personal(websocket, message)
Send message to specific connection.

**Parameters:**
- `websocket` (WebSocket): WebSocket connection
- `message` (str): Message to send

## Exceptions

- `RoomNotFoundError`: Room not found
- `UserNotFoundError`: User not found
- `UserNotInRoomError`: User not in room
- `RoomFullError`: Room is full
- `WebSocketError`: WebSocket error
