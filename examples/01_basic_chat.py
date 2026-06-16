"""Basic chat example."""

from helix_chat_engine import ChatServer

# Create server
server = ChatServer(host="localhost", port=8000)

# Create room
room = server.create_room(
    name="General",
    description="General discussion"
)

print(f"Created room: {room.id}")

# Send message
message = server.send_message(
    room_id=room.id,
    user_id="user-1",
    content="Hello everyone!"
)

print(f"Sent message: {message.id}")

# Get messages
messages = server.get_room_messages(room.id)
print(f"Messages: {len(messages)}")
