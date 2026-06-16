"""Multi-room chat example."""

from helix_chat_engine import ChatServer

# Create server
server = ChatServer()

# Create multiple rooms
rooms = []
for i in range(3):
    room = server.create_room(
        name=f"Room{i}",
        description=f"Chat room {i}"
    )
    rooms.append(room)

print(f"Created {len(rooms)} rooms")

# Send messages to each room
for room in rooms:
    message = server.send_message(
        room_id=room.id,
        user_id="user-1",
        content=f"Welcome to {room.name}"
    )
    print(f"Message sent to {room.name}")
