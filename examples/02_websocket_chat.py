"""WebSocket chat example."""

from fastapi import FastAPI, WebSocket
from helix_chat_engine import WebSocketManager

app = FastAPI()
manager = WebSocketManager()

@app.websocket("/ws/{room_id}/{user_id}")
async def websocket_endpoint(websocket: WebSocket, room_id: str, user_id: str):
    """WebSocket endpoint for chat."""
    await manager.connect(websocket, room_id, user_id)
    try:
        while True:
            data = await websocket.receive_text()
            await manager.broadcast(room_id, data)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        manager.disconnect(websocket, room_id, user_id)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
