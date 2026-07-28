"""Embed an isolated chat engine in a larger FastAPI deployment."""

from pathlib import Path

from samsarix_chat_engine import Settings, create_app

settings = Settings(
    database_path=Path("data/embedded-chat.db"),
    allowed_origins=("http://localhost:3000",),
)
app = create_app(settings)

# Run from the repository root:
# uvicorn examples.embed_app:app --host 127.0.0.1 --port 8000
