# Helix Chat Engine

Helix Chat Engine is a small, local-first room chat service for developers who need persisted messages and live WebSocket delivery without adopting a full collaboration platform. It runs as a standalone FastAPI service or as an embeddable ASGI application, stores data in SQLite, and has no dependency on `helix-unified`, Redis, an LLM provider, or any private Helix package.

Version 0.2.0 is an alpha release candidate. The core single-instance journey is implemented and tested; public distribution is blocked on owner review of the repository's existing license text.

## What works

- Create and inspect rooms over HTTP.
- Post validated messages over HTTP or WebSocket.
- Persist room history in SQLite and recover it after reconnect or restart.
- Broadcast messages and lightweight join/leave presence within one process.
- Retry message submission safely with `Idempotency-Key` or `client_message_id`.
- Protect all chat data with an optional shared API key.
- Bound message size, send rate, connections, room count, and retained history.
- Check liveness at `/healthz`, storage readiness at `/readyz`, and OpenAPI docs at `/docs`.

It deliberately does not provide user accounts, per-room authorization, attachments, moderation, end-to-end encryption, federation, multi-instance fan-out, or AI agents.

## Quick start

Prerequisites: Python 3.10 or newer and Git.

```bash
git clone https://github.com/Deathcharge/helix-chat-engine.git
cd helix-chat-engine
python -m venv .venv
```

Activate the environment with `.venv\Scripts\Activate.ps1` on PowerShell or `source .venv/bin/activate` on POSIX, then:

```bash
python -m pip install --upgrade pip setuptools wheel
python -m pip install .
helix-chat serve
```

The service binds to `127.0.0.1:8000` and creates `data/helix-chat.db`. In another terminal, create a room and send a message:

```bash
curl -X POST http://127.0.0.1:8000/v1/rooms \
  -H "Content-Type: application/json" \
  -d '{"id":"general","name":"General"}'

curl -X POST http://127.0.0.1:8000/v1/rooms/general/messages \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: getting-started-1" \
  -d '{"sender":"Andrew","content":"Hello, room"}'

curl http://127.0.0.1:8000/v1/rooms/general/messages
```

For a complete WebSocket client, install the test extra and run the examples after creating `general`:

```bash
python -m pip install ".[test]"
python examples/01_rest_chat.py
python examples/02_websocket_chat.py
```

See [Getting started](docs/GETTING_STARTED.md) for authentication and browser examples.

## WebSocket protocol

Connect to:

```text
ws://127.0.0.1:8000/v1/rooms/{room_id}/ws?username={display_name}
```

The server sends `ready` and `history`, then accepts these JSON commands:

```json
{"type":"message","content":"Hello","client_message_id":"browser-42"}
```

```json
{"type":"ping"}
```

Clients receive `message.created`, `presence.joined`, `presence.left`, `pong`, and structured `error` events. When `HELIX_CHAT_API_KEY` is set, non-browser clients can send `X-API-Key` or `Authorization: Bearer ...` during the handshake; browser clients first receive `auth.required` and reply with `{"type":"auth","api_key":"..."}`. API keys are never accepted in query strings.

The exact HTTP and event contracts are in [API reference](docs/API_REFERENCE.md).

## Configuration

All settings are optional for loopback development. Copy [.env.example](.env.example) as a reference; the service reads process environment variables directly and does not automatically load `.env` files.

| Variable | Default | Purpose |
| --- | ---: | --- |
| `HELIX_CHAT_DATABASE` | `data/helix-chat.db` | SQLite database path |
| `HELIX_CHAT_API_KEY` | unset | Shared secret protecting all `/v1` data; minimum 16 characters |
| `HELIX_CHAT_ALLOWED_ORIGINS` | unset | Comma-separated exact browser origins for CORS/WebSockets |
| `HELIX_CHAT_MAX_MESSAGE_CHARS` | `4000` | Per-message character limit |
| `HELIX_CHAT_MESSAGES_PER_MINUTE` | `60` | Per-client HTTP and per-connection WebSocket message rate |
| `HELIX_CHAT_MAX_CONNECTIONS` | `200` | Process-wide WebSocket cap |
| `HELIX_CHAT_MAX_CONNECTIONS_PER_ROOM` | `100` | Per-room WebSocket cap |
| `HELIX_CHAT_MAX_ROOMS` | `1000` | Persisted room cap |
| `HELIX_CHAT_MAX_STORED_MESSAGES` | `100000` | Global retained-message cap |
| `HELIX_CHAT_MAX_STORED_MESSAGES_PER_ROOM` | `10000` | Per-room retained-message cap |
| `HELIX_CHAT_WS_AUTH_TIMEOUT` | `5` | Browser authentication deadline in seconds |
| `HELIX_CHAT_WS_SEND_TIMEOUT` | `2` | Slow-client send timeout in seconds |
| `HELIX_CHAT_WS_MAX_BYTES` | `16384` | WebSocket command frame cap used by the CLI server |

The CLI refuses `--host 0.0.0.0` or another non-loopback bind unless `HELIX_CHAT_API_KEY` is configured. `--allow-insecure-public` is an explicit development escape hatch, not a production recommendation.

## Embed it

```python
from pathlib import Path

from helix_chat_engine import Settings, create_app

app = create_app(Settings(database_path=Path("data/chat.db")))
```

`create_app()` returns a complete FastAPI application. Mounting it under another application is supported, but the parent deployment owns proxy limits, TLS, process topology, and graceful-shutdown behavior.

## Architecture and guarantees

```text
HTTP / WebSocket clients
          |
       FastAPI  -- validation, shared-key auth, error contracts, rate limits
          |
    ConnectionManager -- bounded, in-process room broadcast
          |
       ChatStore -- SQLite transactions, idempotency, bounded retention
```

- SQLite writes are serialized within one process and use `BEGIN IMMEDIATE`; foreign keys, WAL mode, and a five-second busy timeout are enabled.
- A message is persisted before `message.created` is broadcast. An HTTP success therefore means the local database committed it.
- WebSocket delivery and presence events are best-effort/at-most-once. Reconnecting clients recover the last 50 messages and can page older history over HTTP.
- Running multiple worker processes is not supported: each process would have an independent connection registry and rate limiter. Use one process or add a real broker in a future release.
- Retention is count-based. Old messages are deleted after successful inserts once configured caps are exceeded.

## Security, privacy, and operating cost

The default loopback bind avoids accidental network exposure. If the shared API key is enabled, it protects all chat data but does not distinguish users or rooms; put the service behind an identity-aware gateway when different users need different permissions. Configure TLS at a reverse proxy for any network deployment. Browser origins are exact-match when configured, and unauthenticated local mode rejects non-loopback browser origins.

Messages and display names are stored as plaintext in the configured SQLite file. The engine does not collect telemetry, call external APIs, or log message bodies or API keys. Backups, filesystem permissions, retention policy, user consent, and deletion workflows are deployment-owner responsibilities. Default operation has no metered API cost; its operating costs are compute, disk, backup, and network transfer only.

## Development and release checks

```bash
python -m pip install --upgrade pip setuptools wheel
python -m pip install -e ".[dev]"
python -m ruff check .
python -m ruff format --check .
python -m mypy helix_chat_engine
python -m pip_audit
python -m pytest --cov=helix_chat_engine --cov-report=term-missing
python -m build
python -m twine check dist/*
```

CI runs the tests on CPython 3.10–3.14 on Linux and CPython 3.12 on Windows. See [Contributing](CONTRIBUTING.md) and the living [productization record](docs/PRODUCTIZATION.md).

## Limitations and project status

This is a coherent single-instance MVP, not a hosted chat platform. The highest-value future work is per-user/per-room authorization, deletion/export administration, a multi-instance broker adapter, and load/soak testing. Those are intentionally not presented as current capabilities.

## License

The existing [LICENSE](LICENSE) contains customized Business Source License 1.1 terms, a June 16, 2027 change date, and a commercial-use threshold. It also identifies the licensed work as “Helix Licensing System,” which may not describe this repository. The package metadata points to that file without reinterpreting it. The owner should obtain legal review and correct the licensed-work identity before public distribution or commercial reliance.
