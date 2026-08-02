# Getting started

This walkthrough starts one local Samsarix Chat Engine, creates a room, exchanges a live message, and verifies that history survives a restart.

## 1. Install

Use Python 3.10 or newer from the repository root:

```bash
python -m venv .venv
```

Activate `.venv\Scripts\Activate.ps1` on PowerShell or `source .venv/bin/activate` on POSIX, then install:

```bash
python -m pip install --upgrade pip setuptools wheel
python -m pip install ".[test]"
```

## 2. Start safely on loopback

```bash
samsarix-chat serve
```

Expected startup address: `http://127.0.0.1:8000`. Open `/docs` for the generated OpenAPI explorer. `GET /healthz` checks the process; `GET /readyz` checks SQLite.

## 3. Create a room and seed history

```bash
curl -X POST http://127.0.0.1:8000/v1/rooms \
  -H "Content-Type: application/json" \
  -d '{"id":"general","name":"General","description":"Local chat"}'

curl -X POST http://127.0.0.1:8000/v1/rooms/general/messages \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: seed-1" \
  -d '{"sender":"setup","content":"The room is ready"}'
```

Repeating the second request with the same idempotency key returns the original message with HTTP 200 rather than creating a duplicate.

## 4. Connect a browser client

Run this in the browser developer console on a page served from localhost:

```javascript
const socket = new WebSocket(
  "ws://127.0.0.1:8000/v1/rooms/general/ws?username=Browser"
);
socket.onmessage = (event) => console.log(JSON.parse(event.data));
socket.onopen = () => socket.send(JSON.stringify({
  type: "message",
  content: "Hello from the browser",
  client_message_id: crypto.randomUUID()
}));
```

The server sends `ready`, `history`, and then `message.created`. You can instead run `python examples/02_websocket_chat.py`.

## 5. Verify restart recovery

Stop the service with Ctrl+C, run `samsarix-chat serve` again, and request:

```bash
curl http://127.0.0.1:8000/v1/rooms/general/messages
```

The response contains the committed messages because the default database is `data/samsarix-chat.db`.

## Add application-user authorization

Use an operator API key to administer rooms and a signing secret to mint short-lived user tokens:

```bash
export SAMSARIX_CHAT_API_KEY="replace-with-a-random-operator-secret"
export SAMSARIX_CHAT_TOKEN_SIGNING_SECRET="replace-with-at-least-32-random-bytes"
samsarix-chat serve
```

In PowerShell, assign the same names through `$env:...`. Keep both values on trusted backends. Create rooms with `X-API-Key`, then issue a room token for an already-authenticated application user:

```bash
samsarix-chat token issue --subject user-123 --room general --expires-in 900
```

HTTP application clients send `Authorization: Bearer <token>`. They can omit `sender`; the engine persists the signed subject and rejects spoofed identities. The default issued permissions are `room:read` and `room:write`.

Browser WebSockets do not expose arbitrary handshake headers, so the server sends `auth.required`. Reply before the configured five-second deadline:

```javascript
socket.onmessage = (event) => {
  const message = JSON.parse(event.data);
  if (message.type === "auth.required") {
    socket.send(JSON.stringify({type: "auth", token: accessToken}));
  }
};
```

When using a token, connect without `?username=` because the server derives identity from the token. Do not put credentials in WebSocket URLs: query strings are routinely recorded by servers and proxies. See [Identity and room authorization](AUTHORIZATION.md) for the permission matrix, token profile, and backend issuance example.

## Troubleshooting

- `401 authentication_required`: the HTTP API requires a valid operator key or access token.
- `403 authorization_denied`: the token lacks the action or room permission.
- `403 identity_mismatch`: a client-provided sender or username conflicts with the signed subject.
- WebSocket close `4401`: authentication was missing, invalid, or late.
- WebSocket close `4403`: the browser `Origin` is not allowed, the token lacks room access, or the username conflicts with its signed subject.
- WebSocket close `4404`: create the room before connecting.
- WebSocket close `4409`: an operator archived the room; reconnect only after it is reopened.
- WebSocket close `1013`: the configured connection cap is full; retry with backoff.
- `503 storage_unavailable` or `/readyz` returning 503: check the database directory permissions and available disk.
- `507 webhook_capacity_reached`: the optional delivery outbox contains only pending rows at its cap; restore the receiver so automatic attempts drain those rows, or raise the cap with matching disk capacity. After correcting a terminal failure, retry that specific row through `/v1/admin/webhook-deliveries/{delivery_id}/retry`.
- CLI refuses a public bind: configure an API key, token signing secret, or static verification JWKS, or bind to loopback. The insecure override is only for isolated development networks.

## Upgrading to 0.12

Version 0.12 continues to use SQLite schema 5. Upgrading from v0.11 requires no database migration. Take an integrity-checked backup first with `samsarix-chat database backup backups/pre-0.12.db`, then start v0.12. Existing HS256 settings continue to work. To adopt verification-only asymmetric tokens, install `.[asymmetric-auth]`, stage a public JWKS, and use the explicit cutover/rotation sequence in [Identity and room authorization](AUTHORIZATION.md). See [Data lifecycle operations](OPERATIONS.md) for migration, verification, and rollback details.
