# Identity and room authorization

Version 0.12 supports two credential classes with deliberately different jobs:

- `SAMSARIX_CHAT_API_KEY` is the deployment-wide operator credential. It can create and list rooms, inspect process statistics, and access every room. Keep it on trusted servers and administration workstations.
- A signed access token represents one application user. It contains a subject, an expiry, allowed room IDs, and `room:read`, `room:write`, `room:pin`, `room:read-receipts`, or `admin` permissions. Give ordinary clients only short-lived room tokens.

The engine does not implement signup, passwords, sessions, or an identity database. Your host application authenticates its user, decides which rooms they may access, and issues a token with `AccessTokenService` or a compatible JWT implementation. The engine can either share an HS256 signing secret or hold only a static public JWKS for verification.

## Shared-secret mode

Generate a high-entropy secret of at least 32 bytes and set it only on trusted token issuers and the chat service:

```bash
SAMSARIX_CHAT_TOKEN_SIGNING_SECRET=<random-secret>
SAMSARIX_CHAT_TOKEN_ISSUER=samsarix-chat-engine
SAMSARIX_CHAT_TOKEN_AUDIENCE=samsarix-chat
```

For a local evaluation, issue a one-hour room token from the CLI:

```bash
samsarix-chat token issue \
  --subject user-123 \
  --room general \
  --permission room:read \
  --permission room:write \
  --expires-in 3600
```

The token is printed to stdout. Treat terminal history, CI output, and captured logs accordingly. The command refuses missing or short signing secrets, invalid room IDs, unknown permissions, and lifetimes outside the configured 60-second to seven-day bounds.

For production integrations, issue tokens inside the host application's authenticated backend:

```python
from samsarix_chat_engine import AccessTokenService

tokens = AccessTokenService(
    signing_secret,
    issuer="samsarix-chat-engine",
    audience="samsarix-chat",
)
access_token = tokens.issue(
    subject=current_user.id,
    rooms=["general"],
    permissions=["room:read", "room:write"],
    expires_in_seconds=900,
)
```

## Verification-only JWKS mode

For production trust separation, keep private signing keys in the host application and mount only their public JWK representations into the chat service. Install the optional crypto dependency when not using the supplied container:

```bash
python -m pip install ".[asymmetric-auth]"
SAMSARIX_CHAT_TOKEN_VERIFICATION_JWKS_FILE=/run/secrets/token-verification.jwks.json
SAMSARIX_CHAT_TOKEN_ISSUER=samsarix-chat-engine
SAMSARIX_CHAT_TOKEN_AUDIENCE=samsarix-chat
```

The file is a standard JWK Set. Every key requires a unique bounded `kid`, an explicit `alg`, and public verification material only:

```json
{
  "keys": [
    {
      "kty": "OKP",
      "crv": "Ed25519",
      "x": "<base64url-public-key>",
      "kid": "2026-08-primary",
      "alg": "EdDSA",
      "use": "sig",
      "key_ops": ["verify"]
    }
  ]
}
```

EdDSA with Ed25519 is the compact recommended choice; RS256 with RSA keys of at least 2048 bits is also accepted. Tokens must carry the matching protected `kid` and `typ: samsarix-access+jwt`. The set is read at startup, limited to 64 KiB and 32 keys, and never fetched from a token-controlled URL. Private, symmetric, duplicate, encryption-purpose, ambiguous-algorithm, or weak RSA entries fail startup.

To rotate without interrupting valid sessions:

1. add the next public key alongside the current key and restart the engine;
2. switch the host application to sign new tokens with the next `kid`;
3. wait at least the maximum token lifetime plus clock skew;
4. remove the retired public key and restart again.

Do not set `SAMSARIX_CHAT_TOKEN_SIGNING_SECRET` at the same time. Switching modes intentionally invalidates tokens from the previous trust rule. The built-in `token issue` command is HS256-only because verification-only mode never gives the engine private signing authority.

## Send the token

HTTP clients use the bearer scheme:

```text
Authorization: Bearer <access-token>
```

Browser WebSocket clients omit the `username` query parameter, wait for `auth.required`, and send:

```json
{"type":"auth","token":"<access-token>"}
```

The persisted message sender and WebSocket presence name are derived from the signed `sub` claim. A conflicting client-supplied `sender` or `username` is rejected with `identity_mismatch`.

## Permission matrix

| Operation | Required access |
| --- | --- |
| Create/list rooms, view `/v1/stats` | operator API key or `admin` token |
| Get a room or its message history | room listed in token plus `room:read` |
| Get, advance, or clear personal read state | stable signed subject, room listed in token, plus `room:read` |
| Query read state for 1–100 rooms | stable signed subject, every room listed in token, plus `room:read` |
| Query or receive participant read receipts | room listed in token, plus both `room:read` and `room:read-receipts` |
| Connect WebSocket and receive events | room listed in token plus `room:read` |
| Post or send typing state over WebSocket | room listed in token plus `room:write` |
| Pin or unpin a shared message | room listed in token plus both `room:read` and `room:pin` |

A read-only WebSocket remains connected when it attempts to publish and receives an `authorization_denied` event. Authorization is checked on every HTTP operation and every WebSocket publish command.

## Token profile and trust boundary

Tokens use a mode-specific fixed algorithm allowlist—HS256 for shared-secret mode, or EdDSA/RS256 for JWKS mode—and the protected type `samsarix-access+jwt`. Verification requires `iss`, `aud`, `sub`, `iat`, `nbf`, `exp`, `jti`, `rooms`, and `permissions`; it rejects excessive lifetimes and malformed or duplicate authorization claims. Defaults are a 24-hour maximum lifetime and 30 seconds of clock skew.

HS256 means every verifier can also mint tokens, so share that secret only with trusted backend components. JWKS mode removes private signing authority from the engine and supports planned overlapping-key rotation, but the file still requires an operator restart and token revocation lists or remote automatic key refresh are not implemented. Prefer lifetimes measured in minutes for browser clients. Token headers that could redirect key selection (`jku`, `x5u`, or `x5c`) and critical/unencoded extensions are rejected. The webhook signing-secret rotation window is a separate outbound-delivery mechanism described in [Reliable application webhooks](WEBHOOKS.md). Container deployments should use mounted secret files described in [Container deployment](CONTAINER_DEPLOYMENT.md).

Read state uses the exact signed `sub` as its durable key. Choose an opaque stable account ID, never a mutable display name or email address. The operator API key deliberately cannot access personal read-state endpoints because it represents a deployment rather than one end user. A cross-room query checks every requested ID against the same token before storage lookup and never returns a partial authorized subset.

Receipt lookup is a separate privilege because participant activity can be sensitive. The caller supplies an explicit bounded subject set from the host application's membership model; Samsarix never lists room members. Ordinary users may mark and clear only their own cursor without receiving other participants' state. Grant `room:read-receipts` only where the product has a clear visibility and consent policy.

Never put API keys or tokens in URLs. Configure TLS at the reverse proxy, an exact `SAMSARIX_CHAT_ALLOWED_ORIGINS` list for browser deployments, filesystem protections for SQLite, and log redaction at upstream gateways.
