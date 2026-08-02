# Security policy

## Supported versions

Security fixes are currently made on the latest source revision. No released version has a long-term support commitment while the project remains in alpha.

## Reporting a vulnerability

Please report suspected vulnerabilities privately to support@samsarix.com. Include the affected revision, reproduction steps, likely impact, and any known mitigations. Do not include live API keys, private chat content, or other third-party data.

Samsarix LLC will acknowledge a complete report as availability permits, coordinate remediation and disclosure in good faith, and credit reporters who request attribution when doing so is safe. This policy does not promise a bounty or a fixed response deadline.

## Deployment boundary

The engine is not an identity provider. Deployments authenticate users in a host application, issue short-lived room tokens, terminate TLS at a trusted proxy, configure exact browser origins, and protect the SQLite file plus operator/token/webhook credentials. The operator API key and HS256 token secret grant high-impact access and must never be shipped to ordinary clients. Prefer static public JWKS verification when the host can retain private Ed25519 or RSA signing authority. Moderation targets the signed token subject, not an untrusted display name; operators must secure moderator workflows and decide their own appeal/retention policy.

The checked-in container runs as a non-root user with a read-only root filesystem in the Compose profile, but the Docker daemon, host, reverse proxy, mounted secrets, and persistent volume remain trusted boundaries. Run exactly one process/replica, never mount the Docker socket, retain the loopback-only port mapping behind TLS, and prefer the `_FILE` secret variables. See [Container deployment](docs/CONTAINER_DEPLOYMENT.md).

Opt-in webhooks transfer selected plaintext content and identifiers to an operator-configured receiver. Remote targets require HTTPS, redirects are not followed, non-public address resolution is blocked by default, the validated address is pinned for each connection, and requests are signed, but deployment routing still requires independent egress controls. Receivers must verify the raw body/timestamp, deduplicate stable IDs, protect signing secrets, and own downstream erasure; local deletion cannot recall accepted data and may race a worker-claimed delivery. Room exports, webhook outbox copies, and database backups contain plaintext chat data—including content deleted after the snapshot—and require the same access, transport, retention, and disposal controls as the live database. See [Identity and room authorization](docs/AUTHORIZATION.md), [Conversation controls](docs/CONVERSATION_CONTROLS.md), [Reliable application webhooks](docs/WEBHOOKS.md), and [Data lifecycle operations](docs/OPERATIONS.md) for the relevant boundaries.
