# Productization record

Last updated: 2026-08-31

## Current v0.13 engineering status

The supported v0.12 product remains the single-process SQLite backend described below. The guarded PostgreSQL preview now wires authoritative storage, leased coordination, rate controls, presence/typing, and the durable relay through the application. Merged PRs #23 (`9960ec9`) and #24 (`4c15537`) established the application runtime and real Uvicorn crash/restart test. The older baseline and release results below are historical evidence, not a claim that v0.13 has completed its release gates.

The next P1 slice tests a database connection reset/refusal for one live replica while another remains available. It adds a local TCP fault proxy (test-only; no infrastructure mutation), explicit reconnect/history recovery checks, relay-claim readiness/admission checks, and best-effort lease cleanup that cannot skip pool closure. The reference test uses a three-second lease and one-second pool timeout; silently blackholed connections, database failover, live-lag fencing, load/soak, cross-process moderation/quotas/webhooks, deployment manifests, and verified PostgreSQL-native backup/PITR/rollback remain local engineering gates. See [preview operations](POSTGRES_PREVIEW.md) and the authoritative [release checklist](MULTI_INSTANCE_ARCHITECTURE.md#release-acceptance-gates).

2026-08-31 baseline: clean `main...origin/main` at `4c15537`, no open PRs, and no local PostgreSQL or Docker executable. Local verification of the network-recovery implementation: `ruff check .`, `ruff format`, `mypy samsarix_chat_engine`, and `git diff --check` pass; `pytest -q -m "not postgres" --timeout=60` passes **148 tests**, with 56 deselected. The three deterministic runtime failure tests pass without a database. Both process tests skip locally because `SAMSARIX_TEST_POSTGRES_URL` is unset; PR #25's dedicated PostgreSQL and full quality jobs are the live verification gates. This slice changes no database schema (PostgreSQL 8 / SQLite 5), no production infrastructure, and no paid-service dependency.

## Repository assessment

The repository was extracted from `helix-unified` in commit `007ec96` and then received generic tests, templates, license, and README changes. At audit start it had three implementation modules but no `__init__.py`. Two modules imported `apps.backend.*` directly; on this workstation those imports silently resolved to `C:\Users\Andrew\Helix\helix-unified`, taking about 23 seconds and hiding the undeclared private-repository dependency. In an independent environment the modules were not usable.

The 20 original tests only asserted `dict` and `MagicMock` fixtures. They never imported or executed production code. The three examples imported nonexistent `ChatServer` and `WebSocketManager` APIs. The README linked to missing docs and CI, used commands inconsistent with the layout, described an unrelated multi-agent ecosystem, and claimed “Production Ready.” Packaging required nonexistent `helix-hub-shared>=0.1.0`; `requirements.txt` pinned a large unrelated AI/database/Discord/Celery stack.

No pre-existing worktree changes were present. The starting branch was `main` at `8a26c3b`, matching `origin/main`. No local feature branches, tags, CI configuration, committed secrets, generated artifacts, or lockfile were present. Python library projects normally publish bounded dependency metadata rather than lock transitive application environments, so v0.2 uses a single PEP 621 source of truth and tests supported dependency ranges in CI.

## Baseline evidence

Commands run before implementation:

| Command | Actual baseline result |
| --- | --- |
| `git status --short --branch` | Clean `main...origin/main` |
| `python --version` | Python 3.11.9 |
| `python -m pytest` | 20 passed in 3.36s, but all were fixture-only tests |
| `python -m compileall -q helix_chat_engine examples` | Passed syntax compilation |
| `python -m build` | Failed: `No module named build.__main__` |
| `python -m pip install --dry-run --no-build-isolation -e .` | Failed: no distribution for `helix-hub-shared>=0.1.0` |
| import of `helix_chat_engine.web_chat_server` | Succeeded only after loading `apps.backend` from sibling `helix-unified`; not independent |

## Product definition

Samsarix Chat Engine is a self-hosted, single-instance room chat backend and embeddable FastAPI application. It gives Python developers a narrow, inspectable way to add authorized private rooms, durable text history, and live delivery to a product, support workflow, private community, internal tool, or prototype.

- Target user: a Python application team that already authenticates users and needs embedded room chat without adopting a full collaboration platform or operating Redis/Postgres.
- Primary use case: an operator creates a room, a host application issues a short-lived room token to its authenticated user, the user commits and receives messages, and reconnect recovers authorized history.
- Independent reason to exist: a small reusable service with no private repository dependency and a conventional protocol.
- Product form: installable `samsarix-chat-engine` Python package, `samsarix-chat` CLI, and ASGI app factory.
- Distribution: source checkout or Python wheel under MPL-2.0; no cloud resources are required.
- Sustainability: maintenance/support or commercial embedding can be offered, but no demand or willingness-to-pay is assumed. Default operation incurs no metered API cost.

## Decisions and current research

- Retain FastAPI because the repository already used it and current official FastAPI guidance directly supports WebSocket endpoints, dependencies, and WebSocket-specific close errors.
- Use modern `pyproject.toml` metadata and extras, matching the Python Packaging User Guide's current dependency/optional-dependency guidance. Tests use Starlette's current `httpx2` TestClient backend rather than its deprecated `httpx` fallback.
- Use standard-library SQLite instead of the copied SQLAlchemy/Postgres stack. It completes persistence with no service dependency and keeps local evaluation honest.
- Persist before broadcast and support idempotency. This makes an acknowledged HTTP message durable and makes ordinary client retry safe.
- Keep presence in memory and explicitly label it best effort. Current Centrifugo guidance likewise treats presence as additional state with cost and privacy implications; this product does not pretend to match Centrifugo's multi-node broker/recovery scope.
- Keep one process. SQLite plus an in-process connection manager is coherent at this scale; adding Redis merely for a scaling claim would expand operations and failure modes.
- Package that one-process boundary as a non-root, read-only-root container with a durable `/data` volume and mounted secret files. Docker's official [build guidance](https://docs.docker.com/build/building/best-practices/) supports multi-stage builds, `.dockerignore`, CI image tests, and numeric non-root users; [Compose secret guidance](https://docs.docker.com/compose/how-tos/use-secrets/) supports per-service file mounts instead of ordinary secret environment values. FastAPI's [container guidance](https://fastapi.tiangolo.com/deployment/docker/) supports exec-form shutdown and one application process per container.
- Defer multi-instance claims until storage and coordination are designed together. Redis documents Pub/Sub as [at-most-once](https://redis.io/docs/latest/develop/pubsub/), while Streams add persisted cursors/acknowledgment rather than an authoritative chat database. PostgreSQL documents `LISTEN/NOTIFY` as interprocess signaling over a [shared PostgreSQL database](https://www.postgresql.org/docs/current/sql-notify.html), with payload, transaction, queue, and startup-race constraints. None alone resolves this repository's SQLite ownership, migration/restore exclusion, webhook leadership, or distributed quotas.
- Adopt PostgreSQL—not multi-process SQLite—as the v0.13 multi-instance source of truth. SQLite now documents a WAL-reset race affecting versions through 3.51.2 under concurrent cross-thread/process writes and checkpoints, while WAL remains same-host-only with one writer. The accepted [multi-instance architecture](MULTI_INSTANCE_ARCHITECTURE.md) requires a transactional event log, notification-assisted replay, leases, global quotas, claimed webhook work, migration/maintenance leadership, gap fencing, and real multi-process failure tests before any scale claim.
- Keep identity ownership in the host application. v0.4 accepts a narrowly profiled signed assertion, derives sender identity from it, and enforces room/action authorization on every request and WebSocket publish.
- Let production hosts retain private signing authority. v0.12 accepts a bounded static public JWKS using only Ed25519/EdDSA or RSA-2048+/RS256, binds every key and token to an explicit algorithm and `kid`, and rejects token-controlled remote key locations. Static local configuration avoids JWKS URL SSRF, cache, refresh, and availability failure modes while still permitting overlapping-key rotation.
- Retain the shared API key only as an operator and compatibility credential. It is intentionally all-room and must not be distributed to ordinary browser clients.
- Require exact non-local browser origins even for authenticated deployments; credentials do not remove cross-site WebSocket-hijacking risk.
- Keep read state subject-scoped, monotonic, capacity-bounded, and self-erasable. Current unread counts exclude self-authored and deleted messages, while cursors survive ordinary message retention.
- Keep typing signals ephemeral, transition-only, separately rate-limited, and automatically expired. They are not persistence or audit data and remain best-effort within one process.

Current official product research checked on 2026-08-01 established the practical feature floor without changing the narrow product boundary: [Sendbird Chat](https://sendbird.com/docs/chat) documents channels, receipts, presence, reactions, files, threads, search, moderation, export, webhooks, and privacy controls; [Ably Chat](https://ably.com/chat) emphasizes support/community embeds plus rooms, presence, typing, reactions, edits, moderation, and receipts. [Centrifugo authorization](https://centrifugal.dev/docs/server/authentication) uses signed identities or application-proxy decisions and per-channel permissions, while its [recovery guidance](https://centrifugal.dev/docs/server/history_and_recovery) distinguishes broker recovery from the application's source of truth. These comparisons support the identity/authorization-first v0.4, the accountable data-lifecycle v0.5, conversation controls next, and broker work only after measured single-instance demand.

The v0.5 lifecycle contract follows [RFC 9110 DELETE semantics](https://www.rfc-editor.org/rfc/rfc9110.html): successful deletion returns 204, repeat requests remain safe with respect to final resource state, and confirmation is carried in a header rather than relying on undefined DELETE request-body semantics. The [OWASP Logging Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html) supports recording administrative data changes and exports while excluding access tokens, secrets, and sensitive content. Mattermost's [compliance export documentation](https://docs.mattermost.com/administration-guide/comply/compliance-export.html) reinforces versioned, reconstructable administrative export as a real deployment need without implying compliance. Backup uses SQLite's official [online backup API](https://sqlite.org/backup.html), and the CLI verifies each generated snapshot before atomic placement.

The security design follows the [OWASP API Security Top 10](https://owasp.org/www-project-api-security/) emphasis on object authorization, authentication, and bounded resource use; the [OWASP Authorization Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Authorization_Cheat_Sheet.html) guidance to deny by default and validate every request; the [OWASP WebSocket Security Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/WebSocket_Security_Cheat_Sheet.html) guidance on Origin validation, message authorization, and avoiding URL tokens; and [RFC 8725](https://www.rfc-editor.org/info/rfc8725/) requirements for explicit JWT algorithms, issuer/audience validation, mutually exclusive token rules, and safe key selection. [RFC 7517](https://www.rfc-editor.org/rfc/rfc7517.html) defines the local JWK Set format. PyJWT 2.13 is the bounded JWT dependency and cryptography 46–49 is the optional asymmetric implementation range.

- Make Samsarix LLC the canonical owner identity in v0.3 while retaining the v0.2 import, command, environment, and database names as tested migration aliases.
- Use the unmodified MPL-2.0: file-level copyleft protects distributed changes to covered files and notice preservation while allowing the engine to be combined with separate proprietary files. AGPL-3.0 would cover network use more strongly but materially narrows embedding adoption; Apache-2.0 would permit closed downstream modifications.

Primary references checked on 2026-07-28: [FastAPI's official WebSocket documentation](https://fastapi.tiangolo.com/advanced/websockets/), the [Python Packaging User Guide](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/), [Starlette's TestClient documentation](https://www.starlette.io/testclient/), FastAPI/Pydantic/HTTPX2 PyPI metadata, [Centrifugo's official channel documentation](https://centrifugal.dev/docs/server/channels), [Mozilla's MPL-2.0 FAQ](https://www.mozilla.org/MPL/2.0/FAQ/), the [official MPL-2.0 text](https://www.mozilla.org/MPL/2.0/), and the [GNU license overview](https://www.gnu.org/licenses/).

## Architecture and trust boundaries

Untrusted HTTP and WebSocket payloads enter FastAPI/Pydantic validation. A configured operator key grants administrative access; signed short-lived tokens bind an application subject to rooms and read/write permissions. The server, not the payload, chooses authenticated sender identity. The service commits validated messages to a local SQLite path and then sends them to the in-process room registry. When an operator explicitly configures a webhook, a separate worker sends selected committed event bodies to that one validated destination; otherwise the engine makes no application-level outbound requests.

Deployment owners control login, membership decisions, TLS/proxying, operator and signing secrets, allowed browser origins, filesystem permissions, backups, deletion obligations, and access to the SQLite file. Any client holding the operator key or signing secret can access every room. Multi-process deployment breaks real-time fan-out and per-process rate-limit accounting, so it is unsupported.

## Findings and disposition

### P0

- [x] Remove the nonexistent `helix-hub-shared` installation blocker.
- [x] Remove runtime imports of sibling/private `helix-unified` modules.
- [x] Replace nonexistent public APIs and non-runnable examples with a real package API and CLI.
- [x] Implement the room/message/reconnect path with persistence and failure contracts.
- [x] Replace mock-only tests with production integration coverage.
- [x] Correct false “production ready,” CI, documentation, and license claims in the README.
- [x] Replace the mismatched customized BSL text with the standard MPL-2.0 and align package metadata, copyright notices, company identity, and contact channels.

### P1

- [x] Validate inputs and return stable, non-leaking errors.
- [x] Add optional shared-key authentication and safe loopback CLI behavior.
- [x] Add connection, room, message-size, send-rate, frame-size, and retention bounds.
- [x] Add idempotency, SQLite transactions/WAL/busy timeout, readiness, and graceful WebSocket shutdown.
- [x] Add Linux/Windows CI, lint, format, typing, coverage, build, and package checks.
- [x] Accurately document security, privacy, cost, recovery, and single-process behavior.
- [x] Add host-asserted user identity and server-side per-room authorization before use with mutually untrusted users.
- [x] Add room/message export and deletion administration for deployments with data-subject or retention obligations.
- [x] Add transactionally durable, signed application webhooks with bounded retries, operator recovery, and explicit receiver/network/privacy contracts.
- [x] Add a hardened, single-replica container/Compose deployment with mounted secret files, persistent-volume recovery, and CI smoke.
- [x] Add verification-only asymmetric access tokens with bounded static public-key rotation.
- [ ] Add a broker/presence adapter and cross-instance integration tests before multiple workers or hosts are supported.
- [ ] Run sustained concurrent load/soak tests and publish measured limits before capacity claims.

### P2

- [x] Add explicit room archive/reopen lifecycle state.
- [x] Add a small framework-neutral TypeScript protocol client.
- [x] Add time-based retention in addition to count-based caps.
- [ ] Add OpenTelemetry hooks only if operators demonstrate a need; keep telemetry off by default.
- [ ] Add conditional HTTP caching/ETags for room lists if read load warrants it.

## Implementation checklist

- [x] PEP 621 package with console entry point and bounded runtime dependencies.
- [x] App factory and environment configuration with early validation.
- [x] SQLite rooms/messages, cursor history, idempotency, and retention.
- [x] HTTP room/message API and health/readiness.
- [x] WebSocket auth, ready/history, broadcast, presence, ping, validation, and close behavior.
- [x] Exact quick start, API reference, runnable examples, and contribution commands.
- [x] Real unit/integration/CLI/package-oriented tests and CI.
- [x] Final clean-environment verification and adversarial review.
- [x] Strict signed access tokens, operator separation, per-room/action authorization, and server-enforced sender identity.
- [x] Streaming room export, archive/reopen, confirmed deletion, age retention, metadata-only audit, and backup/restore.
- [x] Standard Webhooks-compatible committed-event outbox, delivery worker, health/replay API, rotation, and failure runbook.
- [x] Multi-stage non-root image, read-only-root Compose profile, secret files, persistent SQLite volume, and container CI gate.

## Release acceptance criteria

- Fresh install from the built wheel succeeds without another repository.
- `samsarix-chat --help`, `--version`, and loopback startup work; the deprecated `helix-chat` alias remains functional.
- The documented room → message → WebSocket → reconnect journey passes end to end.
- Lint, format, type check, tests with the configured coverage floor, build, and wheel metadata checks pass.
- No locally actionable P0 remains.
- Documentation contains no private-infrastructure requirement or unimplemented capability claim.
- License text, source notices, copyright identity, and wheel metadata all agree on MPL-2.0 and Samsarix LLC.

## Completed work

The copied multi-agent/UCF/Discord/Redis/LLM implementation was removed because it was not an independent product and every meaningful branch depended on `helix-unified`. It was replaced with a deliberately smaller FastAPI/SQLite service, safe CLI, stable protocol, bounded resource behavior, real test suite, CI, examples, and aligned documentation.

The adversarial pass additionally found and fixed a SQLite connection-handle leak, unbounded streamed HTTP request bodies, a WebSocket cancellation race exposed by the current Starlette/HTTPX2 backend, stale license metadata, a dead per-message `Location` link, and an inefficient retention query. The transport and persistence limits now have direct tests.

Version 0.3 completed the Helix-to-Samsarix product migration. The distribution, canonical Python package, CLI, environment variables, service metadata, documentation, support policy, and examples now use Samsarix. Compatibility shims preserve v0.2 imports and the old CLI/environment names, and the default database migration logic avoids silently hiding an existing `data/helix-chat.db`.

Version 0.4 establishes the product's first credible multi-user trust boundary. Host applications can issue short-lived room tokens through Python or the CLI; HTTP and WebSocket handlers validate token type, fixed algorithm, issuer, audience, required time/identity/authorization claims, maximum lifetime, and room/action access. Signed identity overrides display-name input, read-only WebSockets cannot publish, non-local browser origins require an explicit allowlist, and the administrative API key remains backwards compatible.

Version 0.5 closes the primary operational privacy gap for controlled single-instance deployments. Operators can stream versioned NDJSON exports, archive and reopen rooms, and irreversibly delete only an already archived room with an exact confirmation header. Archive is enforced at persistence and protocol layers and deterministically notifies/closes connected clients. Optional age retention complements count caps; a bounded administrative audit trail records actors and lifecycle metadata without duplicating message bodies or credentials. The CLI now creates integrity-checked online SQLite backups and atomically restores them with explicit replacement. Schema version 1 migrates in place to version 2, while unknown future versions are refused without mutation.

Version 0.6 supplies the conversation-control layer needed by embedded support, education, private-community, and live-event products. Signed authors can edit or tombstone their own messages; administrators can moderate any message. Room freeze preserves connected readers while reserving writes for administrators. Relative mute and ban controls bind to stable token subjects, with mute preserving reads and ban immediately evicting matching live sockets. Current message state survives reconnect, and audit records only message IDs and moderation metadata. Schema versions 1 and 2 migrate in place to version 3. The design is grounded in the analogous primitives documented by [Sendbird](https://docs.sendbird.com/docs/chat/platform-api/v3/moderation/moderation-overview), [Discord](https://docs.discord.com/developers/resources/message), and [Ably](https://ably.com/docs/chat/rooms/messages).

Version 0.7 reduces adoption friction with a checked-in, framework-neutral TypeScript client. It uses zero runtime dependencies, generated declarations, explicit ESM exports, injected web-standard transports, stable API errors, typed protocol events, first-message authentication, async credential refresh, and bounded reconnect state. The package shape follows [TypeScript's bundled-declaration guidance](https://www.typescriptlang.org/docs/handbook/declaration-files/publishing.html) and [Node's explicit-exports guidance](https://nodejs.org/api/packages.html); the reconnect observer surface reflects the connection states exposed by mature chat SDKs such as [Sendbird](https://sendbird.com/docs/chat/sdk/v4/javascript/event-handler/managing-connection-event-handlers/add-or-remove-a-connection-event-handler). The npm artifact is buildable and verified but remains unpublished until the owner chooses a package namespace/release gate.

Version 0.8 turns those primitives into an explicit support-room workflow. Signed users receive a persistent, non-regressing per-room cursor and a current unread count that excludes their own and deleted messages. They can remove their own state, while a per-room cap bounds storage. WebSocket writers can emit transient typing transitions under an independent limiter; starts refresh an advertised server deadline and stops occur on explicit command, successful publish, disconnect, or timeout without persistence or audit. The TypeScript client covers both contracts, and a runnable two-party support example demonstrates customer-to-agent-to-customer read state. This shape is grounded in [Stream's unread-state model](https://getstream.io/chat/docs/javascript/unread/) and Sendbird's [channel](https://sendbird.com/docs/chat/sdk/v4/javascript/channel/overview-channel) and [message](https://sendbird.com/docs/chat/sdk/v4/javascript/message/overview-message) guidance. It establishes the application state consumed by the separate v0.9 reliability milestone.

Version 0.9 closes that host-application integration gap with a SQLite transactional outbox for selected committed message and moderation events. It follows the [Standard Webhooks specification](https://github.com/standard-webhooks/standard-webhooks/blob/main/spec/standard-webhooks.md) for the stable ID, per-attempt timestamp, exact-body HMAC-SHA256 signature, rotation list, receiver idempotency, retry, timeout, HTTPS, and SSRF considerations. The single worker provides restart-safe at-least-once delivery, a bounded multi-day schedule with jitter and `Retry-After`, metadata-only health pagination, and manual replay. GitHub's [validation guidance](https://docs.github.com/en/webhooks/using-webhooks/validating-webhook-deliveries) supports raw-body constant-time HMAC verification, while its [webhook practices](https://docs.github.com/en/webhooks/using-webhooks/best-practices-for-using-webhooks) support HTTPS, fast acknowledgement, and stable delivery-ID replay protection. Stripe's [delivery behavior](https://docs.stripe.com/webhooks#event-delivery-behaviors) reinforces automatic retries and the absence of an ordering guarantee. The implementation deliberately rejects redirect/query-secret destinations, blocks non-public resolution by default, pins the validated address for the connection, retains no receiver body, scrubs related payload copies on message/room deletion and age/count retention, and fails a mutation rather than silently dropping its event only when an all-pending outbox is full.

Version 0.10 adds retrieval against the named support-case journey rather than prematurely attempting generic collaboration search. Official [Sendbird message-search guidance](https://docs.sendbird.com/docs/chat/platform-api/v3/message/message-search/message-search-overview) and [Stream search guidance](https://getstream.io/chat/docs/php/search/) establish channel-scoped retrieval as a practical chat capability; [Ably's external-storage guidance](https://ably.com/docs/chat/external-storage-and-processing/data-storage) also makes the boundary clear when an application needs a separately governed long-term index. Samsarix therefore supplies authorized, Unicode-normalized substring matching over only one room's current retained messages, with cursor pagination and a separate limiter. The existing per-room history cap bounds a linear SQLite scan, schema 5 remains unchanged, and global/fuzzy/ranked search remains an explicit external-system decision.

Final v0.10 local verification on 2026-08-02 used Node 24.12.0, CPython 3.11.9 for the declared development environment, and CPython 3.14.6 for the clean installed artifact:

The initial verified implementation is commit `84dfecea8114a2e65e4ac4df1f936e1211eb2926`; Unicode/pagination review hardening and the final artifact surface are commit `4b1456dd1a655a173fc80eca0ad358a37e93cf52`.

| Gate | Result |
| --- | --- |
| Ruff lint / format, mypy, compile, diff check | Passed |
| Python integration and branch coverage | 105 passed; 88.66% total branch coverage, above the 85% gate |
| Python dependency audit | No known third-party vulnerabilities; unpublished local project distributions were skipped by registry lookup |
| TypeScript check / Node test runner | Passed; strict declaration build and 18/18 fake-transport/API tests |
| TypeScript audit / package inspection | Zero runtime dependencies, no known vulnerabilities, and 23 intended artifact files |
| real TypeScript integration smoke | Authenticated HTTP, search/edit convergence, read state, WebSocket auth/history/publish, reconnect, edit, delete, and tombstone recovery passed |
| Python build / Twine | Wheel and source distribution built in isolation and both passed metadata checks |
| clean Python 3.14 wheel install | Version 0.10.0 resolved from `site-packages`; HTTP, search, WebSocket, read state, typing, controls, seven exact-body signed webhooks, export, lifecycle, backup, and graceful shutdown passed |

Artifact SHA-256 digests:

- wheel: `6B837826522E3708ECE1AA51118B46FC9F64C45ADF14FD91BD845E64631AB239`
- source distribution: `7235B3EBE89DD88FD078078038FE5A672417F510949824A77E473296ED678C1F`

No database migration is introduced: v0.10 remains on schema 5. A rollback to v0.9 requires stopping v0.10, removing the optional search-rate environment variable, and starting v0.9 against the same database. Search queries may be copied into ordinary proxy access logs, so those logs remain part of the room-content privacy boundary. Sustained search load, multi-process behavior, fuzzy/global ranking, and an external security assessment were not claimed or run. If measured room size or query concurrency outgrows bounded scans, the next search-specific design gate is a reviewed SQLite FTS5 table or a write-maintained normalized index—not a silent scale claim.

Final v0.9 local verification on 2026-08-02 used Node 24.12.0, CPython 3.11.9 for the declared development environment, and CPython 3.14.6 for the clean installed artifact:

| Check | Actual result |
| --- | --- |
| `ruff check .` / `ruff format --check .` | Passed; 49 Python/Markdown files formatted |
| `mypy samsarix_chat_engine scripts` / `compileall` / `git diff --check` | Passed; no type issues in 12 source/script files |
| `pytest --cov=samsarix_chat_engine --cov-report=term-missing` | 101 passed in 105.07s; 88.41% total branch coverage; no warnings |
| TypeScript `check` / Node test runner | Passed; strict declaration build and 17/17 fake-transport/API tests |
| TypeScript audit / package inspection | Zero runtime dependencies, no known vulnerabilities, and 23 intended artifact files |
| Python sdist / wheel / `twine check` | Wheel built through the sdist; webhook docs, source, smoke, tests, license, and notice included; both metadata checks passed |
| clean Python 3.14 wheel install | Version 0.9.0 resolved from `site-packages`; Samsarix LLC/contact/support/MPL metadata and declared dependencies passed |
| expanded installed-wheel smoke | HTTP, WebSocket, read state, typing, controls, seven exact-body signed webhooks, export, lifecycle, backup, and graceful shutdown passed |
| source/runtime dependency audits | No known third-party vulnerabilities; the unpublished local Samsarix distribution was explicitly reported as unauditable |

Exact artifact digests are recorded in the pull request because embedding them in packaged source would change those digests. The GitHub matrix remains the cross-platform merge gate; artifact digests will change if review fixes alter the source.

Final v0.8 local verification on 2026-08-01 used Node 24.12.0, CPython 3.11.9 for the declared development environment, and CPython 3.14.6 for the clean installed artifact:

| Check | Actual result |
| --- | --- |
| `ruff check .` / `ruff format --check .` | Passed; 46 Python files formatted |
| `mypy samsarix_chat_engine` / `compileall` / `git diff --check` | Passed; no type issues in 9 source files |
| `pytest --cov=samsarix_chat_engine --cov-report=term-missing` | 89 passed in 93.99s; 90.94% total branch coverage; no warnings |
| TypeScript `check` / Node test runner | Passed; strict declaration build and 17/17 fake-transport/API tests |
| TypeScript production audit / package inspection | Zero runtime dependencies, no known vulnerabilities, and 23 intended artifact files |
| real TypeScript integration smoke | Authenticated HTTP, read state, first-message WebSocket auth, history, publish, reconnect, edit, delete, and tombstone recovery passed |
| Python sdist / wheel / `twine check` | Wheel built from the 81-entry sdist; 24 wheel entries; both metadata checks passed |
| clean npm and wheel installs | ESM/type import passed; installed wheel resolved from `site-packages` and passed HTTP, WebSocket, read-state, typing, controls, export, lifecycle, and backup smoke |
| source/runtime dependency audits | No known third-party vulnerabilities; unpublished local Samsarix distributions were explicitly reported as unauditable |

Exact Python sdist, Python wheel, and npm tarball digests are recorded in the pull request because embedding them in packaged source would change the source archive digest.

Final v0.7 local verification on 2026-08-01 used Node 24.12.0 and CPython 3.14.6, including clean installs of both built artifacts:

| Check | Actual result |
| --- | --- |
| `ruff check .` / `ruff format --check .` | Passed; 30 Python files formatted |
| `mypy samsarix_chat_engine` / `compileall` | Passed; no issues in 9 source files |
| `pytest --cov=samsarix_chat_engine --cov-report=term-missing` | 82 passed in 27.17s; 91.20% total branch coverage |
| TypeScript `check` / Node test runner | Passed; strict declaration build and 15/15 fake-transport/API tests |
| TypeScript production audit / package inspection | Zero runtime dependencies, no known vulnerabilities, and 23 intended artifact files |
| real TypeScript integration smoke | Authenticated HTTP, first-message WebSocket auth, history, publish, edit, delete, and tombstone recovery passed |
| Python sdist / wheel / `twine check` | Wheel built from the 78-entry sdist; client source included, generated/dependency trees excluded, and both metadata checks passed |
| clean npm and wheel installs | ESM/type package import and expanded installed-wheel HTTP/WebSocket/lifecycle/backup smoke passed |

Exact Python sdist, Python wheel, and npm tarball digests are recorded in the pull request because embedding them in packaged source would change the source archive digest.

Final v0.6 local verification on 2026-08-01 used CPython 3.11.9 for the source suite and a clean CPython 3.14.6 environment for the installed artifact:

| Check | Actual result |
| --- | --- |
| `ruff check .` / `ruff format --check .` | Passed; 41 Python files formatted |
| `mypy samsarix_chat_engine` / `compileall` / `git diff --check` | Passed; no type issues in 9 source files |
| `pytest --cov=samsarix_chat_engine --cov-report=term-missing` | 82 passed in 136.80s; 91.20% total branch coverage |
| fresh wheel-runtime `pip check` / `pip-audit --path` | No broken requirements; no known third-party vulnerabilities |
| sdist build / wheel rebuilt from sdist / `twine check` | Both 0.6.0 artifacts built and passed metadata checks |
| final wheel installed outside the source tree | Version 0.6.0, Samsarix LLC/contact metadata, dependency resolution, and imports passed |
| expanded installed-wheel smoke | HTTP/WebSocket persistence, edit/delete, freeze, mute/clear, export schema 2, archive/delete, backup, and graceful shutdown passed |

Exact artifact digests are recorded with the pull request because embedding them in the packaged source would change those digests. The GitHub matrix remains the cross-platform merge gate; artifact digests will change if review fixes alter the source.

Initial v0.5 verification on 2026-08-01 used CPython 3.14.6 on Windows and the newest resolved runtime versions inside the declared bounds:

| Check | Actual result |
| --- | --- |
| `ruff check .` / `ruff format --check .` | Passed; 28 Python files formatted |
| `mypy samsarix_chat_engine` | Passed; no issues in 9 source files |
| `pytest --cov=samsarix_chat_engine --cov-report=term-missing` | 72 passed in 46.89s; 91.47% total branch coverage |
| `pip check` / isolated wheel-runtime `pip-audit` | No broken requirements; no known third-party vulnerabilities |
| `python -m build` / `twine check` | Source archive and universal wheel built from the sdist; both passed metadata checks |
| final wheel installed outside the source tree | Version/import/metadata and `pip check` passed |
| expanded installed-wheel smoke | Authorized HTTP/WebSocket persistence plus NDJSON export, archive/delete, online backup, SQLite creation, and graceful shutdown passed |

The lifecycle tests cover admin authorization, audit-content exclusion, idempotent archive, read-only enforcement, active-client teardown/reopen, confirmation failures, 1005-message streaming across batches, explicit and automatic retention counts, v1 migration/data preservation, future-schema refusal, cross-process restore exclusion, stale-sidecar removal, and Windows-safe backup/restore replacement. Exact final artifact digests are recorded in the pull request because including them in the packaged source would change those digests.

Initial v0.4 verification on 2026-08-01: the unchanged v0.3 baseline had 27 passing tests; after implementation and review hardening, `pytest --cov=samsarix_chat_engine --cov-report=term-missing` passed 59 tests in 33.02 seconds with 92.86% branch coverage. The new authorization tests cover tampering, expiry, issuer/audience confusion, malformed signed claims, self-verifiable token size, room and action denial, sender spoofing, subject-wide WebSocket rate limits, OpenAPI security schemes, browser WebSocket authentication, read-only sessions, origin enforcement, and CLI issuance. Final artifact and installed-wheel evidence is recorded after exact-head verification.

Final v0.4 local verification used CPython 3.11.9 and the newest resolved versions inside the declared bounds, including FastAPI 0.141.1, Uvicorn 0.52.1, PyJWT 2.13.0, and WebSockets 17.0.1:

| Check | Actual result |
| --- | --- |
| `ruff check .` / `ruff format --check .` | Passed; 36 Python files formatted |
| `mypy samsarix_chat_engine` | Passed; no issues in 9 source files |
| `pytest --cov=samsarix_chat_engine --cov-report=term-missing` | 59 passed in 33.02s; 92.86% total branch coverage |
| `pip check` | No broken requirements |
| `pip-audit` | No known third-party vulnerabilities; unpublished local distributions skipped |
| `python -m build` / `twine check` | Final sdist and universal wheel built from the sdist; both passed metadata checks |
| final wheel installed outside the source tree | Version/import/metadata and `pip check` passed |
| `scripts/smoke_installed_wheel.py` under final wheel runtime | Real operator room creation, token issuance, authenticated HTTP persistence/history, browser-style WebSocket auth/recovery/publish, SQLite creation, and graceful shutdown passed |

Exact artifact digests are recorded with the pull request because embedding a digest inside its own source archive is self-referential. The GitHub Actions matrix, external security review, and sustained load/soak tests were not run locally and remain named gates rather than implied evidence.

## Version 0.2 verification evidence

All source checks below ran in a newly created `.venv-productization` environment installed only from `.[dev]`. That install resolved FastAPI 0.140.7, Starlette 1.3.1, Pydantic 2.13.4, Uvicorn 0.51.0, HTTPX2 2.9.1, and the declared quality tools without a resolver error.

| Command | Actual result |
| --- | --- |
| `python -m ruff check .` | Passed |
| `python -m ruff format --check .` | Passed; 22 files already formatted |
| `python -m mypy helix_chat_engine` | Passed; no issues in 8 source files with the Python 3.10 target |
| `python -m pip_audit` | No known vulnerabilities found; local package skipped because it is not on PyPI |
| `python -m pytest --cov=helix_chat_engine --cov-report=term-missing` | 23 passed in 69.93s; 91.33% total branch coverage; no warnings |
| `python -m build` | Built sdist and universal wheel without metadata warnings |
| `python -m twine check dist/*` | Both artifacts passed |
| `python -m compileall -q helix_chat_engine examples` | Passed |
| `helix-chat --version` / `helix-chat serve --help` | Returned version 0.2.0 and documented options |
| five repeated multi-client WebSocket tests | 5/5 passed after the cancellation fix |

A second `.venv-runtime` environment installed only the built wheel and runtime dependencies. From `.venv-runtime/smoke` (outside the source directory), `pip check` reported no broken requirements and `helix_chat_engine.__file__` resolved inside `site-packages`. The wheel's CLI started on loopback, `/readyz` returned ready, an HTTP room/message round trip returned persisted content, and `examples/02_websocket_chat.py` received `ready`, recovered history, and a committed `message.created` event. The earlier source-install smoke also ran both shipped REST and WebSocket examples successfully. Ctrl+C stopped both test servers and released their listening ports.

The first advisory scan identified only the system Python's inherited `setuptools 65.5.0`. The build/dev floor and setup instructions were raised to `setuptools >=83`, both isolated environments were upgraded to 83.0.0, and repeated `pip-audit` checks reported no known vulnerabilities in either the full development environment or the wheel-only runtime path. The local `helix-chat-engine` package itself was explicitly skipped because it is not published on PyPI; Ruff security rules and direct threat review cover its source.

Locally available verification was Windows 10/11 with CPython 3.11.9. The configured GitHub Actions matrix for Linux CPython 3.10–3.14 and Windows CPython 3.12 was not executed locally and remains the normal pre-release merge gate. No sustained load/soak test, external security assessment, or multi-process test was run; those unsupported scopes are listed as P1 work rather than implied as passing.

## Version 0.3 verification evidence

The Samsarix migration was verified again from source and from the final wheel on 2026-07-28:

| Check | Actual result |
| --- | --- |
| `python -m ruff check .` / `ruff format --check .` | Passed; 31 Python files formatted |
| `python -m mypy samsarix_chat_engine` | Passed; no issues in 8 canonical source files |
| `pytest --cov=samsarix_chat_engine` | 27 passed in 19.68s; 91.70% total branch coverage |
| runtime-only `pip-audit` | No known vulnerabilities; unpublished local package skipped |
| `python -m build` / `twine check` | Source archive and universal wheel built and passed |
| `python -m compileall` / `pip check` / `git diff --check` | Passed |
| official MPL comparison | Local `LICENSE` matched Mozilla's official MPL-2.0 text after line-ending/trailing-space normalization |

The source archive contains the security policy, documentation, examples, license, notice, and both Python package names. A fresh `.venv-samsarix-runtime` installed only the final wheel and its runtime dependencies. Both commands returned 0.3.0, both imports resolved from `site-packages`, the legacy import emitted its deprecation warning, metadata reported `MPL-2.0` with `LICENSE` and `NOTICE`, and `pip check` passed. The installed `samsarix-chat` command then started on loopback, created a room and persisted message over real HTTP, returned that history, wrote its SQLite database, and shut down gracefully.

This v0.3 verification was local on Windows with CPython 3.11.9. The configured GitHub Actions matrix remains the cross-platform merge gate and had not run before the branch push.

## Deferred and blocked work

Multi-instance storage plus fan-out, attachment storage policy, reactions only against named journeys, and sustained load testing are genuine next-stage engineering, ordered in the roadmap. The current single-process workflow, separated token-signing trust, reliable committed-event integration, and hardened container profile are complete enough for controlled application evaluation; horizontal scale and capacity claims remain intentionally deferred until measured.

Public package publication, hosted deployment, domains, credentials, signing, and pricing remain owner-controlled. No external accounts, infrastructure, releases, or spending were created as part of the local productization work.

## Release disposition

**Alpha release candidate for controlled single-instance evaluation.** The developer product has no known locally actionable P0, and its source, package, authorized primary journey, limits, error states, documentation, brand identity, and standard open-source license are coherent. Production or regulated use still requires deployment-specific identity integration, lifecycle operations, capacity evidence, and external security review.

## Known risks

- The operator API key and optional HS256 token signing secret are high-impact symmetric credentials; prefer public-JWKS verification where practical.
- The webhook HMAC secrets authorize receiver trust and payloads duplicate selected plaintext content in the outbox/receiver. Each attempt validates resolution and pins the selected address while preserving TLS hostname verification, but deployment-level egress/routing controls remain necessary defense in depth.
- Access tokens have bounded expiry and optional asymmetric keys, but no per-token revocation list or automatic remote key refresh; JWKS changes require restart.
- SQLite and the in-process connection registry intentionally limit scale and topology.
- Count-based deletion has no audit log and is unsuitable where legal holds are required.
- Message content is plaintext at rest unless the deployment encrypts the filesystem.
- Presence is at-most-once and may be stale briefly around abrupt disconnects.
- In-memory rate limits reset on restart and are not coordinated across processes.
