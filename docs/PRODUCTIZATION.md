# Productization record

Last updated: 2026-08-31

## Current v0.13 engineering status

Latest verified baseline: clean `main...origin/main` at `542dcee` (merged PR #31), no open PRs, and all ten [post-merge CI jobs](https://github.com/Deathcharge/samsarix-chat-engine/actions/runs/33391994996) passing: **343 tests, two SQLite-inapplicable skips, 89.26% branch-inclusive coverage**. The previous goal turn made verified implementation progress. This increment addresses the next P1: delayed pre-admission relay events overriding a later reconnect.

Four baseline failures demonstrated stale archive, ban, presence and frozen-room events reaching a newly admitted socket. Admission now returns the sequence of its own join event from the reservation transaction. Runtime registration retains that exclusive lower bound; the relay supplies event sequences out of band, and the manager filters under its shared registration/detachment lock. An event at or before admission cannot enqueue a snapshot or close that socket. Later events still reach older eligible sockets. Unsequenced local operations and global fences retain their existing behavior. This reuses the existing commit-order sequencer without a schema, query or lock addition; it does not sample wall clocks, the relay cursor, or a later database head.

Local verification: `python -m ruff check .`, `python -m ruff format --check .`, `python -m mypy samsarix_chat_engine`, and `git diff --check` pass. `python -m pytest -q -m "not postgres" --timeout=60 --tb=short` passes **335 tests, two inapplicable skips, 80 deselected**. `python -m pytest -q tests/test_postgres_runtime.py tests/test_event_fence.py --timeout=30 --tb=short` passes **81 tests**. Coverage includes all eleven dispatched event types below/at/above admission, active/pending sockets, origin/room/subject matching, pre-admission backlog budget exclusion, registration-lock interleaving and invalid sequence values. The original presence test no longer accepts the previously delayed pre-admission join, and snapshot convergence now expects only the three post-admission mutations.

Six new live PostgreSQL cases use explicit relay barriers. Lifecycle writes originate from another real ASGI application replica; archive/reopen, archive/delete/recreate, ban/unban and freeze/unfreeze must preserve a later signed-member admission while still affecting older sockets. A presence case proves the old join is suppressed for the new peer while the new join reaches the existing peer. A final authorization control commits a ban between initial validation and reservation, holds relay dispatch, and requires the post-admission check to reject the member and release the physical lease. The join sequence is also compared directly with its persisted event record. These are real-storage/application tests, not independent OS-process fault injection. No local PostgreSQL/Docker executable is available.

Initial [PR #32 CI](https://github.com/Deathcharge/samsarix-chat-engine/actions/runs/33393843484) at `21f1187` passed all ten jobs: **415 tests, two skips, 89.33% branch-inclusive coverage**, including **80 live PostgreSQL tests**, the TypeScript journey and hardened container smoke. Local `python -m build --outdir dist/pr32-21f1187`, `python -m twine check dist/pr32-21f1187/*`, and a fresh runtime-only wheel installation with `pip check` and `scripts/smoke_installed_wheel.py` all passed; HTTP/search/WebSockets/read state/typing/moderation/webhooks/export/lifecycle/backup were exercised. The final authorization control was added after this run, so final-head CI and rebuilt artifacts remain required before merge. Exact final results and artifact digests are recorded on the PR; the third-party review integration skipped its draft review, not an independent approval.

Remaining ordered P1 work: live-lag fencing; real-process update/delete, quota contention and webhook-worker crash recovery; combined lifecycle/outage/reconnect storms and measured load/soak; deployment manifests and PostgreSQL-native backup/PITR/rollback. Post-admission events may still overlap later ready/history snapshots, and presence counts remain event-time snapshots. No durable per-client cursor, end-of-catch-up marker, or exactly-once delivery is claimed. Optional telemetry/caching remains P2 behind operator demand. SQLite is still the supported single-instance product; PostgreSQL stays an unreleased preview. No schema (PostgreSQL 8 / SQLite 5), dependency, license, production infrastructure, publication or paid-resource change.

### PR #31 evidence (historical)

Latest verified baseline: clean `main...origin/main` at `d553eb9` (merged PR #30), no open PRs, and all ten [post-merge CI jobs](https://github.com/Deathcharge/samsarix-chat-engine/actions/runs/33389393712) passing: **303 tests, two SQLite-inapplicable skips, 89.06% branch-inclusive coverage**. The previous goal turn made verified implementation progress. The current P1 is the ready/history activation gap seen in that turn's crash-test setup.

This increment starts with **seven failing regressions**: the manager discarded a pending presence event, and real SQLite/fake-PostgreSQL handlers dropped create/edit/delete broadcasts injected after a captured history snapshot, before ready, or just before activation. Registered pending sockets now retain immutable serialized snapshots, drain them after ready/history, and atomically enter live delivery only once the queue is empty. Arrivals during the drain join its tail; direct initial frames and the drain share each socket's operation lock. The queue is bounded at 64 events / 256 KiB per socket and 8 MiB aggregate by default, including in-flight activation payloads after detachment. The whole flush has one send-timeout deadline; overflow/failure/timeout attempts close 1013, and cancellation attempts 1012. Every detach path discards queued data. No payloads or private error details are logged.

Local verification: `python -m ruff check .`, `python -m ruff format --check .`, `python -m mypy samsarix_chat_engine`, and `git diff --check` pass. The final local `python -m pytest -q -m "not postgres" --timeout=60 --tb=short` passes **268 tests, two inapplicable skips, 75 deselected**. `python -m pytest -q tests/test_handshake_integration.py tests/test_handshake_buffer.py -m "not postgres" --timeout=30 --tb=short` passes **33 tests**, with one live PostgreSQL case deselected. The integration journey pauses actual history, commits HTTP create/edit/delete operations, then verifies history plus buffered mutations equals current durable rows, including tombstones. The shared PostgreSQL version must pass CI; no local PostgreSQL or Docker executable is available. Final-head CI, package evidence, and merge remain pending verification gates.

Review cases cover immutable snapshots, origin/room exclusion, arrivals during flush, event/UTF-8-byte/global overflow, budget reuse across rooms, all detachment paths, retained in-flight accounting, malformed embedded events, errors/cancellation/deadlines, a continuously replenished queue, and a repeatedly cancelled overflow producer. An additional failing deep-JSON regression exposed an unhandled encoder `RecursionError`; this now follows the same bounded close path. The focused buffer/storage integration command now passes **33 tests**, with one live PostgreSQL case deselected. Payload budgets are not process-RSS or sustainable-capacity claims. Initial history and delayed relay events can overlap; clients merge by ID and apply edits/tombstones rather than appending duplicates. This closes the local post-registration drop window, not rapid archive/reopen ordering, durable per-client replay, or an end-of-catch-up contract.

Initial [PR #31 CI](https://github.com/Deathcharge/samsarix-chat-engine/actions/runs/33390690812) at `2e7913a` passed the full quality job (**342 tests, two skips, 89.21% coverage**) and eight other jobs, including installed-wheel smoke. The dedicated PostgreSQL run exposed an old test assumption: Bob can now receive Alice's historical join (count 1) before his message because initialization buffers rather than drops it. That assertion now recognizes exactly that optional event before retaining the message comparison. This is not a fresh-count guarantee: presence counts are event-time snapshots, possibly older than `ready`. Obsolete relay snapshots are an explicit follow-up gate, not silently treated as current state. Final-head checks remain required after these refinements.

The follow-up Linux Python 3.14 run accepted deeper JSON than Windows, exposing a platform-dependent test assumption rather than a buffer failure. After reproducing the uncaught encoder exception on Windows, its regression now injects `RecursionError` directly instead of requiring a fixed nesting depth to exhaust every platform's C stack. The runtime retains the error handling; valid serializable nested events need not be rejected solely for their depth.

Remaining ordered P1 work: rapid lifecycle changes and obsolete presence/lifecycle snapshots from delayed relays; live-lag fencing; real-process update/delete, quota contention and webhook-worker crash recovery; kernel packet-loss/failover and measured load/reconnect storms; deployment manifests and PostgreSQL-native backup/PITR/rollback. The supported product remains single-instance SQLite and PostgreSQL remains an unreleased preview. Optional telemetry/caching remains P2 behind operator demand. This slice changes no schema (PostgreSQL 8 / SQLite 5), dependency, license, production infrastructure, package publication, or paid resource.

### PR #30 evidence (historical)

Latest verified baseline: clean `main...origin/main` at `c6b810a` (merged PR #29), no open PRs, and all ten post-merge CI jobs passing: **282 tests, two SQLite-inapplicable skips, 88.87% branch-inclusive coverage**. The previous goal turn made verified implementation progress; this turn addresses the next P1, archive versus admission/lease-renewal ordering.

PR #30 starts with **four failing lifecycle cases and two passing storage/expiry controls** on that baseline. A room archived or deleted between validation and admission escaped the normal domain-error contract; a heartbeat observing an archived room before the relay incorrectly reported a storage outage. Admission and failed renewal now diagnose authoritative room state and produce archive (4409) or missing-room (4404) outcomes. Renewal also recognizes archived/deleted rooms after maintenance has reaped the reservation. Healthy renewal remains one query; active-room invalid/expired leases and unavailable storage retain the fail-closed 1012 behavior. An individual manager close can own its final event atomically, so heartbeat/room/member closers cannot duplicate that event or physical close.

The new live PostgreSQL tests deliberately change room state during admission and pause archive relay dispatch until heartbeat teardown wins, including reaped leases and room deletion. An unrelated room remains connected. These barriers exercise real PostgreSQL and ASGI handlers in one application process; they are not separate network-process fault tests. Existing separate-process normal-path moderation acceptance remains complementary evidence, not proof of all interleavings.

Local verification: `python -m ruff check .`, `python -m ruff format --check .`, `python -m mypy samsarix_chat_engine`, and `git diff --check` pass. `python -m pytest -q -m "not postgres" --timeout=60 --tb=short` passes **229 tests, two inapplicable skips, 74 deselected**. Initial [PR #30 CI](https://github.com/Deathcharge/samsarix-chat-engine/actions/runs/33388072769) passed eight jobs but both database jobs exposed one invalid expired-lease fixture (SQLSTATE 23514): its expiry preceded its creation time. The fixture now moves both timestamps into the past while preserving the database constraint. The initial full run had **302 passes and two skips**, with that one failure; final-head CI is still a merge gate. No local PostgreSQL or Docker executable is available, so live verification runs in CI. Final CI and built-artifact evidence will be recorded on PR #30.

The corrected fixture passes all **74 PostgreSQL tests** in [run 33388584287](https://github.com/Deathcharge/samsarix-chat-engine/actions/runs/33388584287), but its full quality job exposed an existing crash-test setup race: Bob could join before Alice was broadcast-active, losing the best-effort join event the test awaited. The crash/reap test now completes Alice's ready/history plus application ping/pong before connecting Bob. All join, message, crash-departure, count, and restart-history assertions remain; this is an explicit setup precondition, not a fix for activation-window delivery. The local handler/manager suite passes **59 tests, two inapplicable skips**. Final-head CI remains required; exact passing results and package digests are recorded on the PR before merge.

Remaining P1 work: ready/history activation-window convergence (now also observed in full CI); rapid archive/reopen with delayed events; live-lag fencing; real-process quota contention and webhook-worker crash recovery; kernel packet-loss/failover and measured load/reconnect storms; deployment manifests and PostgreSQL-native backup/PITR/rollback. The supported product remains single-instance SQLite, and PostgreSQL is an unreleased preview. P2 telemetry/caching stays behind demonstrated operator demand. This slice changes no schema (PostgreSQL 8 / SQLite 5), dependency, license, production infrastructure, package publication, or paid resource.

### PR #29 evidence (historical)

Latest verified baseline: clean `main...origin/main` at `42caa17` (merged PR #28), no open PRs, and all ten post-merge CI jobs passing (241 tests, 88.55% branch-inclusive coverage). The previous goal turn made verified implementation progress; the next P1 was authenticated-handshake resource ownership.

PR #29 starts from **22 failing handler cases** on that baseline (two PostgreSQL-only count cases are inapplicable to SQLite). The old handler registered sockets before its cleanup block and could strand local membership or database reservations when room/moderation rechecks, history, counts, or initial sends failed or were cancelled. Initial storage failures also bypassed the WebSocket error contract. The request now owns cleanup across the whole authenticated session, awaits background task termination and closure/release, preserves direct cancellation, and shields finalization from ASGI cancel scopes and repeated task cancellation. Raw pre-registration close is kept separate from manager-owned closure so a detached socket cannot be revived through a fallback send. Unexpected errors close 1011; translated storage errors close 1012 without database details.

Admission now attempts idempotent release when its commit result is unavailable or cancelled, but preserves existing ownership for known duplicate-ID rejection. A review regression first showed that indiscriminate error cleanup could release an existing duplicate owner; the narrowed error handling and dedicated test fix that. A second regression preserved SQLite `presence.left` after receive-loop storage failure by leaving physical closure and departure metadata with the finalizer. AnyIO is declared directly for cancellation scopes; it was already transitive through Starlette. No schema change (PostgreSQL 8 / SQLite 5), hosted service, production deployment, publication, or paid resource is involved.

Initial [PR #29 CI](https://github.com/Deathcharge/samsarix-chat-engine/actions/runs/33385884619) at `c30fd93` passed all ten jobs: **277 tests, two inapplicable skips, 88.82% coverage**, including **63 PostgreSQL tests**. The live acceptance checks physical lease rows after injected post-reservation failures, then reconnects; expired rows cannot hide behind active-count filtering. Additional pre-admission cancellation and departure-preservation refinements pass locally. A further red/green regression ensures cancelling an individual closer after detachment cannot abandon the physical close, including when request cleanup cancels a closing heartbeat. `python -m pytest -q tests/test_connection_manager.py tests/test_websocket_lifetime.py tests/test_websocket.py tests/test_postgres_runtime.py --timeout=30 --tb=short` passes **69 tests, two inapplicable skips**. Ruff lint, formatting, mypy and diff checks pass. Final-head full CI remains a merge gate. No local PostgreSQL or Docker executable is available; their verification runs in CI.

Final local verification: `python -m ruff check .`, `python -m ruff format --check .`, `python -m mypy samsarix_chat_engine`, and `git diff --check` pass. `python -m pytest -q -m "not postgres" --timeout=60 --tb=short` passes **219 tests**, with two SQLite-inapplicable count cases skipped and 63 live PostgreSQL tests deselected. CI and package artifact evidence for the final head are recorded on PR #29 before merge.

PR #29 left archive versus lease-renewal ordering for PR #30, alongside the remaining P1 gates above. Cancellation shielding cannot survive process kill or guarantee immediate release during a database outage; lease expiry remains the backstop.

### PR #28 evidence (historical)

PR #28 started from `eead4bc` (merged PR #27), whose main CI passed 233 tests with 88.45% coverage. PR #28 subsequently merged as `42caa17` and passed all ten final-head and post-merge jobs; its verification and artifact digests are recorded on the PR.

PR #28 (`92bc2c8` implementation) completes the normal-path real-process moderation acceptance journey. Two independent Uvicorn processes use signed room-scoped members, not operator credentials for member requests. Freeze/unfreeze and mute/unmute enforce writes, ban evicts the same subject on both replicas and denies reconnect until unban, and archive closes both replicas before reopen restores durable history. Unaffected peers and the same subject in an unrelated room remain connected and writable. No hosted application, login database, or metered provider was added: this strengthens the embeddable private/support-room backend's existing core journey.

The accompanying manager regression tests first produced **6 failures / 8 passes** on the baseline: detached sockets accepted sends through a fresh lock, stale broadcast snapshots could send after detachment, and duplicate registration replaced the original lock/room ownership. A separate strengthened assertion failed because send failure forgot the socket without attempting physical close. The fix detaches before close, requires the original live registration for sends, rejects duplicate active registration, and attempts a bounded code-1013 close after failed sends. An already-started send can finish before close; delivery is not upgraded to a durable guarantee. The embedding contract change is explicit in README and CHANGELOG.

Local final implementation verification: `python -m ruff check .`, `python -m ruff format --check .`, `python -m mypy samsarix_chat_engine`, and `git diff --check` pass. `python -m pytest -q tests/test_connection_manager.py tests/test_conversation_controls.py tests/test_postgres_runtime.py --timeout=30` passes **32 tests**; `python -m pytest -q -m "not postgres" --timeout=60` passes **180 tests**, with 61 deselected. [PR #28 CI run 33383479399](https://github.com/Deathcharge/samsarix-chat-engine/actions/runs/33383479399) passes all ten jobs at `92bc2c8`: **241 tests, 88.55% branch-inclusive coverage**, including **61 PostgreSQL tests** in the dedicated Python 3.14/PostgreSQL 18.4 job. Python 3.10–3.14 Linux, Windows 3.12, package build/metadata/installed-wheel smoke, dependency audit, TypeScript-client checks, and container smoke all pass. The documentation follow-up must pass its own CI before merge. No local PostgreSQL or Docker executable is available, so live tests run in CI. No schema change (PostgreSQL 8 / SQLite 5), new dependency, production deployment, publication, or paid resource is involved.

PR #28's adversarial review queued handshake cleanup (addressed in PR #29) and archive versus lease-renewal ordering (addressed in PR #30). Its normal-path moderation tests alone did not prove either race fixed. See the [preview guide](POSTGRES_PREVIEW.md) and [acceptance checklist](MULTI_INSTANCE_ARCHITECTURE.md#release-acceptance-gates).

### Prior increment evidence

Merged PR #27 adds a separate checked-out PostgreSQL operation deadline, finite replacement-connect timeout, and per-session statement/idle-transaction limits. Deadline expiry closes the stalled session before interrupting its waiter, so Psycopg cancellation cannot wait indefinitely on the same transport. The fault proxy verifies silent bidirectional application-traffic stalls without cutting TCP, alongside reset/refusal, healthy-peer progress, readiness fencing, explicit reconnect/history recovery, and resumed delivery. A live query test proves a timed-out session is discarded and the single-connection pool recovers. The Python/OS matrix installs the PostgreSQL extra so database-independent deadline tests run throughout it; Python 3.10 explicitly skips overlapping-cancellation discrimination because it lacks cancellation-count APIs. Final CI passed 233 tests with 88.45% coverage. Timeouts do not prove rollback after a lost commit reply; [preview operations](POSTGRES_PREVIEW.md#operation-deadlines-and-ambiguous-outcomes) documents idempotent retry and reconciliation.

The supported v0.12 product remains the single-process SQLite backend described below. The guarded PostgreSQL preview now wires authoritative storage, leased coordination, rate controls, presence/typing, and the durable relay through the application. Merged PRs #23 (`9960ec9`) and #24 (`4c15537`) established the application runtime and real Uvicorn crash/restart test. The older baseline and release results below are historical evidence, not a claim that v0.13 has completed its release gates.

Merged PR #25 (`c8d77e7`) tests a database connection reset/refusal for one live replica while another remains available. It adds a local TCP fault proxy (test-only; no infrastructure mutation), explicit reconnect/history recovery checks, relay-claim readiness/admission checks, and best-effort lease cleanup that cannot skip pool closure. The reference test uses a three-second lease and a one-second pool timeout only for the interrupted replica; silently blackholed connections, database failover, live-lag fencing, load/soak, cross-process moderation/quotas/webhooks, deployment manifests, and verified PostgreSQL-native backup/PITR/rollback remain local engineering gates. See [preview operations](POSTGRES_PREVIEW.md) and the authoritative [release checklist](MULTI_INSTANCE_ARCHITECTURE.md#release-acceptance-gates).

2026-08-31 baseline: clean `main...origin/main` at `4c15537`, no open PRs, and no local PostgreSQL or Docker executable. Local verification of the network-recovery implementation: `ruff check .`, `ruff format`, `mypy samsarix_chat_engine`, and `git diff --check` pass; `pytest -q -m "not postgres" --timeout=60` passes **148 tests**, with 56 deselected. The three deterministic runtime failure tests pass without a database. Both process tests skip locally because `SAMSARIX_TEST_POSTGRES_URL` is unset; PR #25's dedicated PostgreSQL and full quality jobs are the live verification gates. This slice changes no database schema (PostgreSQL 8 / SQLite 5), no production infrastructure, and no paid-service dependency.

PR #25 final verification: all ten CI jobs passed at `318219c`, including live PostgreSQL and the full quality job (204 tests, 88.23% branch-inclusive coverage). A Python 3.14 matrix run exposed an existing test teardown race: leaving the Starlette socket context cancels its ASGI task before the orderly `presence.left` broadcast necessarily completes. The two-client test now explicitly disconnects Bob and observes his departure before context teardown, retaining the event assertions. The six WebSocket tests and 20 fresh-process repetitions of that disconnect scenario pass; the final Linux 3.10–3.14 and Windows 3.12 jobs also passed.

Late review of PR #25 identified an admission/fencing race: a database reservation could finish after local sockets were fenced, and then register a new socket during recovery. The follow-up binds admission to both the database generation and a monotonic local admission epoch, validates that token under the connection-manager lock shared with fencing, and releases reservations rejected by the guard or local capacity. Deterministic tests cover a fence during reservation, same-generation recovery before reservation completes, registration waiting on the shared lock, fencing after successful admission, capacity rejection, and a mismatched database generation. The test harness also restores normal pool timeouts outside the interrupted replica, drains child logs concurrently into a bounded tail, and verifies a child can emit one million bytes without blocking. These changes add no database schema, runtime service, or paid dependency; their final-head CI is the follow-up merge gate.

Follow-up local verification: `python -m ruff check .`, `python -m ruff format --check .`, `python -m mypy samsarix_chat_engine`, and `git diff --check` pass. `python -m pytest -q -m "not postgres" --timeout=60` passes **155 tests**, with 57 deselected. The child-output test passes when invoked explicitly without a database. An isolated in-memory mutation that disables the registration guard causes the lock-interleaving regression test to fail as expected; no source files were changed by that probe. Live PostgreSQL acceptance still requires CI because no local database is available.

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
- [x] Reject sends after socket detachment and prove normal-path signed-member moderation/archive teardown across real PostgreSQL processes.
- [x] Cover authenticated-handshake failure/cancellation cleanup, repeated cancellation, ambiguous reservation release, and physical live-database lease cleanup/reconnect.
- [x] Diagnose archived/deleted rooms during admission and heartbeat renewal, including reaped leases and a delayed archive relay; prevent duplicate final notifications across competing closers.
- [x] Replace dropped post-registration initialization broadcasts with bounded ready/history-to-live queues, fail-closed overflow/deadlines, and storage/handler convergence tests.
- [x] Fence pre-admission relay events with a committed join sequence and exercise delayed lifecycle/presence ordering across real ASGI replicas.
- [ ] Cover combined lifecycle changes, OS-process outages, live lag and reconnect storms; controlled relay barriers alone do not prove these combinations.
- [ ] Complete the PostgreSQL release gates before multiple workers or hosts are supported; shared storage and durable relay/presence are implemented, while fault/lag/contended-quota/webhook-crash acceptance remains.
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
