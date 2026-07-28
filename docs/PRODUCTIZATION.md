# Productization record

Last updated: 2026-07-28

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

Samsarix Chat Engine is a local-first, single-instance room chat service and embeddable FastAPI application. It gives Python developers a narrow, inspectable way to add durable text-room chat to a prototype, internal tool, local collaboration utility, or reference implementation.

- Target user: a Python developer who needs persisted HTTP/WebSocket chat without adopting a full collaboration platform or operating Redis/Postgres.
- Primary use case: create a room, connect one or more clients, commit and broadcast messages, then reconnect and recover history.
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
- Use a shared API key only as an optional deployment boundary. User accounts and room authorization remain out of scope rather than being superficially implemented.

- Make Samsarix LLC the canonical owner identity in v0.3 while retaining the v0.2 import, command, environment, and database names as tested migration aliases.
- Use the unmodified MPL-2.0: file-level copyleft protects distributed changes to covered files and notice preservation while allowing the engine to be combined with separate proprietary files. AGPL-3.0 would cover network use more strongly but materially narrows embedding adoption; Apache-2.0 would permit closed downstream modifications.

Primary references checked on 2026-07-28: [FastAPI's official WebSocket documentation](https://fastapi.tiangolo.com/advanced/websockets/), the [Python Packaging User Guide](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/), [Starlette's TestClient documentation](https://www.starlette.io/testclient/), FastAPI/Pydantic/HTTPX2 PyPI metadata, [Centrifugo's official channel documentation](https://centrifugal.dev/docs/server/channels), [Mozilla's MPL-2.0 FAQ](https://www.mozilla.org/MPL/2.0/FAQ/), the [official MPL-2.0 text](https://www.mozilla.org/MPL/2.0/), and the [GNU license overview](https://www.gnu.org/licenses/).

## Architecture and trust boundaries

Untrusted HTTP and WebSocket payloads enter FastAPI/Pydantic validation. A configured shared key gates all `/v1` HTTP data and the WebSocket protocol. The service commits validated messages to a local SQLite path and then sends them to the in-process room registry. It makes no outbound requests.

Deployment owners control TLS/proxying, the API secret, allowed browser origins, filesystem permissions, backups, deletion obligations, and access to the SQLite file. Any client holding the shared key can read and write every room. Display names are claims, not authenticated identities. Multi-process deployment breaks real-time fan-out and per-process rate-limit accounting, so it is unsupported.

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
- [ ] Add real user identity and server-side per-room authorization before use with mutually untrusted users.
- [ ] Add room/message export and deletion administration for deployments with data-subject or retention obligations.
- [ ] Add a broker/presence adapter and cross-instance integration tests before multiple workers or hosts are supported.
- [ ] Run sustained concurrent load/soak tests and publish measured limits before capacity claims.

### P2

- [ ] Add optional room metadata updates and explicit archival.
- [ ] Add a small framework-neutral TypeScript protocol client.
- [ ] Add time-based retention in addition to count-based caps.
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

Per-user authorization, multi-instance fan-out, administrative deletion/export, and load testing are genuine next-stage local engineering, ordered above. They are not needed to evaluate the single-instance developer service but are gates for broader or regulated deployments.

Public package publication, hosted deployment, domains, credentials, signing, and pricing remain owner-controlled. No external accounts, infrastructure, releases, or spending were created as part of the local productization work.

## Release disposition

**Alpha release candidate.** The single-instance developer product has no known locally actionable P0, and its source, package, primary journey, limits, error states, documentation, brand identity, and standard open-source license are coherent. Deployments involving mutually untrusted users still require the named P1 per-user/per-room authorization work.

## Known risks

- A shared API key is coarse-grained and rotation disconnects/rejects all clients.
- SQLite and the in-process connection registry intentionally limit scale and topology.
- Count-based deletion has no audit log and is unsuitable where legal holds are required.
- Message content is plaintext at rest unless the deployment encrypts the filesystem.
- Presence is at-most-once and may be stale briefly around abrupt disconnects.
- In-memory rate limits reset on restart and are not coordinated across processes.
