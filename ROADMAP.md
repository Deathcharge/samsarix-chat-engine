# Samsarix Chat Engine roadmap

This roadmap turns the engine into a credible embedded-chat backend without pretending to be a complete collaboration suite. Merge, release, package publication, hosted operation, and flagship adoption are separate gates.

## Product boundary

Samsarix Chat Engine is a self-hosted, local-first backend for applications that need private text rooms, durable history, and live delivery without operating a full chat platform. Its most plausible early uses are:

- an authenticated support or customer-success room embedded in a product;
- private cohort, classroom, game-guild, or community rooms whose membership is decided by a host application;
- incident, field-team, or internal operations chat for a small single-instance deployment;
- a transparent FastAPI/SQLite reference backend for teams prototyping their own chat UI.

Its wedge is operational simplicity and inspectable source, not feature parity with Sendbird, Ably, Mattermost, or Slack. The host application owns login and room membership; this engine enforces the short-lived authorization result, commits messages, and delivers room events.

## v0.4 — tenant-safe access boundary

- [x] Signed, expiring application-user access tokens with fixed HS256 verification rules.
- [x] Server-enforced token subject, room IDs, and read/write/admin permissions.
- [x] HTTP and browser WebSocket token flows without URL credentials.
- [x] Operator API-key compatibility and safe unauthenticated loopback compatibility.
- [x] CLI and Python token issuance, migration guidance, strict claim validation, and adversarial tests.
- [x] Exact browser-origin allowlisting for non-local origins, including authenticated deployments.

Acceptance gate: full lint, format, type, test/coverage, dependency audit, package build, installed-wheel smoke, and GitHub matrix checks on the exact merge head.

## v0.5 — data lifecycle and accountable administration

Highest-value next milestone for support, education, and internal-tool deployments:

- [x] room export in streaming NDJSON with stable schema/version metadata;
- [x] explicit room archive and irreversible-delete workflows protected by operator/admin access;
- [x] lifecycle events for connected clients and deterministic WebSocket teardown;
- [x] time-based retention in addition to current count caps;
- [x] bounded administrative audit records without message-body duplication;
- [x] backup, restore, export, and deletion runbooks with integration tests.

This closes the largest remaining privacy and operational gap. It does not claim regulatory compliance; deployment owners still determine policy and legal obligations.

## v0.6 — conversation controls for real communities

- [x] message edit and tombstone deletion with author/operator authorization and persisted event semantics;
- [x] room freeze plus stable-subject mute/ban primitives for moderation;
- [x] immediate live-socket eviction for bans and reconnect convergence for edits/deletes;
- [x] metadata-only moderation audit, schema migration, API/runbook documentation, and integration tests.

Attachments, reactions, and mentions should be added only against a named consumer journey. Search now serves support-case retrieval, and one-depth threads serve contextual support, classroom, and incident follow-up. Binary files should use operator-provided object storage rather than SQLite blobs.

## v0.7 — integration ergonomics

- [x] ship a small framework-neutral TypeScript protocol client with reconnect and event-state helpers;
- [x] bundle declarations, explicit ESM exports, fake-transport tests, package verification, and a dedicated Node CI gate;

## v0.8 — application workflows

- [x] add opt-in read cursors/unread counts and ephemeral typing indicators with explicit privacy/cost limits;
- [x] publish a complete reference integration against an authenticated support-room journey.

## v0.9 — reliable application delivery

- [x] add signed webhooks for committed message and moderation events with timeouts, bounded retries, replay protection, and idempotency;
- [x] expose delivery health and terminal-failure recovery without weakening local message commits;
- [x] document receiver verification, secret rotation, network/privacy boundaries, and failure-mode runbooks.

## v0.10 — support-room retrieval

- [x] add authorized, Unicode-normalized current-message search within one room;
- [x] preserve stable chronological pagination while excluding deleted content and reflecting edits;
- [x] bound scan cost by retained room history and add an independent per-principal search limit;
- [x] expose the workflow through the TypeScript client and document its privacy/performance boundary.

This is deliberately per-room substring retrieval rather than global full-text ranking. It serves the named support-case journey without adding an external index, a migration, or a misleading scale claim.

## v0.11 — hardened single-instance deployment

- [x] add a multi-stage, non-root container image with exec-form shutdown and storage readiness health checks;
- [x] provide a single-replica Compose profile with a read-only root filesystem, dropped capabilities, bounded temporary state, persistent SQLite volume, and loopback-only port mapping;
- [x] support mounted one-line secret files for operator, token, and webhook secrets without exposing their contents;
- [x] add CI image build/security/persistence smoke and backup/upgrade/rollback guidance.

This makes the supported topology repeatable without implying that a container makes an in-process connection registry horizontally scalable.

## v0.13 — measured multi-instance operation

- [x] define a storage-neutral application boundary without changing the single-instance default;
- [x] implement the internal PostgreSQL authoritative store, ordered event log, read state, stable exports, retention, and leased webhook outbox;
- [x] implement a cursor-backed per-process realtime relay with ordered replay and lease-loss socket fencing;
- [x] implement PostgreSQL-owned expiring connection leases with atomic global/per-room caps and crash reclamation;
- [x] implement bounded PostgreSQL-owned message, search, and typing rate buckets with database-time windows;
- [x] implement connection-bound PostgreSQL typing transitions, refresh, and bounded expiry sweeping;
- [x] derive join/leave presence from connection leases with generation-fenced restart and crash convergence;
- [x] bound the retained event log behind live cursors and fence/recover stale workers that return after a gap;
- [x] expose explicitly guarded PostgreSQL preview configuration without changing the SQLite default;
- [x] wire cross-instance fan-out, presence, typing, rate controls, connection leases, maintenance, and readiness into the application;
- [x] prove two real Uvicorn processes share HTTP/WebSocket state, reclaim a killed replica's socket lease, restart its stable identity, and recover durable history;
- [x] prove a database TCP reset/refusal fences an isolated replica, preserves healthy-peer progress, and recovers history/fan-out on explicit client reconnect without application restart;
- [x] bound checked-out PostgreSQL operations and prove recovery from a silent bidirectional traffic stall over open TCP connections;
- [x] prove signed-member freeze/mute/ban controls, cross-process archive/ban teardown, reconnect denial/recovery, and unrelated-room isolation against real network processes;
- [x] finalize interrupted authenticated handshakes, shield cleanup from repeated cancellation, and verify physical PostgreSQL lease release/reconnect after initialization failure;
- [x] distinguish archive/deletion from storage failures during admission and renewal, including reaped reservations and a deliberately delayed archive relay;
- [x] replace dropped post-registration initialization broadcasts with bounded ready/history-to-live buffering and fail-closed overflow/deadline handling;
- [x] fence pre-admission relay events using the committed join sequence, with delayed archive/reopen, room recreation, ban/unban, freeze/unfreeze and presence acceptance;
- [x] make current-schema startup inspection-only, retain serialized transactional upgrades, enforce exact readiness compatibility and close cancelled startup pools; live lock/rollback/replica tests gate acceptance;
- [x] implement configurable pre-batch count/age lag fencing, retry-safe generation/cursor recovery, and controlled signed-member two-application history/fan-out acceptance;
- [x] bound SDK connection attempts, wait for initial history/activation before publishing, retain retry budgets across flapping connections, and verify native-WebSocket reconnect/history/resumed delivery;
- [x] prove contending real-process idempotent creates, ordered edits/deletes, author enforcement and recovered history, plus shared socket/room caps and HTTP/search/WebSocket/typing budgets;
- [x] prove a killed webhook worker's live claim is recovered by a separate surviving process after natural database lease expiry, retaining signed ID/body, while deletion-before-reclaim cancels pending payloads;
- [x] enforce a total webhook network-attempt deadline, interrupt owned TCP/TLS sockets on stop/cancellation, and bound unresolved native DNS to one daemon job without blocking process exit; storage and whole-process shutdown retain separate budgets;
- [x] verify kernel-paused process count/age lag and natural expiry/pruned-gap recovery through lifecycle changes, tombstoned history, bounded signed-member reconnect and healthy-peer continuity;
- [x] prove measured live-lag and combined lifecycle/outage/reconnect-storm behavior before stronger reconnect-delivery claims;
- [ ] prove kernel-level packet blackholes and database failover against real network processes;
- [ ] run sustained controlled-host load/soak tests and publish owner-environment capacity limits; the checked reconnect-storm profile remains a bounded three-minute shared-runner measurement;
- [x] verify a hardened Kubernetes preview derives each replica's stable instance ID from its StatefulSet Pod name and rejects duplicate live ownership;
- [x] execute the checked two-replica manifest in pinned disposable kind, with TLS-verified PostgreSQL, cross-replica HTTP/WebSocket delivery, exact live identities, and same-version Pod replacement;
- [x] validate separate-process live-lag and retained-gap recovery under measured traffic;
- [x] exercise and publish a PostgreSQL-native logical dump into a fresh database, application-level restore verification, post-restore writes, and rollback runbook;
- [x] exercise a verified physical base backup, archived-WAL replay to a named point, recovered-timeline application checks, post-recovery writes, and application-role fencing on a disposable CI cluster;
- [ ] prove external old-primary process/network fencing, routing cutover, database failover and failback on controlled infrastructure;
- [ ] add OpenTelemetry hooks only when an operator needs them, with telemetry disabled by default.

No horizontal-scale claim is acceptable before those tests pass. Redis Pub/Sub is at-most-once and does not solve shared storage, migration/restore coordination, webhook leadership, or distributed quotas. A broker and shared authoritative database must solve a demonstrated topology together rather than decorate the architecture.

The relay currently polls; transactional `NOTIFY` emission is not consumed by a listener. Notification-assisted latency is an optional optimization, not a release prerequisite. If introduced, it must pass listener-loss/reconnect tests without weakening the polling correctness path.

## v0.14 — contextual threaded replies

- [x] add an optional parent message ID to HTTP and WebSocket message creation;
- [x] keep the contract deliberately one level deep and reject replies to replies;
- [x] add authorized, chronological reply pagination with thread-scoped cursors;
- [x] preserve idempotency, edit/delete, search, export, webhook, and realtime semantics for replies;
- [x] migrate SQLite and PostgreSQL safely, including retention behavior for surviving replies;
- [x] expose `listReplies()` and `sendReply()` through unpublished TypeScript SDK 0.5.0.

This milestone serves named support, classroom, and incident journeys without adding arbitrary nesting or a separate conversation store. Room history remains a chronological flat stream so existing clients continue to receive every message; clients can use `parent_message_id` to render thread context.

## Deliberate non-goals

- no built-in password database, social graph, billing, or end-user frontend;
- no AI agents, content generation, or metered model dependency;
- no end-to-end encryption claim without a separately reviewed key-management protocol;
- no federation or Slack-compatible protocol emulation;
- no production hosting, package publication, domain, pricing, or support-SLA commitment without owner approval.

## Evidence required for every milestone

A milestone is complete only when its exact commit, verification commands/results, artifact digest, migration impact, rollback path, and remaining risks are recorded in its pull request or release notes. README claims must not exceed that evidence.
