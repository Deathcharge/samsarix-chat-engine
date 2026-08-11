# Multi-instance architecture

Status: **accepted design; implementation in progress for v0.13**.

This document is a contract, not a scale claim. Version 0.12 still supports exactly one Samsarix process and one replica. Multi-instance support becomes releasable only after every acceptance gate below passes against multiple real service processes.

## Decision

The supported multi-instance topology will use PostgreSQL as both the authoritative chat database and the durable coordination plane. Psycopg 3's async connection pool will be an optional installation extra. SQLite remains the zero-service default for single-instance deployments.

PostgreSQL `LISTEN`/`NOTIFY` will be a low-latency wake-up hint only. Every client-visible realtime event will first be inserted into a bounded, ordered database event log in the same transaction as the state change. Each service instance reads committed rows from its own durable cursor. A listener reconnect, notification-queue loss, or process pause therefore causes polling/replay rather than silent event loss.

The relay implements that polling/replay correctness path and is now wired into the guarded PostgreSQL application runtime. Each process exclusively claims an expiring generation-owned cursor, dispatches an ordered batch to its local socket manager, and checkpoints the last successfully dispatched event at the batch or failure boundary. A duplicate live instance ID fails closed; after replacement, a stale generation cannot heartbeat, read, or acknowledge. A dispatch exception keeps the relay alive and retries from the failed event without replaying acknowledged predecessors. A database acknowledgement failure can make the relay-to-process handoff at least once across a crash boundary; the process fences its old local sockets before replay, preserving the existing at-most-once boundary for any individual socket connection. An expired lease or database interruption likewise fences all local sockets before the process renews the same cursor. Schema v8 records a durable pruning watermark: a returning cursor behind that watermark closes every local socket, rotates its process generation so old connection leases remain stale, and advances to the current authoritative head before serving later events. Public message, room-state, presence, and typing envelopes are broadcast; sender exclusion strips the internal origin connection ID before delivery. Archive and active-ban events invoke deterministic local teardown, while other internal lifecycle records are not leaked onto the public protocol. Real subprocess recovery and notification-assisted latency remain release gates.

The initial event-log implementation serializes sequence allocation with a transaction-scoped advisory lock. PostgreSQL identity values alone are not commit ordered: without this lock, a later sequence could commit and be acknowledged before an earlier transaction becomes visible. Event append must remain the final lock-taking phase of a domain mutation. Sustained-load acceptance tests will determine whether this intentionally simple global sequencer is sufficient or must be partitioned without weakening cursor correctness.

Schema v3 uses PostgreSQL database time for room/message/moderation ordering, read cursors, webhook due times and leases, and retention boundaries. The internal store now implements the full storage protocol, including monotonic subject-scoped read state, transactionally stable bounded-memory exports, explicit retention, and a leased transactional webhook outbox. Transaction-scoped capacity locks currently serialize room creation, message mutation/retention, bounded audit insertion, and webhook capacity changes across replicas. Deletion and retention cancel unsent sensitive webhook payloads and scrub message bodies from older durable event and terminal-webhook envelopes before commit. These conservative global locks make correctness inspectable first; the load gate must measure their throughput before v0.13 receives a scale claim.

Schema v4 adds one database-owned lease per admitted WebSocket. A transaction locks the live owning instance and usable room, serializes the capacity decision, removes stale reservations, and atomically checks deployment-wide and per-room caps before inserting. Renewal and release require the owning instance ID. Counts exclude expired socket leases, expired owners, and archived rooms; cleanup returns those stale rows for presence convergence. Schema v7 supersedes the original stable-ID restart behavior with generation fencing so expired rows remain available for leave-event recovery without becoming live again. All replicas must use identical capacity settings; public application admission and presence wiring remain release gates.

Schema v5 adds fixed-window counters for message, search, and typing controls. PostgreSQL time chooses each boundary, and an atomic row update admits no more than the configured count for a scope/key across all replicas. Existing identities contend only on their own row; creation of new identity buckets uses a separate advisory lock to prune expiry and enforce a hard cardinality bound. Raw subjects and client addresses are not stored: a scope-separated SHA-256 digest is persisted instead. That digest is data minimization, not anonymization, because predictable identities may still be guessed. All replicas must use identical limits, window lengths, and bucket capacity. Fixed windows can admit traffic on both sides of a boundary; load tests must validate whether that declared behavior is sufficient before public wiring.

Schema v6 binds ephemeral typing rows to live connection leases. An inactive start writes one durable `typing.started` coordination event; repeated starts refresh the database-time deadline without producing an event storm. Explicit stop and a bounded `SKIP LOCKED` expiry sweep delete state before writing `typing.stopped` in the same transaction. Events retain an opaque origin connection ID so later relay wiring can exclude the sender, but the public relay intentionally ignores typing events until that exclusion is implemented. Connection deletion cascades typing state; a missed stop after a crash remains safe because clients already expire typing locally from `expires_in`. Typing rows and events are operational coordination, never chat history or audit records.

Schema v7 derives presence transitions from committed connection changes. Admission serializes capacity, stale cleanup, insertion, the resulting live room count, and `presence.joined`; explicit owned release serializes deletion, optional `typing.stopped`, the decremented count, and `presence.left`. A bounded sweeper uses `SKIP LOCKED` to reclaim crashed sockets and emits typing stops before leaves. Each process registration has a UUID generation: renewing a live process preserves it, while re-registering an expired stable ID rotates it. Every connection stores the admitting generation, and counts, renewal, typing, and stale detection require a match. Old sockets therefore remain non-live and sweepable instead of being silently revived or deleted before convergence. Presence is still best effort and non-authoritative; durable coordination makes cross-process delivery recoverable but reconnecting clients must accept a fresh room count.

Schema v8 adds singleton event-retention metadata. A bounded maintenance pass enforces count and age ceilings only through the minimum live instance cursor; expired instances do not hold storage forever. Every deletion advances the durable pruning watermark, including when the log becomes empty. Reads compare their cursor with that watermark, so identity-sequence gaps caused by rolled-back transactions remain harmless while actual retention gaps fail explicitly. Recovery is allowed only for a real recorded gap and atomically rotates the process generation, advances to the logical head, and renews the lease after local socket fencing.

No Redis dependency is planned for the first supported topology. Redis Pub/Sub is at-most-once, while Streams introduce a second durable system whose commit cannot be atomic with the authoritative database without an additional outbox relay. PostgreSQL already supplies transactions, row locks, advisory locks, `SKIP LOCKED`, and commit-coupled notifications needed by this product's current scale boundary.

## Why multi-process SQLite is rejected

SQLite WAL supports concurrent processes only on the same host, never across a network filesystem, and still permits only one writer at a time. More importantly, SQLite's current WAL documentation records a WAL-reset corruption race affecting versions through 3.51.2 when separate threads or processes write or checkpoint concurrently. Python 3.14.6 on the development host currently embeds SQLite 3.50.4.

The existing SQLite backend serializes application writes inside its one supported process and retains its lifecycle lock. v0.13 will not weaken that protection, put a SQLite volume on network storage, or describe same-host worker experiments as horizontal availability.

## Transaction and fan-out contract

For message creation, editing, deletion, room lifecycle, and member moderation:

1. lock and validate the authoritative PostgreSQL rows;
2. commit the state mutation, audit/outbound webhook rows, and realtime event-log row together;
3. issue a small transaction-coupled notification containing only a cursor hint;
4. let every active instance read ordered event rows after its cursor and dispatch them to local sockets;
5. update the instance cursor only after local dispatch completes.

Notifications never contain message bodies and never authorize access. Missing a notification changes latency, not correctness. Realtime WebSocket delivery remains at-most-once at the individual socket boundary; reconnecting clients recover authoritative history over HTTP.

An instance that falls behind the retained event window must close its local sockets with restart semantics and advance only after clients can recover from authoritative history. It must never pretend that an event gap was delivered.

## Distributed coordination

PostgreSQL-backed deployments require all of the following:

- **Migrations:** one transaction-scoped advisory lock serializes schema inspection and migration. A newer unsupported schema fails closed.
- **Connection capacity:** schema-v4 per-socket leases enforce deployment-wide and per-room caps with database-time expiry and owner-bound renewal/release. The application renews each admitted socket at one third of the configured lease; crashed-instance and archived-room rows are excluded and reclaimable.
- **Presence:** schema-v7 joins/leaves derive from connection transactions, and bounded generation-aware application maintenance emits expiry transitions for crashed instances. Presence remains best effort and carries no authorization meaning; the public relay excludes the origin socket and strips its internal ID.
- **Rate limits:** schema-v5 atomic time-bucket counters enforce deployment-wide subject/client limits for message, search, and typing scopes on the real request paths. Database time defines boundaries so host clock skew cannot multiply quotas.
- **Typing:** schema-v6 state uses transition-only durable events, database-time refresh, and bounded expiry sweeping. The application path and sender-excluding relay are wired; typing is not chat history or audit content.
- **Moderation teardown:** room archive and member-ban events reach every instance, which closes matching local sockets deterministically.
- **Webhook work:** implemented outbox workers claim due rows with an expiring owner lease using row locks and `SKIP LOCKED`. Only the current unexpired owner can acknowledge a claim. A crashed claim becomes eligible for redelivery with its stable ID; receivers still deduplicate that ID.
- **Maintenance leadership:** advisory locks elect one retention, stale-lease, and event-pruning worker at a time. Losing leadership is harmless and retryable.
- **Readiness:** readiness covers pool acquisition, schema compatibility, and event-cursor health. Liveness never depends on PostgreSQL.

## Event retention and backpressure

Event rows can now be bounded by age and count without pruning past the minimum healthy instance cursor. Instance leases distinguish an offline instance from a slow active instance. An expired instance may fall behind the recorded retention watermark; when it returns, the relay fences its sockets and atomically skips to the logical head so clients reconnect and reload database history. A configurable lag ceiling for a still-live but unhealthy process and application-owned scheduling of the bounded maintenance pass remain release gates.

Payloads contain the same versioned event envelopes sent over WebSockets. Administrative metadata may be logged, but access tokens, API keys, database credentials, and message bodies are excluded from operational logs.

## Configuration and deployment boundary

The guarded preview accepts a protected PostgreSQL URL or preferred connection-string file, requires `sslmode=verify-full` for non-loopback servers, exposes bounded pool/lease/poll/maintenance/retention settings, and rejects simultaneous SQLite and PostgreSQL configuration. Connection strings do not appear in settings representations, translated errors, readiness responses, or generated OpenAPI. A live-lag ceiling is still pending.

Application replicas must run the exact same Samsarix version and security configuration. PostgreSQL backup, point-in-time recovery, high availability, encryption, user privileges, patching, and capacity remain deployment responsibilities. The SQLite backup/restore CLI will refuse PostgreSQL targets rather than implying that copying one file backs up a remote database.

## Release acceptance gates

v0.13 cannot claim multi-instance support until CI proves:

- two or more real app processes share one PostgreSQL database and deliver create/update/delete events exactly once to each connected test socket under normal operation (two independently lifespanned application instances now pass; separate OS subprocesses remain);
- a listener disconnect/reconnect replays committed event rows without relying on `NOTIFY` delivery (the internal polling relay and cursor replay case are implemented; the real-process listener gate remains);
- concurrent idempotent message creation returns one authoritative message;
- global and per-room connection caps plus rate limits hold across processes (storage-level concurrency and real application request paths are implemented; separate OS subprocess contention remains);
- archive and ban actions close matching sockets on every process;
- two webhook workers never hold the same live claim, and a killed worker's claim is recovered (the storage-level lease/recovery case is implemented; the killed-process gate remains);
- migration concurrency is serialized and newer schemas fail closed;
- crashed connection leases expire and presence converges;
- an event-log gap fences the lagging instance and clients recover through history (the storage/relay contract is implemented; the real-client recovery gate remains);
- sustained load and forced database/network interruptions have measured, published outcomes;
- SQLite single-instance behavior, package installation, Windows support, and rollback documentation remain green.

## Primary references

- [SQLite write-ahead logging](https://sqlite.org/wal.html) documents same-host-only WAL, single-writer concurrency, checkpoint behavior, and the WAL-reset issue.
- [PostgreSQL `NOTIFY`](https://www.postgresql.org/docs/current/sql-notify.html) documents transaction-coupled interprocess notifications, payload bounds, and queue behavior.
- [PostgreSQL explicit locking](https://www.postgresql.org/docs/current/explicit-locking.html) and [`SELECT`](https://www.postgresql.org/docs/current/sql-select.html) define advisory and row-lock behavior, including `SKIP LOCKED` for queue-like consumers.
- [Redis Pub/Sub](https://redis.io/docs/latest/develop/pubsub/) and [Redis Streams](https://redis.io/docs/latest/develop/data-types/streams/) document the delivery and persistence tradeoffs behind the no-Redis decision.
- [Psycopg async pooling](https://www.psycopg.org/psycopg3/docs/advanced/pool.html) documents bounded asynchronous pool lifecycle and transaction contexts.
