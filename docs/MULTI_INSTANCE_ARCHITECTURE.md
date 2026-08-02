# Multi-instance architecture

Status: **accepted design; implementation in progress for v0.13**.

This document is a contract, not a scale claim. Version 0.12 still supports exactly one Samsarix process and one replica. Multi-instance support becomes releasable only after every acceptance gate below passes against multiple real service processes.

## Decision

The supported multi-instance topology will use PostgreSQL as both the authoritative chat database and the durable coordination plane. Psycopg 3's async connection pool will be an optional installation extra. SQLite remains the zero-service default for single-instance deployments.

PostgreSQL `LISTEN`/`NOTIFY` will be a low-latency wake-up hint only. Every client-visible realtime event will first be inserted into a bounded, ordered database event log in the same transaction as the state change. Each service instance reads committed rows from its own durable cursor. A listener reconnect, notification-queue loss, or process pause therefore causes polling/replay rather than silent event loss.

The initial event-log implementation serializes sequence allocation with a transaction-scoped advisory lock. PostgreSQL identity values alone are not commit ordered: without this lock, a later sequence could commit and be acknowledged before an earlier transaction becomes visible. Event append must remain the final lock-taking phase of a domain mutation. Sustained-load acceptance tests will determine whether this intentionally simple global sequencer is sufficient or must be partitioned without weakening cursor correctness.

Schema v2 uses PostgreSQL database time for room/message/moderation ordering and retention boundaries. Transaction-scoped capacity locks currently serialize room creation, message mutation/retention, and bounded audit insertion across replicas. Deletion and retention scrub message bodies from older durable event envelopes before commit. These conservative global locks make correctness inspectable first; the load gate must measure their throughput before v0.13 receives a scale claim.

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
- **Connection capacity:** expiring per-socket leases enforce deployment-wide and per-room caps. Heartbeats renew leases; crashed-instance leases expire.
- **Presence:** join/leave transitions derive from connection leases. A leader-elected sweeper emits bounded expiry transitions for crashed instances. Presence remains best effort and carries no authorization meaning.
- **Rate limits:** atomic time-bucket counters enforce deployment-wide subject/client limits. Database time defines bucket boundaries so host clock skew cannot multiply quotas.
- **Typing:** transition events use the durable event path but expire automatically and are not retained as chat history or audit content.
- **Moderation teardown:** room archive and member-ban events reach every instance, which closes matching local sockets deterministically.
- **Webhook work:** workers claim due rows with an expiring owner lease using row locks and `SKIP LOCKED`. A crashed claim becomes eligible for redelivery; receivers still deduplicate stable webhook IDs.
- **Maintenance leadership:** advisory locks elect one retention, stale-lease, and event-pruning worker at a time. Losing leadership is harmless and retryable.
- **Readiness:** readiness covers pool acquisition, schema compatibility, and event-cursor health. Liveness never depends on PostgreSQL.

## Event retention and backpressure

Event rows are bounded by age and count, but pruning may not pass the minimum healthy instance cursor. Instance leases distinguish an offline instance from a slow active instance. When a configured lag ceiling is exceeded, the lagging instance is fenced: it closes sockets, marks its cursor recoverable, and requires clients to reconnect and reload database history. One unhealthy process therefore cannot grow the log without bound.

Payloads contain the same versioned event envelopes sent over WebSockets. Administrative metadata may be logged, but access tokens, API keys, database credentials, and message bodies are excluded from operational logs.

## Configuration and deployment boundary

The eventual PostgreSQL mode will accept a protected connection string or connection-string file, require TLS validation for non-loopback servers by default, expose bounded pool/lease/lag settings, and reject simultaneous SQLite and PostgreSQL configuration. Connection strings must never appear in errors, process listings, readiness responses, or generated OpenAPI.

Application replicas must run the exact same Samsarix version and security configuration. PostgreSQL backup, point-in-time recovery, high availability, encryption, user privileges, patching, and capacity remain deployment responsibilities. The SQLite backup/restore CLI will refuse PostgreSQL targets rather than implying that copying one file backs up a remote database.

## Release acceptance gates

v0.13 cannot claim multi-instance support until CI proves:

- two or more real app processes share one PostgreSQL database and deliver create/update/delete events exactly once to each connected test socket under normal operation;
- a listener disconnect/reconnect replays committed event rows without relying on `NOTIFY` delivery;
- concurrent idempotent message creation returns one authoritative message;
- global and per-room connection caps plus rate limits hold across processes;
- archive and ban actions close matching sockets on every process;
- two webhook workers never hold the same live claim, and a killed worker's claim is recovered;
- migration concurrency is serialized and newer schemas fail closed;
- crashed connection leases expire and presence converges;
- an event-log gap fences the lagging instance and clients recover through history;
- sustained load and forced database/network interruptions have measured, published outcomes;
- SQLite single-instance behavior, package installation, Windows support, and rollback documentation remain green.

## Primary references

- [SQLite write-ahead logging](https://sqlite.org/wal.html) documents same-host-only WAL, single-writer concurrency, checkpoint behavior, and the WAL-reset issue.
- [PostgreSQL `NOTIFY`](https://www.postgresql.org/docs/current/sql-notify.html) documents transaction-coupled interprocess notifications, payload bounds, and queue behavior.
- [PostgreSQL explicit locking](https://www.postgresql.org/docs/current/explicit-locking.html) and [`SELECT`](https://www.postgresql.org/docs/current/sql-select.html) define advisory and row-lock behavior, including `SKIP LOCKED` for queue-like consumers.
- [Redis Pub/Sub](https://redis.io/docs/latest/develop/pubsub/) and [Redis Streams](https://redis.io/docs/latest/develop/data-types/streams/) document the delivery and persistence tradeoffs behind the no-Redis decision.
- [Psycopg async pooling](https://www.psycopg.org/psycopg3/docs/advanced/pool.html) documents bounded asynchronous pool lifecycle and transaction contexts.
