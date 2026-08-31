# Measured PostgreSQL workload and recovery

This is a development acceptance tool, not a production load generator, hosted benchmark service, or capacity guarantee. It starts two real CLI/Uvicorn processes on loopback and uses an explicitly disposable PostgreSQL database. The default SQLite offering and guarded PostgreSQL preview status are unchanged.

## Run safely

Use a Linux checkout with Python 3.10–3.14, procfs and `python -m pip install -e ".[postgres,test]"`. Provision a **separate disposable** PostgreSQL 18 instance with a database named exactly `samsarix_test`. Never run this harness against application data or concurrently with the live pytest suite. It deletes the scratch database's Samsarix tables before and after the run.

Set `SAMSARIX_TEST_POSTGRES_URL` privately to a numeric-loopback URL with an explicit port and `/samsarix_test`, for example `postgresql://<test-user>:<test-password>@127.0.0.1:5432/samsarix_test`. Query parameters, host overrides, DNS names, remote hosts and other database names are rejected before connection. The actual connected database name is checked again; a session advisory lock excludes other harnesses and an activity check refuses an already-used database. These guards do not make shared infrastructure safe: use the isolated instance, and do not start other tests during a run.

```bash
python -m scripts.load_postgres --allow-reset-test-database \
  --scenario steady --duration 180 --rate 20 \
  --rooms 4 --clients-per-room 8 --concurrency 32 --message-bytes 128 \
  --output dist/steady-180s.json
```

The report path must be new; existing reports are never overwritten. Exit 0 means the specified workload met the acceptance checks, not that the system has unlimited capacity. Exit 1 preserves a failed/partial report; setup validation errors exit 2. Reports contain synthetic-workload metrics, configuration, revision and environment information, never tokens, database URLs or message bodies. No public target, cloud account, paid service or telemetry exporter is used.

The CI workflow calls this tool for `steady`, `count`, `age` and `retained-gap` independently, each with a new PostgreSQL service and a 180-second, 20-create-cycle/s, 32-subscriber profile. JSON artifacts are uploaded even when acceptance fails and retained for 30 days. The **PostgreSQL measured workload** manual workflow supports longer runs; its default steady profile is 900 seconds at the same rate. Download evidence before artifact expiration or reproduce the pinned revision. Inputs are passed through quoted environment variables, not interpolated into shell programs.

## Workload contract

- Writes enter replica 1; subscribers are split evenly between both replicas and four rooms by default. This measures cross-instance fan-out, not balanced ingress or geographically distributed clients.
- Scheduled create cycles arrive independently of earlier completion. Twenty percent attempt one edit; ten percent attempt that edit followed by deletion. `--rate` is **create cycles**, not total HTTP requests or fan-out deliveries. Dependent mutations stop if an earlier request is rejected or has an unknown outcome.
- Late schedule slots and exhausted concurrency are counted and dropped, never accumulated into an unbounded queue or replayed in a catch-up burst. Actual-start delay is measured. Any dropped work makes acceptance fail; it must not disappear into a misleading successful-request latency percentile.
- HTTP timings include client/pool/network/server work. Live delivery timings run from that mutation's request start to the observing client's frame receipt using one driver's monotonic clock. They are not commit-to-delivery latency. History items, duplicates, missing deliveries and disconnected periods are not invented as zero-latency samples; inspect their counters and the convergence result alongside percentiles.
- Clients verify signed subject/room boundaries, immutable identity, synthetic content and tombstones. They consume all history-to-live handoff frames, use a post-history ping barrier, and paginate authoritative history on connection/reconnection while continuing to consume live frames. Only one edit and one deletion per synthetic message are supported by this harness's version reconciliation.
- After writes stop, **without a final history reload into the clients**, every client's materialized state must match the complete authoritative HTTP history. Every acknowledged version must still exist at that version or newer. Ambiguous HTTP outcomes are counted separately, and final database state identifies committed creates lacking an observed acknowledgement.
- Every acknowledged create/edit/delete version must also appear exactly once on each uninterrupted stream; final state alone cannot hide a missed intermediate edit. Faults excuse interruptions only on the paused replica and the explicitly archived room, not unrelated healthy streams. Deletion intentionally scrubs bodies from older retained events: the harness accepts and counts empty prior-version live payloads only after that message's deletion was attempted, still requires all event versions and final authoritative tombstones, and never accepts blank non-deleted HTTP history as normal content.
- All connection leases must drain to zero. No socket drop, readiness refusal or recovery is silently retried in the steady scenario. Its initially empty clients must observe no duplicate or older message frames. Fault runs explicitly account for reconnect attempts, history overlap and close codes.

The open-arrival design follows the measurement concerns in [Grafana's open/closed workload explanation](https://grafana.com/docs/k6/latest/using-k6/scenarios/concepts/open-vs-closed/) and [dropped-iteration guidance](https://grafana.com/docs/k6/latest/using-k6/scenarios/concepts/dropped-iterations/); this harness does not depend on k6.

## Fault profiles

At one quarter of the scheduled duration, the fault profiles pause only their own replica 0 child. The existing kernel-stop/database-idleness barrier avoids freezing a database transaction. Writes continue through replica 1 at the original offered schedule. Room 0 is frozen/unfrozen and archived/reopened during the pause; healthy subscribers in other rooms must remain connected.

`count` waits for the real unread backlog to exceed the configured count bound while the lease remains live. `age` waits for real database-clock age to exceed three seconds while the lease remains live. `retained-gap` uses natural three-second lease expiry and waits for the healthy application's actual periodic pruning to pass the old cursor. No clock, event, cursor, lease or pruning timer is rewritten. Fault profiles require at least 180 scheduled seconds to include recovery and a subsequent live interval.

After resume, old replica 0 sockets must close with 1012 without obsolete message or lifecycle frames. A new live generation/cursor and all affected clients' reconnect/history/activation must be observed while offered traffic continues. Final live state must converge. The report includes the measured barrier and time from process resumption to reconnected clients. This is a synchronized reconnect burst at the stated population, not proof for every larger storm or client implementation.

## Interpret the report

`schema_version: 1` contains the profile, effective non-secret server overrides, exact checkout revision, Python/OS/CPU information, PostgreSQL version, counts, exact nearest-rank p50/p95/p99/max latency distributions, timeline, per-client counters and one-second-interval samples. Empty distributions have null percentiles, not zeroes.

Samples include actual unread event count/age, live lease/cursor/retention positions, application-process RSS/PSS from the kernel's documented [`smaps_rollup`](https://www.kernel.org/doc/html/latest/filesystems/proc.html), and total scratch database bytes. Sampling and SQL instrumentation share the same host and add overhead. These are sampled values, not hard maxima or memory-leak proof; RSS can double-count shared pages, and application-process memory excludes PostgreSQL and the driver. PostgreSQL observers use autocommit to avoid holding a stale transaction snapshot, following its [statistics documentation](https://www.postgresql.org/docs/current/monitoring-stats.html).

The harness caps scheduled creates at 20000, subscribers at 128, in-flight cycles at 128, duration at 1800 seconds, payload at 4096 ASCII bytes and retained client-payload estimates at 128 MB. Python object/metric overhead is additional. Each HTTP operation, connection attempt, fault wait, final drain and process teardown also has a bound. These are harness safety limits, not advertised product limits. Every profile records its actual capacity/rate/pool/lease/lag overrides; do not compare it to unmodified production defaults.

## Evidence and remaining gates

The first live [run 33422510240](https://github.com/Deathcharge/samsarix-chat-engine/actions/runs/33422510240) passed the ten existing jobs but failed all four new profiles. The harness incorrectly required original content in every retained create/edit event, overlooking the store's intentional deletion scrubbing; 16 subscribers in the two rooms receiving deletions rejected those frames. All 3600 scheduled create cycles started in each profile, but these failed subscriber/recovery results are **not accepted measurements**. The validator now distinguishes scrubbed prior-version live events from normal HTTP content, retains exact event-coverage and final-state checks, and tests redaction/history overlap without restoring deleted bodies. Corrected live results remain required; 59 local harness tests alone do not establish performance.

Loopback WebSocket connections explicitly disable environment proxies on versions that support automatic proxy discovery, without forwarding unsupported options to older supported versions. See the [websockets proxy documentation](https://websockets.readthedocs.io/en/stable/topics/proxies.html).

Measured host-specific profiles will not by themselves close kernel packet-loss, database failover, production-sized datasets, long-duration resource-growth, backup/PITR/restore/rollback or deployment-identity gates. The framework-neutral SDK has its own recovery tests; this Python workload is not a substitute for a browser/client-device matrix. Multi-instance production support remains gated by the full [architecture acceptance list](MULTI_INSTANCE_ARCHITECTURE.md).
