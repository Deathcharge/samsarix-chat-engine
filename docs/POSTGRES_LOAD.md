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

Unset `PGHOSTADDR`, `PGSERVICE`, `PGSERVICEFILE` and `PGOPTIONS` before running. The harness rejects these [libpq environment defaults](https://www.postgresql.org/docs/current/libpq-envars.html), which can redirect the numeric host or silently alter the measured sessions. It does not print their values or modify the user's environment.

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

### First corrected measurements — 2026-08-31

All four profiles in [run 33423357906](https://github.com/Deathcharge/samsarix-chat-engine/actions/runs/33423357906) passed at PR head `4c8ba301708e85b68624a96b2f2561b9cdd1c0e8` (tested merge checkout `3c118eb67267227fba329233d80660036e72e5c4`). Each ran 180 scheduled seconds, 20 create cycles/s, four rooms, eight subscribers/room, 128-byte content, concurrency limit 32 and the documented per-scenario overrides. Every job used its own shared GitHub Ubuntu x86-64 runner with four visible/affinity CPUs, Python 3.14.7 and PostgreSQL 18.4 (Debian). This fixed CI database version is the measured environment, not a claim that it is the latest PostgreSQL patch.

| Profile | Started / offered creates | Accepted creates / edits / deletes | POST p95 / p99, ms | Resume to reconnected, s |
| --- | ---: | ---: | ---: | ---: |
| steady | 3600 / 3600 | 3600 / 720 / 360 | 11.49 / 13.15 | not injected |
| count | 3600 / 3600 | 3590 / 717 / 357 | 13.96 / 40.71 | 0.537 |
| age | 3600 / 3600 | 3590 / 717 / 357 | 11.78 / 14.06 | 0.614 |
| retained-gap | 3600 / 3600 | 3589 / 717 / 357 | 12.00 / 14.77 | 0.861 |

POST percentiles include all 3600 responses, including the deliberate lifecycle refusals. Each fault profile recorded exactly 11 expected frozen/archived HTTP rejections, zero unexpected rejections/ambiguous outcomes, 16 stale-owner closes with 1012, and 52 successful connections including reconnects. All profiles had zero scheduling/concurrency drops, 32 converged clients, 32 verified cross-room denials, zero committed creates without acknowledgement, and zero final connection leases. Steady streams had zero missing or duplicate acknowledged live versions; all uninterrupted healthy streams in fault profiles also passed exact coverage. Fault-interrupted streams deliberately miss live versions and recover through history; their aggregate missing-version counters were count 388, age 392 and retained-gap 7732, **not zero-latency deliveries or an exactly-once fault guarantee**.

Steady live delivery p95/p99 was 62.74/65.43 ms on replica 0 and 63.02/66.04 ms on replica 1 (request-start to frame receipt). Start-delay p99 stayed below 2.13 ms in these four runs. Sampled application RSS peaks ranged from 87072 to 91044 KiB and PSS from 64582 to 68510 KiB; PostgreSQL and driver memory are excluded. Final scratch database sizes were approximately 14.65–15.39 MB versus 8.66–8.69 MB initially, with intentionally accumulated message history. Three-minute samples are not resource-growth or leak evidence.

Count and age barriers both observed backlog 107 with a live lease; measured oldest unread age was about 4.03 s. Natural retained-gap recovery waited for the application's real periodic bounded pruning, not just the three-second lease expiry: its pause lasted about 74.69 s, and its eventual barrier observed a pruned gap with an expired lease. The resumption figures above exclude that intentional pause. One-second samples can miss a barrier peak and can exceed the later barrier's backlog after pruning; do not equate their maxima with the precise barrier snapshot.

Original JSON artifact SHA-256 digests (artifact names are `postgres-load-<profile>-33423357906-1`, file `load-report.json`; retention 30 days):

| Profile | SHA-256 |
| --- | --- |
| steady | `D9D70DE91C07FAF1B6C59E5744EACAD712B2AE791670B6E1D3DFE99C48D8D966` |
| count | `D50240B899D8A270B0BF070A5F21642296327AB18B4A768C18A8DCBE6482F81D` |
| age | `6A3C150736A7539D4C62DD1D66C6DF44AF4E864031B24FF470114EEB4BBDAC0A` |
| retained-gap | `9A00BDFD4CF55B246FCCFB7DDF880E984CF72B9EE248D3E08F4BB802982207B8` |

These are the first corrected workload results, not the final reviewed-head or post-merge acceptance record. Subsequent validation adds event-type and libpq-environment checks; final CI and installed-package evidence is recorded on PR #41 before merge.

Final dependency review found the [PostgreSQL 18.6 security/bug-fix release](https://www.postgresql.org/docs/release/18.6/) and verified `18.6-bookworm` in the [official Docker image manifest](https://github.com/docker-library/official-images/blob/master/library/postgres). Both existing PostgreSQL CI services and the load service now use that patch. The 18.4 measurements above remain historical; final-head and post-merge runs must verify 18.6 separately. No live database is upgraded by this repository change. Deployment owners must review the release's configuration/data-cleanup notes and back up their own database before their upgrade; the disposable CI tests do not establish production migration safety.

Measured host-specific profiles will not by themselves close kernel packet-loss, database failover, production-sized datasets, long-duration resource-growth, backup/PITR/restore/rollback or deployment-identity gates. The framework-neutral SDK has its own recovery tests; this Python workload is not a substitute for a browser/client-device matrix. Multi-instance production support remains gated by the full [architecture acceptance list](MULTI_INSTANCE_ARCHITECTURE.md).
