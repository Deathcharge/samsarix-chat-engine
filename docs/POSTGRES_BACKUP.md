# PostgreSQL backup, restore, and recovery contract

Status: **logical restore and disposable physical PITR rehearsals implemented; provider failover remains deployment-owned and unverified**.

Samsarix does not copy PostgreSQL files or hide database recovery behind an application command. PostgreSQL exposes three distinct backup families—logical dumps, physical filesystem/base backups, and continuous WAL archiving—and they have different portability, recovery-point, privilege, and version properties. The authoritative starting point is PostgreSQL's [backup and restore chapter](https://www.postgresql.org/docs/18/backup.html).

## What CI now proves

The `PostgreSQL logical restore rehearsal` workflow provisions a disposable PostgreSQL 18.6 service and then:

1. initializes current schema 14 through the real Samsarix application;
2. creates rooms, edited and tombstoned messages, searchable current content, a member read cursor, archived/frozen lifecycle history, audit metadata, realtime events, cursors, and identity sequences through HTTP/application behavior;
3. stops the source application cleanly;
4. creates a whole-database custom-format archive using `pg_dump --format=custom --no-owner --no-privileges`;
5. confirms `pg_restore` can inspect the archive, records its SHA-256 in the job log, and restores it with `--exit-on-error --single-transaction` into a fresh database created from `template0`;
6. starts a different Samsarix instance against the restored database and verifies readiness, room metadata, edited history, tombstones, search, read state, archived state, metadata-only audit actions, and schema compatibility;
7. proves the restored database remains writable through create, edit, delete, unarchive, and rearchive operations.

The probe refuses remote hosts, URL query/fragment overrides, libpq routing environment overrides, any database name except the two fixed disposable rehearsal names, and execution without an exact confirmation value. HTTP failures never echo response bodies. This is repeatable application-level logical-restore evidence, not a production backup service or durability SLA.

The separate `PostgreSQL physical PITR rehearsal` workflow also provisions a disposable PostgreSQL 18.6 cluster and:

1. enables continuous WAL archiving, provisions a separate application role, and initializes current schema 14 through Samsarix;
2. records baseline application state, takes a plain whole-cluster `pg_basebackup` with streamed WAL and SHA-256 manifest checksums, and requires `pg_verifybackup --exit-on-error` to accept it;
3. commits another message after the base backup, creates a named restore point, then commits a divergent message after that target;
4. forces and confirms archival of the WAL segment containing the post-target write;
5. revokes the old primary's application-role `CONNECT` privilege, terminates its sessions, and proves that credential cannot reconnect;
6. starts the physical backup as an isolated second cluster, replays archived WAL to the named restore point, promotes it, and requires a newer timeline with the same cluster system identifier;
7. verifies through a different Samsarix instance that base-backup and WAL-only state survived, the later divergent message did not, search/read-state/audit invariants hold, and five post-recovery writes succeed; and
8. publishes a content-free JSON evidence artifact with the exact revision, PostgreSQL version, restore point/LSN, timelines, archived-WAL count, manifest/inventory hashes, fencing result, and application acceptance result.

Its second safety probe accepts only `samsarix_pitr_source` on loopback port 5432 for source phases and port 55432 for recovery verification, with a separate exact confirmation. This is real physical recovery evidence on one ephemeral CI host. The archive and backup share that host, and the old process remains available to its superuser: the test proves an application-role database fence, not storage durability, network/power fencing, routing cutover, provider promotion, failback, RPO, or RTO.

## Choose the recovery layer deliberately

### Logical archive for portability and pre-upgrade rollback

PostgreSQL documents that `pg_dump` takes an internally consistent snapshot without blocking ordinary readers or writers. Custom archives can be inspected and selectively or fully restored with `pg_restore`; unlike a physical backup, a logical dump can generally move to a newer PostgreSQL major version or different machine architecture. See [`pg_dump`](https://www.postgresql.org/docs/18/app-pgdump.html), [SQL dumps](https://www.postgresql.org/docs/18/backup-dump.html), and [`pg_restore`](https://www.postgresql.org/docs/18/app-pgrestore.html).

For a dedicated Samsarix database, take the whole database rather than naming today's tables. That includes future schema objects and the sequences used by realtime events. A portable role-neutral archive can use:

```bash
export PGSERVICE=samsarix-backup
export PGPASSFILE=/run/secrets/samsarix-pgpass
umask 077
pg_dump \
  --format=custom \
  --no-owner \
  --no-privileges \
  --file=/protected-backups/samsarix-YYYYMMDDTHHMMSSZ.dump
pg_restore --list /protected-backups/samsarix-YYYYMMDDTHHMMSSZ.dump >/dev/null
sha256sum /protected-backups/samsarix-YYYYMMDDTHHMMSSZ.dump \
  > /protected-backups/samsarix-YYYYMMDDTHHMMSSZ.dump.sha256
```

Configure `PGSERVICE` and `PGPASSFILE` as protected files; do not place passwords in commands, logs, process arguments, repository files, or backup names. `--no-owner --no-privileges` deliberately excludes ownership and grant recreation, so provision and test the destination role separately. If exact cluster roles/tablespaces are part of the recovery requirement, manage them as infrastructure or capture reviewed `pg_dumpall --globals-only` output with equivalent secret and change control.

A hash detects accidental change only when its stored value is separately protected; it does not authenticate an archive against an attacker who can replace both files. Encrypt backups with an operator-controlled key and test key recovery independently.

### Physical backup and PITR for a bounded recovery point

A periodic logical dump cannot recover commits made after its snapshot. PostgreSQL PITR starts from a physical base backup and replays a continuous, complete WAL archive. PostgreSQL 18 requires `wal_level=replica` or higher and enabled archiving; `pg_basebackup` can create a live cluster base backup suitable for PITR or a standby. See [continuous archiving and PITR](https://www.postgresql.org/docs/18/continuous-archiving.html), [`pg_basebackup`](https://www.postgresql.org/docs/18/app-pgbasebackup.html), and [WAL configuration](https://www.postgresql.org/docs/18/runtime-config-wal.html).

Samsarix cannot choose an archive repository, retention window, encryption/KMS design, replication topology, or recovery objective for the operator. The repository now proves base-backup/WAL mechanics and application-level target acceptance on a single disposable host. Before production use, the deployment owner must still prove on isolated infrastructure that:

- every required WAL segment reaches durable storage and alerting detects an archive gap;
- the base backup and WAL archive can be decrypted without the primary environment;
- recovery reaches an exact timestamp/LSN or named restore point on the intended PostgreSQL major version;
- the recovered database passes Samsarix schema/readiness and application-history checks;
- the measured recovery point objective (RPO) and recovery time objective (RTO) meet the service commitment;
- promotion, DNS/router cutover, fencing of the old primary, and failback cannot create two writable primaries.

This repository does not yet run provider/database failover, external old-primary fencing, routing cutover, or failback gates, so PostgreSQL remains a preview.

## Restore rehearsal and cutover

Never restore over the only copy of a database. Restore to an empty database or isolated cluster first:

```bash
export PGSERVICE=samsarix-restore-admin
export PGPASSFILE=/run/secrets/samsarix-restore-pgpass
createdb --template=template0 samsarix_restore_candidate
pg_restore \
  --dbname=samsarix_restore_candidate \
  --exit-on-error \
  --single-transaction \
  --no-owner \
  --no-privileges \
  /protected-backups/samsarix-YYYYMMDDTHHMMSSZ.dump
```

Then apply this gate:

1. Keep the candidate isolated from user traffic and webhook egress. A restored pending webhook can represent an external effect that already happened after the snapshot; receivers must deduplicate stable delivery IDs.
2. Provision the reviewed least-privilege application role, TLS roots, authentication keys, limits, retention settings, and unique instance identities outside the archive.
3. Start exactly one matching Samsarix binary against the candidate and check `/readyz`, schema version, rooms, history/tombstones, search, read state, moderation/lifecycle state, audit metadata, and webhook/outbox state. Do not edit the schema marker to force compatibility.
4. Perform a controlled write/read/delete cycle in a synthetic room. Remove that room through the supported lifecycle if the candidate will become live.
5. For an application rollback, restore the backup captured before its schema upgrade and run the matching older binary. Do not point an older binary at a newer schema or attempt a downgrade by dropping tables.
6. Fence every writer to the old database before routing traffic to the candidate. Preserve the old database until the recovery decision and retention policy permit disposal.
7. Record archive identity, source/destination PostgreSQL and Samsarix versions, recovery target, RPO/RTO observed, verifier results, approver, and rollback decision without recording credentials or chat content.

`pg_restore` executes database definitions contained in the archive. PostgreSQL warns that an archive from an untrusted source can execute source-superuser-chosen code; inspect and trust the archive provenance before restore. A successful command exit is necessary but insufficient—the application-level acceptance above is the recovery proof.

## Privacy, deletion, and cost

Logical and physical backups contain plaintext chat content, tombstones and historical content present at their snapshot, token subjects, audit metadata, and pending webhook payloads. A later live deletion cannot recall an older archive. Protect backup access, transport, encryption keys, retention, legal hold, and eventual destruction as carefully as the live database.

Samsarix adds no metered backup vendor. Storage copies, WAL volume, cross-region transfer, encryption/KMS operations, restore compute, monitoring, and rehearsal time remain operator costs. Measure archive growth and restore duration with representative retained history before setting schedules or claims.
