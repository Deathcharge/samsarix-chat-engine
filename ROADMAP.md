# Samsarix Chat Engine roadmap

This roadmap separates four gates: merge, release, publication, and flagship adoption. Passing one does not imply the next.

## Product boundary

Portfolio role: **reference implementation**. Keep this as a bounded reference implementation unless a real consumer proves a stronger role. Avoid turning it into a second canonical backend or user-facing platform.
Planned repository identity: `Deathcharge/samsarix-chat-engine` (ready).

Current disposition: Merge the productization branch after exact-head verification and rollback-ref creation; release and adoption remain separate decisions.

## Stabilize the productized default

- Keep the default branch buildable from a clean checkout and preserve exact-head CI evidence.
- Keep Samsarix LLC branding, package identity, license metadata, and compatibility aliases internally consistent.
- Preserve the pre-productization default under a rollback ref before merging; do not delete legacy history.
- Review priority: Create a review PR, run cross-platform CI and a two-client installed-wheel acceptance test, then decide whether to publish-and-freeze or name a real consumer.

## Release candidate

- Tag a reproducible reference snapshot with truthful support status.
- Add one end-to-end example that runs from the distributed artifact.
- Freeze feature growth unless a named consumer adopts the contract.

Current hardening backlog:

- No real identities, per-room authorization, moderation, deletion/export, attachments, or end-to-end encryption.
- No supported multi-worker topology, broker, distributed rate limits, or load/soak measurements.
- Public deployment creates an ongoing security/operations burden disproportionate to demonstrated demand.
- The flagship already has broad chat capabilities; no consumer validates this separate protocol.
- Version/package publication, remote matrix, support, and data-governance commitments remain unresolved.

## Samsarix adoption

- Define a public API, event, schema, artifact, or deployment contract before connecting to Samsarix Unified.
- Add a consumer-owned contract fixture covering authentication, privacy, limits, errors, and version compatibility.
- Make one implementation canonical; remove or freeze duplicate behavior only after parity and rollback are proven.
- Record an owner, support level, compatibility window, and measurable adoption signal.

## Completion evidence

A milestone is complete only when its exact commit, commands and results, artifact digest, consumer or deployment, and rollback path are recorded in a pull request or release record. README claims must not exceed that evidence.
