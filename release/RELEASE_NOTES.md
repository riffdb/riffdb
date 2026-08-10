# RiffDB 0.1.0 POC Candidate

This is a proof-of-concept candidate, not a production release. It demonstrates
contract-first command execution, shared public transports, durable outcomes,
provenance, outbox intent, projection frontiers, native MCP, offline
maintenance, and crash recovery only when the revision-specific acceptance
report passes.

## Product Binaries

- `riffdbd`
- `riffdb`
- `riffdb-mcp`

Comparison, fixture, conformance, and safety runners are not shipped products.

## Compatibility

Public protocol, durable records, storage, contract IR, generated schema, MCP,
CLI output, backup manifest, and maintenance receipt formats are versioned.
This artifact carries `release/durable-format-manifest-v1.json` plus the exact
compatibility fixture inventory and release-pair table used to generate it.
The current identity is alpha epoch 1, writer 1. Downgrade is unsupported. See
`docs/compatibility.md`.

The manifest distinguishes the redb layout and each maintenance, migration,
and format-upgrade receipt family rather than treating coincident numeric
versions as interchangeable. Every supported edge names both source and target
release labels.

Daemon startup now compares the retained format marker before redb open and
prints `RDB-FORMAT-0101` with the sole safe action on mismatch. Operators can
run the same source-free check with `riffdb storage preflight`. The declared
alpha-1 writer-0 to writer-1 transition is available only through the offline,
verified-backup-bound, restartable `riffdb storage upgrade` command. New
physical backups carry the exact format marker and compatible restore range;
legacy range-less backups require their source release.

## Offline Contract Migration

P7 supports exact direct-parent offline migration through additive, structural,
and key/ownership gates. The operator workflow is available only through the
shared gRPC administration service, Rust SDK, and CLI with dedicated
`MigrateContract` authority. It retains an immutable pre-migration backup,
publishes a fully validated same-filesystem stage atomically, fences predecessor
writes, and recovers or rolls back before readiness. MCP and application
drivers intentionally expose no migration operation.

## Reactive Applications

P8 adds compiler-proved partitioned domain-event streams, bounded durable
consumers, live named queries, and contextual agent subscriptions. The
TicketDesk Application Source V4 example provides queue and detail watches
through an application-owned authenticated SSE relay and crash-safe generated
reaction helpers across Rust, TypeScript, Python, and MCP. Reactive delivery is
at least once and partition ordered; it is not raw CDC or exactly-once external
effect delivery.

## License

RiffDB is offered under either the MIT License or the Apache License, Version
2.0, at your option. The release bundle includes `COPYRIGHT`, `LICENSE-MIT`,
and `LICENSE-APACHE`.

Copyright © 2026 Kevin O'Shea and O'Shea & Sons, LLC.

## Destructive Restore Warning

Restore preserves `DatabaseId`, advances the durable `history_incarnation`, and
may rewind both authoritative sequence spaces to the backup frontier. Clients
that send their observed incarnation receive a typed mismatch instead of
silently binding a destroyed-suffix observation to new history. Clients that
do not participate remain unvalidated; discard every post-frontier locator,
cursor, session, sequence expectation, authorization observation, and
idempotency assumption after destructive restore.

## Evidence Qualification

- The product server uses redb. The isolated Fjall comparison fails the
  unchanged semantic conformance profile, so its report contains no eligible
  performance samples and this release makes no redb-versus-Fjall speed claim.
- Published redb, canonical PostgreSQL-versus-public-RiffDB, and POC semantic
  workload results are revision-specific. They carry their methodology, raw
  samples, storage medium, durability mode, and checked report hashes.
- Deliberately unsafe PostgreSQL counterexamples are correctness evidence only
  and are excluded from every performance result.
- WP-190 inventories 38 crash boundaries, executes the 18 required
  process-matrix cases, consumes named owner-package evidence for the
  remainder, and records zero production synchronization gaps.
- The bundle includes an installed-binary demo. It is distinct from the full
  source-checkout POC acceptance run and uses no Cargo or test harness.
- POC exit remains unsigned until the three acceptance commands pass at the
  release revision and a maintainer records the architecture-review decision.

See `docs/known-limitations.md` and `release/evidence/`.
