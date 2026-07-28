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
Downgrade is unsupported. See `docs/compatibility.md`.

## License

RiffDB is offered under either the MIT License or the Apache License, Version
2.0, at your option. The release bundle includes `LICENSE-MIT` and
`LICENSE-APACHE`.

## Destructive Restore Warning

Restore preserves `DatabaseId` and rewinds both authoritative sequence spaces.
The POC has no incarnation/history epoch. A destroyed sequence suffix may be
reused for different records, and idempotency keys that existed only in that
suffix may be accepted again. Discard every post-frontier locator, cursor,
session, sequence expectation, authorization observation, and idempotency
assumption.

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
