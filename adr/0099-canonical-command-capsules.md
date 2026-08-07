# ADR-0099: Canonical Command Capsules and Locator Rows

- **Status:** Accepted
- **Date:** 2026-08-06
- **Decision owners:** RiffDB maintainers
- **Related requirements:** `STO-002`, `STO-020`, `STO-021`, `STO-022`,
  `REC-002`, `TXN-041`, `TXN-042`, `TXN-043`, `TXN-044`, `PERF-008`,
  `PERF-009`, `PERF-010`, `PERF-016`
- **Related work package:** `WP-477`
- **Amends:** ADR-0005, ADR-0006, ADR-0021, ADR-0061, ADR-0082,
  ADR-0083, ADR-0085, ADR-0098

## Context

After the retained command-CPU packages, a full public TicketDesk seed commits
19,220 independent commands in about 2.1 seconds. The writer already averages
roughly 128 commands per physical commit, and a measured attempt to pre-form and
merge larger writer units changed total seed time by only about one percent.
The retained writer decomposition is approximately 0.76 seconds in validation,
encoding, and staging and 1.00 second in redb commit/fence work.

A sampled full-process profile has no single dominant routine. Its cost is
distributed across compact-envelope CRC and structural preflight, Protobuf
encode/decode, allocation, and redb B-tree reads and writes. That shape matches
the durable model: the same immutable command identity, plan, actor, logical
time, partition, conflict hashes, outcome, provenance, and audit facts are
encoded in several independently framed rows across the commit, idempotency,
provenance, and service-audit trees.

Those rows provide required semantic lookup surfaces, but complete duplicated
payloads are a storage mechanism rather than a product guarantee. Before alpha,
the format can make one canonical record the durable owner and retain the lookup
surfaces as self-verifying locators. After alpha, the same change would be a
larger compatibility and retained-history migration.

## Decision

### One canonical successful-command capsule

Every newly committed successful command has exactly one
`StoredCommandCapsuleV1`, keyed by the unchanged big-endian commit-sequence key
in the existing `commits` table. The capsule is the sole durable owner of the
shared successful-command facts currently repeated by `StoredCommitRecordV3`,
`StoredOutcomeV2`, `StoredProvenanceRecordV2`, and the command's linked Started
and terminal service-audit records.

The capsule contains each shared fact once and contains enough bounded data to
reconstruct those four existing semantic views exactly:

- idempotency identity, admission request, exact executable-plan reference,
  canonical input hash, actor, logical time, partition key and hash, ordered
  conflict hashes, admitted provenance claims, and optional command causation;
- assigned commit sequence, declared outcome, provenance identity, durability
  mode, canonical read dependencies, ordered entity post-image references,
  ordered event references, and ordered outbox event identities;
- provenance affected-entity and event identities where they are not already a
  total function of the commit references; and
- one normalized command service-audit invocation plus the Started and terminal
  administration sequences, timestamps, terminal phase, and exact command link.

The semantic storage API continues to return the existing checked outcome,
commit, provenance, and audit types. No application, gRPC, MCP, CLI, generated
client, projection, event, query, or recovery caller receives a capsule or
locator type.

The existing authoritative entity, secondary-index, index-generation, event,
event-route, and outbox rows remain separately owned. The capsule continues to
carry only hash-bearing entity and event references. This decision does not
reintroduce duplicated entity post-images or event payloads.

### Compact self-verifying locators

The existing physical lookup tables and their key byte layouts remain:

- a successful terminal row in `idempotency` stores a
  `StoredCommandLocatorV1` naming the command sequence instead of a full
  `StoredOutcomeV2`;
- a row in `provenance` stores the same locator instead of a full
  `StoredProvenanceRecordV2`; and
- the command-owned Started and terminal rows in `audit` store a
  `StoredCommandAuditLocatorV1` naming the command sequence and closed Started
  or terminal member instead of a full `ServiceAuditRecordV2`.

`audit_by_request` keeps its existing request/administration-sequence locator.
Non-command service-audit records remain complete `ServiceAuditRecordV2` rows.
Durable Pending and deterministic `ExecutionFailed` idempotency rows retain
their existing complete records because no successful command capsule exists.

A locator is never trusted by itself. In one read transaction the adapter must
load the named capsule, validate the capsule's physical commit key and canonical
payload, reconstruct the requested semantic view, and prove that view against
the lookup key:

- the complete idempotency key equals the reconstructed outcome identity;
- the provenance key equals the reconstructed provenance identity;
- the audit key equals the selected administration sequence and the selected
  record has the exact request, operation, principal, targets, phase, and command
  link; and
- the request index still reciprocates with the reconstructed audit record.

Missing capsules, cross-role locator substitution, mismatched keys, duplicate
members, malformed or noncanonical bytes, and broken reciprocal links are
authoritative corruption and fail closed. Locator values contain no business
payload, credential, actor, customer data, or arbitrary diagnostic text.

### Atomic write and uncertainty boundary

The sole writer constructs the capsule and every locator only after the same
complete command graph and service-audit transitions have passed their current
semantic, canonical, transaction-current, capacity, sequence, and reciprocal
validation. Entity/index/event/outbox rows, the capsule, every locator, both
request-index rows, and both allocators are staged in the same redb transaction.

No result, notification, transient index, derived work, or response is released
before the existing direct Immediate boundary or ADR-0098 tail fence is known
successful. Unknown status continues to fence writes and resolve the exact
idempotency identity; successful resolution follows the locator to the capsule
and reconstructs the original persisted outcome. Physical grouping, FIFO
sequence assignment, cancellation, retry, and independent command semantics do
not change.

The conservative 16 MiB pre-sequence reservation is revised to dominate the
capsule plus locators and every unchanged row. Final compact-envelope encoding
still proves exact per-record and aggregate bounds before insertion.

### Bounded restartable migration

The record registry gains current writable entries for
`StoredCommandCapsuleV1`, `StoredCommandLocatorV1`, and
`StoredCommandAuditLocatorV1`. Historical commit, outcome, provenance, and
service-audit revisions remain readable migration inputs and immutable
compatibility fixtures.

An offline dormant migration processes commands in increasing commit-sequence
order, bounded per durable page by at most 500 commands and 4 MiB of inspected
plus replacement bytes. For each command it loads the existing commit, outcome,
provenance, linked Started/terminal audit rows, and request-index rows from one
snapshot; validates every existing canonical and reciprocal rule; constructs
and revalidates the exact semantic views from the new capsule; then atomically
replaces that command's current rows with the capsule and locators.

Mixed old/new rows are valid only while the predecessor registry digest remains
active during the migration. Each migrated command is internally complete; a
restart skips already-capsuled rows after revalidating them and resumes at the
first old commit. The new registry digest is published only after every retained
successful command is capsulated and a complete structural/historical pass
succeeds. Normal operational ports are not exposed during migration.

Rollback to a pre-capsule binary after the new digest publishes is unsupported.
Backup and restore preserve exact bytes and run the same migration/validation
sequence when restoring an older digest.

### Retention and history

Commit sequence remains the single total order. Existing retention watermarks,
holds, checkpoints, and entity-chain validation bind the reconstructed semantic
commit view. Pruning a capsule is equivalent to pruning the historical command
record and may occur only under the existing retention rules. Locator pruning
must be atomic with or safely ordered behind the capsule watermark so no
retained locator points below available history.

The accepted idempotency identity key bytes do not change. This ADR amends
ADR-0082 only to permit the successful terminal value to become a checked
locator; it does not permit an alternate identity, locality prefix, or weaker
retry lookup.

## Consequences

- Shared successful-command payload is canonically encoded and checksummed once
  instead of independently in commit, outcome, provenance, and command-audit rows.
- Hot command transactions retain the same lookup keys but write much smaller
  values to three auxiliary trees.
- Idempotency, provenance, and audit reads add one bounded commit-table lookup;
  these surfaces are materially less frequent than fresh successful writes and
  remain one storage snapshot.
- Startup and migration validation become capsule-centric while continuing to
  prove every semantic relationship.
- This is an intentional pre-alpha durable-format cut, not a relaxation of the
  product's command, audit, provenance, idempotency, crash, or authorization guarantees.

## Rejected alternatives

- **More scheduler windows or a larger command count ceiling.** Measured groups
  are already large and a retained experiment produced only about one percent.
- **Embed entity or event payloads in the capsule.** Reintroduces duplication
  removed by ADR-0083 and the event-reference migration.
- **Delete provenance or command audit.** Weakens product semantics rather than
  changing their representation.
- **Trust locators without joining the capsule.** Allows missing or substituted
  authority and is fail-open.
- **Keep full duplicated rows as a cache.** Preserves the write amplification
  this decision exists to remove and creates two authoritative copies.
- **Introduce an asynchronous WAL first.** Adds overlay reads, checkpointing,
  and a second recovery protocol before exhausting the simpler redb layout lever.

## Acceptance

The maintainer explicitly approved proceeding with the canonical command
capsule, compact locator, and bounded migration format cut on 2026-08-06 after
reviewing the measured grouping result, sampled command path, incompatible
durable-format boundary, and retained semantic guarantees.
