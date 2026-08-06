# ADR-0061: Semantic Durability, Terminal Admission, and Storage Format V2

- **Status:** Accepted
- **Date:** 2026-07-30
- **Decision owners:** RiffDB maintainers
- **Related requirements:** `IDEM-001`, `REC-002`, `TXN-004`, `TXN-005`,
  `PERF-004`, `PERF-005`, `PERF-009`, `PERF-010`, `PERF-011`, `PERF-012`
- **Related work packages:** `WP-371`, `WP-372`, `WP-373`, `WP-374`, `WP-375`
- **Amends:** ADR-0004, ADR-0005, ADR-0006, ADR-0007, ADR-0012, ADR-0022,
  ADR-0023, ADR-0058, ADR-0059, ADR-0060
- **Amended by:** ADR-0098

## Context

The post-WP-369 application benchmark proves that RiffDB's remaining write
cost is no longer accidental history growth. A normal successful command still
uses two durable application transitions, and each transition forces redb's
optional two-phase commit mode. The retained record graph also repeats complete
event payloads in the event, commit, and outbox tables; repeats a full type name
and schema hash in every envelope; and advances one durable index epoch for the
whole index and every complete leading prefix affected by an index entry.

These mechanisms implement important semantic guarantees, but the mechanisms
are not themselves the product contract. Freezing them at application alpha
would make later storage, WAL, and migration work needlessly incompatible.

redb 4.1's default Immediate commit uses one fsync plus checksummed commit slots.
Its optional two-phase mode adds a second fsync to mitigate a theoretical
partial-commit attack requiring control of page flush ordering and crash timing,
knowledge of database contents, and arbitrary write data. RiffDB's standard
single-node profile trusts the host, kernel, and storage device not to be
Byzantine. A hardened profile remains available for a stronger local threat
model.

The deterministic command runtime performs no effect and releases no data. It
is therefore safe to treat bounded evaluation as speculative preparation.
Durable Pending is not required for a synchronous attempt until an externally
observable or recoverable admission point exists. A command can transition
atomically from a vacant idempotency identity to its complete terminal graph.

## Decision

### The durability contract is semantic

For one application command:

- no application response is released until the command's durable boundary is
  known committed;
- acknowledgement means the complete terminal command survives process and
  operating-system crash under the selected documented storage profile;
- mutations, declared outcome, event intent, provenance, application commit,
  idempotency terminal state, and terminal service audit are atomic;
- before acknowledgement, recovery may find either the complete terminal
  command or no command, but never a partial command;
- unknown commit status fences authoritative writes and is resolved through the
  exact idempotency identity before another execution;
- reads, command preparation, outbox dispatch, projections, and subscribers
  MUST NOT observe state that may later roll back; and
- every command retains independent identity, outcome, sequence, audit,
  provenance, retry, and uncertainty semantics even when physical durability is
  shared.

`SPEC.md` performance requirements state these guarantees without requiring
redb, a particular fsync count, a Pending row, or a fixed number of physical
transactions. Engine and scheduler mechanisms remain accepted ADR decisions.

### Standard and hardened redb profiles

The standard production application profile uses redb `Durability::Immediate`
with one-phase checksummed commit slots. It assumes a non-Byzantine host and
storage stack. The hardened profile enables redb two-phase commit for operators
whose local threat model justifies the second fsync. The synchronous recovery
oracle uses the hardened profile.

Both profiles provide the same RiffDB application acknowledgement contract.
The physical profile is not an application command input and does not change
authorization, command semantics, durable record identity, or public results.
Rare initialization, migration, restore, and control-plane transitions MAY
remain hardened independently of the hot application profile.

Changing the default requires same-run application evidence and crash/reopen
coverage. Performance claims use the measured result rather than assuming
redb's engine-level maximum improvement.

### Terminal admission for synchronous commands

For a newly executed synchronous mutation, bounded snapshot materialization and
deterministic evaluation MAY occur before a durable Pending row or durable
`Started` audit exists. This work is speculative preparation:

- it uses a fixed invocation-local `tx.time`;
- it performs no network, filesystem, clock, entropy, external effect, durable
  mutation, or response release;
- it remains behind initial current authorization and canonical conflict
  acquisition;
- a fresh authorization check occurs after evaluation and immediately before
  authoritative writer admission; and
- the authoritative transaction rechecks the vacant/equal idempotency identity,
  exact input, selected immutable plan, current capability facts, dependencies,
  invariants, bounds, and conflict ownership.

The common successful path atomically:

1. appends the invocation's `Started` service-audit record;
2. assigns the command and administration sequences;
3. writes the complete application command graph and terminal idempotency state;
4. appends the linked terminal service-audit record; and
5. publishes no result until the durable commit is known.

The direct execution-failure path atomically writes `Started`, the exact
dependency-validated `ExecutionFailed` terminal identity, and linked terminal
audit without first persisting Pending.

Concurrent equal identities may evaluate independently, but the final
transaction admits exactly one terminal result. An equal committed winner is
replayed; different input fails closed. A crash before the terminal transaction
is observationally pre-admission and a later invocation may select a new active
plan and `tx.time`. A crash during an unknown terminal commit uses ordinary
same-key resolution.

Durable Pending remains a versioned compatibility and future explicitly
resumable-workflow state. Existing Pending rows retain their historical plan,
actor, claims, and `tx.time` and use the accepted resume protocol. New
synchronous v1 commands do not create Pending merely to cross runtime
evaluation.

This amends the service-audit boundary: speculative in-process preparation that
releases no data and creates no effect need not leave a durable attempt row
after process crash. Every committed, replayed, denied, cancelled, or
service-observed terminal invocation retains its exact required durable audit.

### Storage format V2

Before application alpha, RiffDB performs one intentional durable-format cut.
V2 uses a compact checked record header rather than repeating a Protobuf
`StoredEnvelope` with a full type name and 32-byte schema hash in every row.
The compact header contains:

- storage-format marker and version;
- closed record tag;
- nonzero schema revision;
- CRC32C of the exact payload; and
- canonical Protobuf payload bytes.

A database-level durable registry digest binds every `(record tag, schema
revision)` pair to its canonical fully qualified type and descriptor-set hash.
The per-row revision remains mandatory so restartable migrations can contain
old and new revisions simultaneously. Readers reject unknown tags, revisions,
registry digests, alternate encodings, key/value mismatches, and checksum
failures before semantic use.

V1 fixtures remain immutable compatibility input. A restartable, idempotent,
bounded migration rewrites V1 envelopes to V2 and verifies the complete result
before activation. New writes use V2 only after migration completion.

### One authoritative event payload

The immutable event table is the sole durable owner of an event's type, payload,
and event hash. Commit records contain ordered `EventReferenceV2` values with
event ID and hash. Outbox intent contains the same small reference, or is
represented by the event row itself when every event is dispatchable.

Event, commit reference, and outbox reference are created atomically. Recovery
loads the event once and proves exact reciprocal IDs and hashes. Dispatch and
projection materialization consume the authoritative event row. No component
may reconstruct or silently substitute an event payload.

Complete historical mutation post-images remain in the authoritative commit log
for this decision. The current entity table necessarily owns the latest
materialized value for point reads. Removing historical post-images requires a
separate retention and replay decision and is not implied by event
deduplication.

### Conservative partition/index generations

The prefix-length epoch fan-out is replaced by one conservative generation for
each compiler-proven `(partition, index)` pair. A command that changes one or
more entries in that pair advances the generation exactly once in its atomic
command transaction.

An index range observation or stable cursor records the pair and generation.
Any later mutation to the index in the same partition invalidates it. This may
cause conservative reevaluation or cursor refresh but cannot admit a stale
range. Cross-partition command reads remain rejected.

If the compiler cannot prove a bounded partition/index generation for a
write-influencing range, it rejects the construct with an actionable
diagnostic. Future finer-grained generations or projections require an accepted
additive decision; application code never chooses an unsafe granularity.

### Non-durable commit chaining is not accepted

redb non-durable commits are visible to later transactions and may roll back
after crash. Chaining them behind one durable tail would violate RiffDB's read
contract unless every reader, command snapshot, outbox worker, projection, and
subscriber is held behind an exact unpublished-generation gate.

Such a protocol requires a tail manifest, independent unknown-result recovery,
prefix-sequence proof, visibility fencing, and process-crash evidence. It is
deferred. This ADR does not permit non-durable authoritative application state
to become visible.

## Consequences

- The common synchronous command has one RiffDB durable transition and one
  one-phase Immediate redb commit.
- Commands retain atomic terminal idempotency without durable pre-execution
  churn.
- A process crash during speculative evaluation may leave no durable audit or
  Pending row because no protected output or effect became visible.
- Standard and hardened profiles share application semantics but state
  different host/storage threat assumptions.
- Storage V2 substantially reduces small-row framing and event amplification.
- Conservative generations trade some false invalidation for bounded safe
  writes.
- Existing V1 databases require the reviewed migration before V2 activation.

## Rejected alternatives

- **Keep two-phase redb commits as the only profile.** It freezes a stronger
  local adversary assumption into every application write.
- **Acknowledge a one-phase commit before `fsync`.** This violates the semantic
  durability contract.
- **Run effectful or output-releasing work before terminal admission.** Only
  deterministic, bounded, side-effect-free preparation may be speculative.
- **Use table-level schema metadata without per-row revision.** Restartable
  migrations may contain mixed revisions.
- **Reference mutable current entities as historical commit post-images.** A
  later update would destroy historical replay evidence.
- **Remove range invalidation.** RiffDB rejects unsupported range dependencies
  rather than permitting stale reads.
- **Chain visible non-durable commits.** This allows state observed before
  acknowledgement to roll back.

## Acceptance reference

The human maintainer explicitly approved the complete pre-alpha semantic
durability, one-phase standard profile, fused terminal-admission, storage-format
V2, event-reference, conservative epoch, and semantic-requirement plan in the
current Codex session on 2026-07-30. The approval did not authorize visible
non-durable state or weaken atomicity, authorization, uncertainty recovery,
audit redaction, provenance, outbox, or fail-closed startup behavior.
