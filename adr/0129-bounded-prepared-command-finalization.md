# ADR-0129: Bounded Prepared Command Finalization and Pay-Once Batch Apply

- **Status:** Proposed
- **Direction approved:** 2026-08-16 (maintainer)
- **Exact text accepted:** No
- **Decision deadline:** Before WP-640 changes command preparation, current-state
  validation, sequence assignment, or authoritative batch apply
- **Requires:** ADR-0058, ADR-0059, ADR-0060, ADR-0094, ADR-0095, ADR-0096,
  ADR-0097, ADR-0098, ADR-0099, ADR-0101, ADR-0104, and ADR-0123
- **Defines or blocks:** WP-640

Direction approval authorizes the WP-639 ledger and this draft. Because the
decision changes where transaction-current proofs are constructed and consumed,
AGENTS.md requires exact human acceptance before implementation.

## Context

WP-638 closed unary and c32 write service. WP-639 then measured the exact full
seed boundary. On N1, every command consumes 68.78 us in deterministic
evaluation, 201.58 us in the current mixed validation/encoding/staging stage,
157.91 us in final apply, and 24.83 us sealing the journal after apply. Writer
busy is 492.10 us/command. On the workstation the same costs are 16.84, 52.70,
41.82, and 2.96 us, with 126.80 us total writer busy.

The mixed c32 gate needs a 17--19% gain, but the seed gates need 5.10x on the
workstation and 6.39x on N1. Moving deterministic evaluation alone, or even
pretending the complete validation bucket is movable, cannot reach seed parity.
Final apply currently builds command audit records, command capsules, encoded
segments, transient-index deltas, index-generation postimages, and allocator
state on the ordered lane. The same already-proven graph must become immutable
preparation material and be consumed once, while every current-state and commit
guarantee remains admission ordered.

The safety invariant is not "all command CPU runs on one thread." It is that
fresh authority, transaction-current dependencies, row policy, conflict
ownership, commit sequence, final authoritative mutation, durability, and
publication are checked and applied in their required order. Pure work may be
parallel only when an unforgeable proof reconnects it to those current facts.

## Proposed Decision

### 1. Establish one pay-once proof rule

A guarantee may be proven per datum or per process generation, never per
operation, unless the operation itself is what is being guaranteed. The
coordinator may not decode, normalize, hash, encode, derive, or structurally
validate immutable bytes a second time merely because they crossed an internal
lane. The receiving lane verifies a bounded identity/digest/ordinal proof and
the transaction-current facts that can actually have changed.

Architecture tests enumerate every production consumer of prepared command
material and fail if the authoritative apply path calls the original graph
normalizer, canonical structural validator, capsule payload encoder, or
semantic hasher again. Recovery, startup, external input, retained-history
reads, backups, and replication remain complete validation boundaries; the
rule applies only to already-proven in-process ownership.

### 2. Pure preparation produces no storage or commit authority

After ordinary request admission, current authentication, compiler-declared
snapshot reads, and conflict-domain acquisition, a fixed bounded worker pool
may consume an `EvaluatedCommandAttempt` and produce a move-only
`PreparedCommandBody`. The body contains only:

- exact application, contract, plan, capability revision, request-control,
  input-fingerprint, snapshot, and conflict-domain identities;
- deterministic command outcome and mutation/event templates;
- canonical pre-encoded immutable submessages whose bytes cannot depend on
  current rows, sequence, provenance, service time, index epoch, or audit link;
- exact read-dependency and row-policy evidence digests needed for current
  revalidation; and
- bounded byte, entity, index, event, audit, and outbox charges.

The type carries no storage handle, current-state proof, conflict grant that can
escape cancellation, sequence, durability receipt, publication permission, or
application-visible result. Worker panic, cancellation, capacity refusal, or
proof mismatch destroys the body and follows the existing typed rollback or
retry path. It never produces a partial command.

The pool has a first-party fixed ceiling no greater than eight workers and a
FIFO capacity no greater than the existing accepted coordinator workload
capacity. The standard profile selects at most `available_parallelism - 1`
workers. Applications and operators cannot select worker count, reorder work,
or request the old unsafe shape. Backpressure happens before unbounded material
is retained.

### 3. Admission-order finalization retains every current guarantee

The coordinator consumes prepared bodies in original admission order. For each
body it freshly and exactly:

1. rechecks request cancellation/deadline and post-evaluation authorization;
2. rechecks idempotency disposition and exact pending intent;
3. reads transaction-current dependencies through the prior validated private
   frontier and rejects any changed absence, version, range epoch, or row-policy
   relationship;
4. verifies compiler-declared conflict ownership and affected index epochs;
5. reserves the exact prepared capacity charge;
6. assigns the next authoritative commit sequence and service-owned provenance/
   audit facts; and
7. binds those facts into an `OrderedFinalizationProof` whose ordinal, prior
   frontier, conflict domain, current-evidence digest, prepared-body digest,
   assigned sequence, and resulting logical frontier are exact.

No worker may perform or attest any step in this list. A prepared body is only
an optimization hint until the coordinator consumes it. Any mismatch rejects
or retries under the existing semantics; there is no fallback that trusts a
nearby body.

### 4. A private logical frontier permits bounded encoding overlap

After finalization, the coordinator advances one unpublished logical frontier
containing exact entity postimages, deletions, index-generation postimages, and
read-dependency effects. That frontier is not durable, visible, queryable, or
publishable. It exists only so a later compatible command observes every prior
validated mutation while earlier finalization proofs are being encoded.

Pure workers may bind finalization facts into complete canonical command
capsule fragments and one immutable prepared group. Results may finish out of
order, but a fixed reorder buffer accepts no more than one maximum command group
and releases only the contiguous ordinal prefix. A missing, duplicate, stale,
wrong-domain, wrong-frontier, or wrong-digest result rolls back the complete
unpublished epoch and follows existing uncertainty rules. Conflict leases stay
owned until the same point they are released today.

The physical storage port accepts only a move-only `CheckedPreparedCommandGroup`
constructed from that contiguous prefix. It verifies count, byte charge,
sequence interval, prior/result frontier, affected-epoch interval, and whole-
group digest, then applies the supplied canonical rows and immutable segment
once. It must not re-decode, re-normalize, re-hash, or reconstruct the mutation/
event graph. Index-epoch compare-and-swap, allocator advancement, command rows,
entities, indexes, outcomes, events, provenance, audit, outbox, and commit
records remain one unpublished atomic application.

### 5. Ordering is exact but not accidentally global

The POC retains one commit-sequence allocator and one final authoritative apply
lane. This ADR does not shard storage or introduce concurrent commits. However,
every preparation and finalization proof names its compiler-declared conflict
domain and exact dependencies; it does not claim that unrelated partitions
semantically require one preparation order. The only total order introduced by
this design is the already-authoritative commit sequence at finalization and
publication.

A future coordinator may schedule preparation and current proof per disjoint
ADR-0059 domain while retaining one sequence arbiter, or may map those domains
to replicated leaders. This design must not require rewriting a global mutable
preparation cache or treating arrival order as a cross-domain business fact.

### 6. Durability, publication, and uncertainty do not move

Prepared work and the private logical frontier are discarded on crash. No
response, event, outbox intent, projection frontier, or read view observes them.
The ordered apply produces the same unpublished epoch as today; the pre-zeroed
journal fence, receipt, FIFO fence publication, response release, same-key
uncertainty lookup, replay, and crash recovery remain unchanged.

Acknowledgement still implies that the complete command graph survives crash.
An encoding worker failure before apply is a proven noncommit. Failure after
apply follows the existing unknown-status path and can never be retried under a
new identity. No optimization may turn a worker completion into durability.

### 7. Mechanics and public gates activate the implementation

WP-640 begins with isolated mechanics, not a full semantic rewrite. Activation
requires all of these on both the workstation and N1:

- deterministic evaluation plus validation/encoding preparation at most 180
  us/command on N1, including proof construction, and no slower than current on
  the workstation;
- admission-ordered finalization plus final apply and journal sealing at most
  60 us/command on N1 and 25 us/command on the workstation;
- full seed at most 1.10x same-run safe-application PostgreSQL;
- mixed interactive c32 at least 0.90x same-run safe-application PostgreSQL;
- no read-throughput, read p99, correctness, durability, uncertainty, crash,
  recovery, or write-p95 regression above five percent; and
- exact group-size, frame-byte, physical-fence, and ordered-fence-latency
  evidence retained so a tail change cannot hide behind mean throughput.

The candidate claims no unary c1 latency improvement. Pool handoff may not
regress any unary command by more than five percent, and the release remains
blocked until every representative unary write is at most 1.10x PostgreSQL.
ADR-0127 transport remainder and separately attributed pre-fence work retain
ownership after this package. A candidate that passes c32 but misses seed or
unary is not reported as release-complete.

If the mechanics cannot meet the preparation and ordered-lane thresholds,
WP-640 stops without changing production dispatch. An evaluation-only
candidate is explicitly rejected.

## Options Considered

1. **Move only deterministic evaluation:** rejected. Its 1.34x c32 upper bound
   cannot address seed and says nothing useful about unary latency.
2. **Move evaluation plus all validation without a proof:** rejected. Current
   dependency, row-policy, affected-epoch, capacity, and sequence facts cannot
   be trusted from a worker snapshot.
3. **Parallel commits for disjoint partitions now:** rejected. It changes the
   storage and publication model beyond the measured need and would prematurely
   couple this work to replication.
4. **Bounded pure preparation, admission-order finalization, and pay-once batch
   apply:** proposed because it attacks both measured CPU and final apply while
   preserving the exact authoritative boundary.

## Consequences

- The commit crate gains move-only preparation/finalization proof types and the
  storage port gains one checked prepared-group input.
- The implementation becomes a bounded pipeline with explicit rollback and
  reorder states; deterministic schedule coverage is mandatory.
- A private logical frontier temporarily duplicates a bounded current group,
  never database history or full state.
- Seed parity becomes an activation requirement rather than an aspirational
  extrapolation.
- Unary performance remains a separately owned blocker.

## Compatibility

The decision changes no public gRPC, MCP, CLI, generated SDK, contract grammar,
RiffQL, command/query IR, bundle/module/plan hash, capability, durable record,
journal frame, backup, export, changelog, or replication format. It is an
internal execution and storage-port change. Exact canonical durable bytes and
recovery fixtures must remain byte-identical for identical authoritative facts.

## Security

Workers receive only already-authorized bounded application values in protected
first-party Rust types and emit no logs or diagnostics containing values. Fresh
authorization and row policy remain coordinator checks. Proof failures expose
only closed public error classes and bounded incident identities. No worker,
application, configuration, or transport gains storage, sequence, durability,
policy, or transaction authority.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** the application still invokes
  only a compiled command with typed input and idempotency identity. It cannot
  select workers, proof modes, ordering, grouping, transaction scope,
  durability, or fallback behavior. Every operation retains fresh authority,
  current dependency checks, atomic mutation/outcome/event/provenance, and
  typed uncertainty.
- **Scale:** worker count, preparation queue, reorder buffer, private logical
  frontier, proof bytes, and prepared group are capped by existing command,
  group, and retained-byte ceilings. The design retains no per-history or
  full-state cache and names conflict domains so future partition-local
  scheduling remains possible.

## Testing

- Deterministic schedules for every worker completion order, cancellation,
  panic, queue saturation, preparation mismatch, current dependency change,
  idempotency replay, row-policy change, conflict, group split, and shutdown.
- Architecture tests proving workers cannot name storage, sequence assignment,
  durability, publication, clock, entropy, transport, or policy authority.
- Production-port tests proving checked apply performs no second normalization,
  semantic validation, graph construction, decode, or hash of proven material.
- Byte-identical command segment, audit, event, outcome, provenance, entity,
  index, outbox, commit, backup, recovery, and changelog fixtures.
- Process crash arms before/after preparation, finalization, logical-frontier
  advance, each ordered apply boundary, journal submit, fence, publication, and
  response release.
- Workstation and N1 mechanics, seed-only, c32 interactive, unary, read, and
  tail receipts under the unchanged `PERF-018` comparator.

## Requirements and Work Packages

- **Requirements:** `PERF-001`, `PERF-004`, `PERF-005`, `PERF-008`, `PERF-018`
- **Defines or blocks:** `WP-640`
- **Final evidence:** `WP-640`, followed by the complete release matrix

## Decision Deadline

Exact human acceptance is required before WP-640 changes transaction-current
validation, command sequence assignment, writer-private state, or the storage
apply port.

