# ADR-0132: Bounded Journal Fence Pipeline and Ordered Durable-Prefix Publication

- **Status:** Proposed
- **Direction approved:** 2026-08-17 (maintainer)
- **Exact text accepted:** No
- **Decision deadline:** Before WP-649 changes journal submission, durability
  completion, publication ordering, or acknowledgement
- **Requires:** ADR-0058, ADR-0098, ADR-0101, ADR-0103, ADR-0104, ADR-0123,
  ADR-0127, and ADR-0129
- **Defines or blocks:** WP-649

Direction approval authorizes reject-first mechanics, closed production timing
evidence, and this draft. It does not authorize a transaction-ordering,
durability, publication, or acknowledgement change before exact human
acceptance.

## Context

Current cloud evidence has converged on the journal completion tail. At
interactive c32 the physical journal write and `fdatasync` work is about 1.4
milliseconds per physical flush, while ordered command completion is about 6.6
milliseconds per group. During the full seed the corresponding ordered
completion grows to about 24.7 milliseconds per group. WP-644's bounded
multiplexed session already reaches 1.01x and 0.94x safe-application PostgreSQL
mixed c32 throughput on the N1 and E2 profiles, but misses the c32 p95 gate at
1.39x and 1.29x. Unary and session writes share the same journal tail.

The current journal lane owns one worker. It drains every immediately ready
submission, encodes and positionally writes each physical frame, issues one
`sync_data`, then completes every receipt in that physical batch. Callers
publish through a separate FIFO queue: waiting for any ticket waits and
publishes every predecessor first. This is already a sound coalesced group
commit, but the one worker cannot accept or physically prepare the next prefix
while it is blocked in the current flush, and the publication waiter combines
durability wait, predecessor drain, state publication, and response release in
one observed tail.

The measured difference is not permission to weaken FIFO semantics. Commit
sequences, journal hashes, composite views, command outcomes, read-after-commit
fences, changelog observations, and the public application frontier form one
gap-free prefix. An acknowledged command must be both durably recoverable and
immediately visible to an authorized read using the returned commit sequence.

## Proposed Decision

### 1. Durability and publication remain monotonic prefixes

The journal append sequencer remains the sole authority for physical position,
generation, predecessor hash, and logical frame order. A durability operation
captures one exact journal tail and can prove at most that complete prefix. A
successful later-tail durability operation proves every earlier written frame
in the same generation, but public publication still advances only through the
contiguous validated prefix.

An internal fence may complete out of submission order. Its receipt is not
application authority. The ordered publisher buffers bounded completion facts
and advances the composite read view, transient indexes, durable read frontier,
journal published frontier, changelog observation, and response eligibility in
the original frame order. It never exposes a hole or lets a later command
bypass an unpublished predecessor.

Acknowledgement requires all of the following for the command's frame:

1. one known-successful durability operation covers the frame and every byte
   required for its recovery prefix;
2. every predecessor frame has passed the same validation and has been
   published;
3. the exact successor composite view and application frontier are installed;
4. the command's persisted outcome, events, provenance, audit, outbox intent,
   and changelog observation are released under their existing rules; and
5. an immediate read using the returned commit sequence can observe at least
   that published frontier.

The design does not introduce independent per-command durability, out-of-order
commit visibility, partition-local public frontiers, or an acknowledgement
that merely means "the caller's syscall returned."

### 2. Mechanics and a closed ledger precede production activation

WP-649 first adds fixed-cardinality timestamps and counters for:

- command/group enqueue and oldest-command residence;
- journal worker dequeue and physical-batch selection;
- extent-frame encoding and positional write;
- durability syscall start and completion;
- receipt-ready to ordered-publisher acquisition;
- predecessor publication drain;
- composite/frontier/transient publication; and
- publication to application acknowledgement.

The stages must sum to client-observed command latency within five percent for
one representative unary write and must reconcile physical groups, commands,
bytes, and receipts exactly for interactive c32 and seed. Evidence carries no
application value, tenant, command identity, path, or unbounded label.

A same-filesystem mechanics probe then compares, for the production frame-size
range and depths one, two, and four:

- the current write-then-fence worker;
- ordered positional writes overlapped with one in-progress fence;
- bounded concurrent same-file durability calls on cloned handles; and
- a separate-file concurrent control that distinguishes device capability from
  same-inode serialization.

The probe runs on the workstation, N1, and E2 profiles. Concurrent same-file
fences remain rejected unless they improve p95 prefix-completion latency by at
least twenty percent on both cloud profiles without reducing completed bytes or
groups per second by more than five percent. A filesystem that serializes or
merely coalesces the calls is recorded as a falsification, not hidden by an
interactive benchmark.

### 3. Group residence is bounded without adding an idle wait

The journal lane adds no fixed batching sleep. A ready submission is eligible
for immediate append, as it is today. If evidence shows that pre-submit group
formation contributes materially to the failing tail, the sequencer may seal a
physical prefix when the oldest selected command reaches a first-party latency
budget no greater than `PERF-015`'s existing two-millisecond unpublished-epoch
budget. The budget begins at command enqueue, not when a timer happens to be
armed, and therefore caps residence rather than adding a collection window.

The budget, maximum transitions, maximum logical bytes, maximum physical bytes,
and maximum in-flight prefixes are closed process constants. Applications and
operators cannot select them. Queue pressure may cause smaller prefixes; it may
not defer an idle command merely to seek a sibling. If smaller prefixes make
the public throughput, seed, or fence-byte tail gates worse, this branch is not
activated.

### 4. Append preparation may overlap a bounded durability operation

After the append sequencer has written a complete generation- and position-
bound prefix and captured its exact tail, a durability worker may fence that
prefix while the sequencer encodes and writes the next bounded prefix at later
positions. Later bytes are writer-private and receive no durability credit from
an earlier receipt even if the device incidentally flushes them. They require a
subsequent successful durability operation whose captured tail covers them.

At most four durability operations or the existing unpublished transition and
byte ceilings may be in flight, whichever is reached first. The sequencer,
durability workers, completion buffer, and publication queue apply bounded
backpressure before retaining additional frames. Extent recycle, checkpoint
rebase, backup, maintenance, administration, migration, shutdown, and hardened-
profile barriers drain all in-flight operations before proceeding.

Multiple simultaneous same-file durability calls are an optional mechanics-
gated implementation of this pipeline, not a required mechanism. A single
durability worker with append preparation overlap is valid if it produces the
winning evidence. If neither overlap shape meets the predeclared public gates,
the current worker remains active.

### 5. Errors remain prefix-wide and fail closed

Any positional-write, durability, completion-channel, prefix-proof,
publication-order, composite-view, or frontier mismatch fences authoritative
writes and releases no affected command result or effect. A failed or unknown
earlier fence cannot be bypassed by a later successful fence. Same-identity
recovery resolves each command under the existing unknown-status contract.

On shutdown or cancellation, accepted frames are either drained through known
durability and ordered publication or left unacknowledged for recovery. Worker
panic, channel disconnect, queue poison, missing completion, duplicate
completion, out-of-range tail, stale generation, and reordered completion are
explicit closed states. No timeout fabricates failure or success for a syscall
whose durable outcome is unknown.

### 6. Activation is sized against all remaining alpha gates

Before production dispatch changes, WP-649 freezes per-host queueing arithmetic
from the closed ledger. The candidate must predict and then demonstrate on both
N1 and E2:

- every representative unary scenario at most 1.10x same-run safe-application
  PostgreSQL;
- mixed c32 throughput at least 0.90x;
- mixed c32 p95 at most 1.25x;
- the full seed at most 5.0x;
- no read-throughput or read-tail regression above five percent; and
- exact correctness, durability, uncertainty, recovery, backup, shutdown, and
  evidence closure.

WP-644's bounded session is rerun unchanged as the acceptance consumer. It may
be proposed for release activation only if its original ADR-0127 activation
set and these gates all pass. This ADR does not itself amend `PERF-018`, change
the generated-client default, or accept the session comparator.

## Options Considered

1. **Tune a longer group-formation window:** rejected. It adds latency to idle
   and lightly loaded commands and repeats the timer-window failure already
   measured by WP-376.
2. **Acknowledge each command after its own syscall:** rejected. It can expose a
   hole in the application frontier and break immediate read-after-commit.
3. **Publish completed frames out of order:** rejected. Composite views,
   journal hashes, changelog frames, and recovery all require one gap-free
   prefix.
4. **Unbounded asynchronous fsync workers:** rejected. It converts latency into
   unbounded memory, dirty pages, uncertainty, and shutdown work.
5. **Bounded append/fence overlap with ordered durable-prefix publication:**
   proposed because it attacks the measured queueing tail while retaining the
   existing semantic frontier.

## Consequences

- The journal runtime may gain separate append-sequencing, durability, and
  ordered-publication states with bounded channels and explicit prefix proofs.
- More than one unpublished physical prefix may exist, but the existing
  transition, logical-byte, physical-byte, overlay-memory, and extent-capacity
  ceilings continue to dominate the sum.
- Concurrent fence calls are not promised; the mechanics probe may reject them
  while retaining append/fence overlap or the current implementation.
- Tail latency becomes a first-class production invariant and evidence field,
  not a batching heuristic.
- Read fast paths and projection result-set work remain separate packages.

## Compatibility

The decision changes no public gRPC, MCP, CLI, generated SDK, contract grammar,
RiffQL, executable IR, bundle/module/plan hash, capability, durable record,
journal frame, extent header, backup, export, changelog, or replication format.
The journal physical bytes and recovered authoritative state remain identical
for identical command facts. It is an internal scheduling, receipt, and
publication-pipeline change.

Because `PERF-018` currently freezes unary transport/client shapes, a successful
session rerun still requires ADR-0127's separately receipted comparator
amendment before session-shaped release evidence can qualify.

## Security

The pipeline handles only already-checked internal frames and fixed redacted
metadata. It gains no authentication, authorization, row-policy, storage-
selection, sequence, or public transaction surface. Current authorization and
row-policy checks remain per operation. Queue and timing telemetry has fixed
labels and cannot reveal tenant activity, values, command names, or durable
identities. Failure remains fail closed and cannot be selected or weakened by
an application principal.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** applications continue to invoke
  only compiled commands and cannot choose grouping, residence budgets,
  in-flight depth, durability workers, publication order, acknowledgement,
  transaction scope, or uncertainty behavior. Every acknowledged write retains
  atomic state/outcome/event/provenance, immediate read-after-commit, and exact
  recovery.
- **Scale:** channels, workers, in-flight prefixes, completion facts, retained
  bytes, dirty bytes, extent positions, and telemetry are bounded by fixed
  process and existing journal ceilings. The design retains no history-sized
  queue or full-state copy and does not prevent a future conflict-domain or
  partition-local physical implementation beneath the one public frontier.

## Testing

- Same-filesystem mechanics probes at depths one, two, and four on workstation,
  N1, and E2, including same-file and separate-file controls.
- Deterministic schedules for every write/fence completion order, queue-full
  boundary, publisher race, cancellation, shutdown, and worker failure.
- Simulation and process crash arms before and after positional write, stale-
  tail seal, each durability submission/completion, durable-prefix advance,
  each predecessor publication, frontier installation, acknowledgement, extent
  recycle, checkpoint rebase, and shutdown drain.
- Invariants proving one physical position owner, gap-free frame/hash order,
  monotonic durable prefix, monotonic published prefix not exceeding durability,
  exact read-after-commit visibility, and no result release above publication.
- Byte-identical journal, recovery, backup, export, changelog, and replication
  fixtures for identical authoritative facts.
- Paired current-HEAD N1/E2 unary, mixed c32, seed, read, writer-stage, physical-
  group, byte, p50, p95, and p99 receipts against safe-application PostgreSQL.

## Requirements and Work Packages

- **Requirements:** `PERF-004`, `PERF-005`, `PERF-008`, `PERF-009`, `PERF-015`,
  `PERF-017`, `PERF-018`, `REC-001`, `REC-002`, `STO-001`, `STO-002`
- **Defines or blocks:** `WP-649`
- **Final evidence:** `WP-649`, followed by the complete `PERF-018` release
  matrix

## Decision Deadline

Exact human acceptance is required before WP-649 changes journal worker count,
append/fence overlap, completion ordering, publication, public frontier advance,
or acknowledgement. Reject-first telemetry and mechanics probes may merge
before acceptance because they change no authoritative behavior.
