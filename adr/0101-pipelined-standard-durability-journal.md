# ADR-0101: Pipelined Standard-Profile Durability Journal

- **Status:** Accepted
- **Date:** 2026-08-06
- **Decision owners:** RiffDB maintainers
- **Related requirements:** `PERF-004`, `PERF-005`, `PERF-007`, `PERF-009`,
  `PERF-010`, `PERF-015`, `REC-001`, `REC-002`, `TXN-041`, `TXN-042`,
  `TXN-043`, `TXN-044`
- **Related work package:** `WP-478`
- **Amends:** ADR-0005, ADR-0058, ADR-0060, ADR-0061, ADR-0098

## Context

The retained 32-client safe-application comparison reaches about 32,380
operations per second in PostgreSQL and 16,077 through RiffDB's public gRPC
application path. RiffDB reads have the lower median, but mixed-workload command
latency is about 11 milliseconds versus PostgreSQL's 4 milliseconds. Server
telemetry attributes roughly 7.2 of 9.6 writer-busy seconds to 1,419 durable
redb fences. Command admission, compatibility, evaluation, encoding, staging,
and publication are not large enough to explain or close the gap.

redb 4.1's standard one-phase `Immediate` commit performs one `sync_data`.
Replacing it with a sequential sidecar append and `sync_data` does not improve
the same-host mechanics: both take about five milliseconds per fence. Reducing
the number of tables touched saves only 3--7 percent in a representative
mechanics probe. The storage device's synchronous-flush latency is the floor.

A bounded probe that let the sole ordered writer apply complete redb
`Durability::None` groups while a separate journal lane grouped already encoded
frames behind durable flushes completed 1,000 representative groups in
0.14--0.41 seconds instead of 5.0--5.4 seconds. It required 27--76 durable
journal flushes rather than 1,000. This is the first measured mechanism with
enough headroom to change the application comparison materially.

The journal cannot be an optimization whose loss is repaired from redb: an
acknowledged command must survive a crash even when its materialized redb root
has not been checkpointed. It is therefore an authoritative local durability
boundary, while redb remains the authoritative materialized state at a known
checkpoint plus the exact journal suffix.

## Decision

### 1. Standard-profile authority is checkpoint plus journal suffix

For the standard application durability profile, authoritative state is:

1. the newest known-durable redb checkpoint; and
2. the exact, gap-free, checksummed sequence of durable command frames after
   that checkpoint.

The combination is one logical database. Neither component alone may be
presented as complete. The hardened profile remains one redb two-phase
`Immediate` commit per existing physical group and does not use the journal.

An idle singleton with no open journal epoch retains the existing direct
one-phase redb `Immediate` path. Before that direct commit, every prior journal
frame must be known durable. The direct commit checkpoints the prior suffix and
the singleton together; journal reclamation may occur only after that redb
checkpoint is known successful. A crash at any reclamation edge must leave
either the old checkpoint plus suffix or the newer checkpoint sufficient for
complete recovery.

### 2. Frames contain complete checked command graphs

One journal frame contains one or more complete ordinary command subgroups in
FIFO sequence order. A frame stores the canonical bytes necessary to replay
every entity, index, generation, terminal outcome, event, route, outbox,
provenance, commit, and linked service-audit mutation without rerunning command
logic. It also stores:

- format version and database incarnation;
- predecessor and covered commit-sequence frontier;
- command and byte counts;
- a hash of the preceding durable frame;
- a checksum over the complete header and payload; and
- a length-delimited footer that makes a torn tail distinguishable from a
  complete frame.

Frame construction consumes only complete storage-validated command graphs.
Recovery performs the same envelope, key, canonical-byte, semantic-bound,
cross-record, sequence, hash-chain, and database-incarnation checks before
replay. Unknown versions, gaps, reordered frames, checksum failures, valid-byte
substitution, cross-database frames, or nonreciprocal records fail closed.

### 3. Bounded pipelined group durability

The sole coordinator remains the only sequence allocator and the sole ordered
authoritative apply lane. Under contention it may:

1. apply a complete subgroup through redb `Durability::None`;
2. retain the exact post-subgroup redb read snapshot, command results, transient
   deltas, and encoded journal frame;
3. submit the immutable frame to the dedicated journal lane; and
4. continue applying later FIFO subgroups while an earlier journal flush is in
   progress.

The journal lane drains only the ready FIFO prefix before beginning one
`sync_data`. It never waits for a configurable batching delay. Commands that
arrive during that flush form the next group. One durable flush may cover at
most 256 commands and 16 MiB of encoded frame bytes. At most 256 commands and
16 MiB may be applied but unpublished process-wide. Queue and transport bounds
remain independent and unchanged.

No control-plane, administration, lifecycle, shutdown, backup, contract,
capability, or query-module barrier may cross an open journal epoch. Such a
barrier first drains the journal lane and checkpoints or safely retains the
durable suffix.

### 4. Publication follows the journal fence

The read snapshot retained after the final subgroup covered by one successful
journal flush becomes the new last-durable frontier. Storage atomically
publishes that snapshot before releasing any covered outcome, notification,
transient index delta, projection work, outbox work, subscription update, or
response. Older covered snapshots are discarded only after the successor is
installed.

Readers continue to see either the predecessor durable snapshot or one complete
successor journal frontier. Applied-but-unflushed redb roots are writer-private.
One caller cannot observe or transact with a sibling command merely because
their frames share a flush.

### 5. Failure, uncertainty, and recovery

A journal append or flush error fences authoritative writes, retains the prior
published frontier, releases no affected result, and invalidates unpublished
transient state. Unknown flush status is ordinary commit uncertainty: after
restart, exact frame validation and command identity determine committed versus
absent. A fully durable command whose response was lost replays its persisted
outcome under the existing idempotency contract.

On restart, redb opens at its last `Immediate` checkpoint. Recovery then scans
the journal from the checkpoint frontier, ignores only an incomplete terminal
tail, validates every complete frame, replays the suffix through ordinary
storage validation, and performs one redb checkpoint before operational
handoff. Frames at or below an already advanced checkpoint must match that
checkpoint's recorded journal frontier before reclamation; they are never
blindly replayed or silently skipped.

Process tests kill before append, during frame write, during `sync_data`, after
flush before snapshot publication, after publication before response, during
checkpoint, and during reclamation. Every restart must produce the preceding
frontier or the complete next durable prefix, never a partial command graph or
a visible unfenced subgroup.

### 6. Retention, backup, and replication boundaries

Normal history retention does not remove an unreclaimed recovery frame. Backup
either drains and checkpoints the journal first or captures the exact
checkpoint plus suffix as one verified unit. Restore validates and replays that
unit before readiness.

The local journal is not a public CDC stream or a replication protocol. A
future replication frame may be derived only from published durable journal
frontiers and remains governed by its own accepted ADR.

## Consequences

- Durable flushes are grouped without weakening acknowledgement durability.
- redb remains the materialized storage engine and retains MVCC read behavior.
- Standard-profile recovery gains a versioned append-only durable component and
  must validate two coordinated frontiers.
- Busy writes no longer force the sole apply lane to sit idle during every
  storage-device flush.
- Idle singleton latency and the hardened recovery oracle retain their current
  paths.
- The journal is first-party Rust and introduces no unsafe code, native
  dependency, network effect, or target-language semantic implementation.

## Rejected alternatives

- **Raise batching windows.** Closed-loop writers cannot supply enough work
  without adding latency, and current coalescing already consumes its accepted
  bound.
- **Sequential sidecar WAL.** It pays the same synchronous flush per group and
  measured no improvement.
- **Only consolidate redb tables.** It reduces mechanics by single-digit
  percentages while leaving the flush floor intact.
- **Acknowledge `Durability::None`.** A crash can erase acknowledged work and is
  prohibited.
- **Expose applied roots before journal durability.** Readers could observe a
  command that recovery later removes.
- **Use the journal in the hardened profile.** The independent two-phase oracle
  remains deliberately simple.
- **Replace redb.** The measured problem is synchronous pipeline structure, not
  redb's correctness or read engine.

## Acceptance

The maintainer approved pursuing the large write-path mechanism after the
retained comparison showed RiffDB at about half of PostgreSQL safe-app
throughput and small CPU/table candidates could not close the gap. Production
enablement remains gated on the crash matrix, exact semantic suites, c1
guardrail, and c32/c128 public comparison in `WP-478`.
