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

WP-649's current-HEAD ledger localizes the cloud tail. At interactive c32 the
N1 mean group residence is 4.82 ms, journal queue is 0.16 ms, `fdatasync` is
1.30 ms, durable-receipt-to-publication is 4.04 ms, and actual ordered
publication work is 0.16 ms. E2 measures 3.20/0.15/1.16/2.61/0.12 ms for the
same stages. Group residence is oldest-selected-command age at dispatch: it
mostly means waiting behind the busy writer, not time spent inside a batching
timer. The durable-to-publication wait occurs because the same writer that
submitted an earlier fence is performing deterministic work for a successor
when the earlier receipt becomes ready; it polls and publishes the predecessor
only after returning to its loop edge.

The same-filesystem mechanics gate rejected concurrent same-file durability
and append/fence worker overlap. Their closest cloud result improved p95 by
13% while losing 15% throughput, short of the predeclared 20%/5% thresholds.
An isolated diagnostic also disabled the existing completion-edge collection
window: N1 throughput fell 1.9% while p95 rose 5.0%, and E2 throughput fell
3.4% while p95 rose 3.2%. Therefore neither more fence syscalls nor removal of
the collection window is the accepted candidate. The remaining evidence-sized
candidate is separating ordered completion from authoritative apply work.

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

### 3. Existing collection semantics remain unchanged

The journal lane adds no new batching sleep. A ready submission remains
eligible for immediate append. The existing completion-edge collection window
remains enabled because the isolated no-coalescing diagnostic made throughput,
aggregate p95, and CreateComment p50 worse on both cloud profiles while leaving
oldest-command residence effectively unchanged.

The accepted candidate does not tune a duration, expose a knob, shrink groups,
or reinterpret the existing two-millisecond bound. Group-residence telemetry is
retained as queueing evidence and is named as residence, not formation time.
Any later change to collection policy requires a separate predeclared A/B and
exact amendment.

### 4. Ordered completion is separated from authoritative apply work

The sole apply writer retains command evaluation, transaction-current
revalidation, conflict ownership, sequence assignment, mutation application,
journal-frame submission, and the private successor frontier. After submission
it transfers one `SubmittedWriterUnit` into a bounded FIFO completion lane. The
transfer contains an opaque checked fence, exact FIFO metadata, fixed telemetry
facts, and response senders; it confers no storage transaction, sequence,
conflict, policy, or mutation authority.

The completion lane owns only these ordered actions:

1. wait for or poll the oldest submitted unit's durability result;
2. validate that result against its exact covered frame and captured tail;
3. publish through the existing storage publication queue, which itself drains
   every predecessor and advances only a contiguous durable prefix;
4. publish first-commit notifications after storage publication;
5. record terminal telemetry; and
6. release the corresponding application or administration responses.

It never evaluates a command, chooses a group, assigns a sequence, opens a
write transaction, changes a mutation, skips a predecessor, or publishes a
later unit first. Replay/no-write `AfterPredecessor` units and submitted service
audit units use the same FIFO so the shared application/administration order is
unchanged. The apply writer may prepare a successor while completion publishes
the predecessor, but it cannot observe completion facts as private-frontier
authority or reuse them to admit work.

The FIFO is bounded by the existing unpublished transition, logical-byte,
physical-byte, read-root, journal-suffix, and channel ceilings. A full lane
backpressures the apply writer before accepting another private successor. A
unit that requires pipeline drain inserts an ordered barrier and blocks new
apply work until completion acknowledges the drained prefix. Checkpoint rebase,
extent recycle, backup, maintenance, migration, hardened-profile barriers,
shutdown, and cancellation use the same drain protocol.

Before production activation, the mechanics build must prove that every opaque
command and audit fence crossing the lane is `Send` without unsafe code or
duplicating its read root, and that the completion owner holds no authoritative
operational port. If the existing checked-fence interface cannot meet that
bound, the candidate is rejected; the implementation may not replace it with
raw receipts or an unchecked publication callback.

The WP-649 compile-only probe has proven the first half: adding `Send` as a
supertrait of the two existing checked-fence interfaces and asserting the
complete `SubmittedWriterUnit: Send` passes Rust 1.97 checks for storage API,
redb storage, and commit crates with all features. Production implementation
must preserve that exact typed boundary and add the architecture test excluding
operational ports from the completion owner.

### 5. Physical journal scheduling remains unchanged

The accepted candidate retains one ordered append sequencer, current ready-
drain coalescing, one same-file durability call at a time, exact generation and
position binding, and the existing captured-tail proof. The rejected concurrent
same-file and append/fence mechanics remain documented evidence only. No later
bytes receive durability credit from an earlier fence, and no additional dirty-
byte or uncertain-syscall budget is introduced.

### 6. Errors remain prefix-wide and fail closed

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

### 7. Activation is sized against all remaining alpha gates

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
5. **Bounded append/fence overlap:** rejected by the workstation/N1/E2 mechanics
   gate. The closest cloud cell traded 15% throughput for only 11--13% p95
   improvement.
6. **Remove completion-edge coalescing:** rejected by the N1/E2 public A/B. It
   reduced throughput and worsened both aggregate and write tails.
7. **Separate ordered completion from authoritative apply:** proposed because
   it attacks the measured 2.49--3.88 ms receipt-ready wait above actual
   publication work while preserving the same physical journal and one public
   frontier.

## Consequences

- The commit runtime may gain separate authoritative-apply and ordered-
  completion owners with one bounded FIFO and explicit prefix proofs.
- More than one unpublished physical prefix may already exist under the current
  writer pipeline; the existing transition, logical-byte, physical-byte, read-
  root, overlay-memory, and extent-capacity ceilings continue to dominate the
  sum.
- Concurrent fence calls, append/fence overlap, and removal of completion-edge
  coalescing are rejected by evidence and are not part of the implementation.
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
