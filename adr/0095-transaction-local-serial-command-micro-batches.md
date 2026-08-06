# ADR-0095: Transaction-Local Serial Command Micro-Batches

- **Status:** Accepted
- **Date:** 2026-08-05
- **Decision owners:** RiffDB maintainers
- **Related requirements:** `TXN-001`, `TXN-004`, `TXN-013`, `PERF-004`,
  `PERF-005`, `PERF-008`, `PERF-009`, `PERF-010`
- **Related work packages:** `WP-453`, `WP-454`
- **Amends:** ADR-0004, ADR-0058, ADR-0059, ADR-0060, ADR-0061,
  ADR-0094
- **Amended by:** ADR-0096, ADR-0098

## Context

ADR-0094 safely permits commands sharing an aggregate conflict key to share one
physical commit when the compiler proves that they are commutative child
appends. This materially improves that narrow case, but mixed application load
still leaves the sole writer saturated while the modal physical group is one
command. Commands that mutate an existing row, enforce a requirement, or
otherwise cannot carry the commutative proof split the queue even though their
ordinary FIFO execution is valid.

Fresh post-WP-452 evidence records 32-client RiffDB throughput of 7,495
operations per second against PostgreSQL minimal at 42,843 operations per
second. At 128 clients RiffDB reaches 8,622 operations per second and remains
writer saturated. The writer spends most of the run in physical commit/flush,
while conflict-key splits and singleton groups prevent sufficient amortization.
Compatible same-snapshot grouping alone therefore cannot satisfy `PERF-008`.

The storage transaction protocol accepted by ADR-0004 already supports up to 64
independent command graphs in one transaction, consecutive sequence assignment,
transaction-current validation including earlier staged writes, and one atomic
durable commit. The missing decision is how production may evaluate overlapping
commands against that private staged state without exposing a cross-command
transaction or weakening any command boundary.

## Decision

The sole FIFO commit coordinator MAY execute a bounded prefix of ordinary
commands as one **transaction-local serial command micro-batch**. Each command
is evaluated and staged in ingress order against the private transaction state
produced by earlier commands in that same micro-batch. The resulting history
MUST be observationally equivalent to executing and durably committing those
commands separately in that FIFO order.

This is a physical durability optimization only. It does not create an
application transaction, a public batch atomicity guarantee, a command-combining
primitive, or a way for one caller to control another caller's command.

### Narrow transaction-local execution protocol

Storage exposes a consuming, typed serial-batch protocol rather than a generic
transaction callback or key/value handle. The coordinator drives this closed
sequence for each candidate:

1. derive the exact compiler-owned bounded `SnapshotRequest` from the selected
   immutable plan and canonical input;
2. read that snapshot from the current write transaction, including every
   earlier staged command in the micro-batch;
3. materialize and validate the snapshot through the ordinary catalog-owned
   schema path;
4. run the ordinary deterministic runtime with the invocation's fixed
   transaction context;
5. freshly authorize and recheck request control;
6. recheck the exact idempotency identity, input, plan, capability facts,
   dependencies, invariants, bounds, and conflict authority against the same
   transaction-current state;
7. reserve capacity before assigning the next sequence; and
8. stage the complete command graph and linked audit transition before
   considering the next candidate.

The transaction-local snapshot surface accepts only existing bounded semantic
snapshot requests. It exposes no engine transaction, arbitrary read, arbitrary
write, callback, SQL, raw record, or caller-selected sequence. Deterministic
runtime execution remains first-party Rust and retains every no-I/O,
no-operating-system-clock, no-untracked-randomness, and bounded-execution rule.

Later commands observe all earlier staged writes that match their declared
snapshot dependencies, including exact entities, unique lookups, root
validations, and bounded index ranges. They cannot inspect undeclared state.
Every influential observation is retained and checked through the existing
transaction-current validation path.

### Eligibility and ordering

The production serial path is initially limited to newly executed synchronous
commands whose complete request, plan, snapshot, deterministic execution,
response-release proof, and authoritative record graph fit the existing
bounded application-command path. Legacy `Pending` resumes, historical replays,
execution-failure recovery, control-plane work, and unsupported snapshot shapes
use their existing paths until an accepted amendment and equivalent recovery
tests add them.

Selection is a contiguous FIFO prefix. The coordinator MUST NOT overtake a
command to improve grouping. Catalog or query-module deployment, capability
creation or revocation, migration, maintenance, shutdown, fencing, readiness
changes, and every existing ADR-0060 hard barrier terminate selection.

Commands with disjoint conflict keys and ADR-0094-proved commutative child
appends MAY continue using parallel preparation and same-snapshot grouping.
Commands that share an unproved mutable conflict key MAY use the serial path
only while the sorted, deduplicated union of their compiler-derived conflict
capabilities remains held by one sealed group authority from first
transaction-local snapshot through commit, rollback, or uncertainty fencing.
No conflict capability is released between staged commands.

Within one micro-batch, application commit sequences and linked administration
audit sequences are assigned in FIFO staging order. A zero-mutation declared
business outcome remains an independent terminal command and receives the same
sequence treatment it would receive alone. Physical grouping MUST NOT merge or
renumber sequences.

### Independent command semantics

Every staged command retains its own:

- idempotency identity and canonical request hash;
- current-policy authorization and capability proof;
- deterministic transaction context and command plan identity;
- declared outcome and application commit sequence;
- mutations, events, outbox intent, provenance, and causation;
- `Started` and terminal service-audit lifecycle;
- response-release proof, retry classification, and acknowledgement; and
- same-key uncertainty and replay resolution.

A public batch remains a bounded collection of ordinary commands. It does not
gain caller-visible atomicity, rollback, transient-output references, or a
guarantee that all items share one physical commit. The statement in ADR-0059
that no batch item may observe another is narrowed: callers cannot request or
depend on a shared transaction, but an internally serially grouped later item
MAY observe an earlier item's staged state exactly as it would after a separate
FIFO commit.

### Bounds and prefix handling

One serial micro-batch retains the existing hard ceilings of 64 commands and
16 MiB of semantic and conservative encoded write-set capacity. The scheduler
also retains the existing oldest-item 200-microsecond maximum window, bounded
queue, request, snapshot, runtime, recursion, range, audit, event, outbox, and
response limits. No limit is converted into an unbounded retry or allocation.

If the next candidate cannot begin because a count or byte ceiling was reached,
the coordinator MAY commit the already staged nonempty prefix and retry that
candidate first in a later transaction. If a candidate is independently
ineligible for the serial protocol, the coordinator commits no later command
ahead of it; it may commit the valid prefix and then process the candidate on
its ordinary path.

A dependency, identity, policy, integrity, storage, or validation failure after
a candidate begins MUST either return the exact unchanged prior batch state or
abort the complete uncommitted transaction. No partial candidate may remain.
Storage and integrity failure abort the complete physical batch. A deterministic
business outcome is not such a failure; it is staged as that command's ordinary
terminal result.

### Cancellation and deadlines

Request control is checked before each candidate becomes staged. Cancellation
or deadline expiry detected before staging ends the serial prefix: an already
staged prefix may commit, the affected command follows its ordinary terminal
audit behavior, and no later command overtakes it.

After a command is completely staged, caller cancellation changes only response
interest. It cannot selectively remove that command from the physical
transaction or make later commands observe a state that is then rolled back.
The coordinator completes or rolls back the whole physical transaction and
uses ordinary same-key recovery before releasing any retained result.

### Durability, visibility, and recovery

The standard profile performs one redb `Immediate` one-phase commit for the
complete staged micro-batch; the hardened oracle performs its configured
two-phase commit. There is no chain of visible non-durable redb commits.

Before commit succeeds, staged state is private to the transaction: application
reads, command preparation outside this transaction, projections, outbox
workers, event consumers, and subscribers MUST NOT observe it. No response or
success acknowledgement is released before the complete physical commit is
known durable.

A proven pre-commit abort leaves every selected command absent. A successful
commit makes every staged command complete and durable. Recovery therefore
observes the physical batch as all complete or all absent, never as a durable
prefix, but it still validates and resolves every command by its own exact
identity and reciprocal record graph.

Unknown commit status fences authoritative writes. Resolution performs bounded
independent identity lookups for every staged command and MUST classify them all
before releasing any result or admitting new writes. The implementation MUST
NOT infer one command's outcome from another command's presence. Any mixed or
partial durable classification is an integrity failure and keeps the writer
fenced.

### Activation sequence

The serial path is not production-active merely because this ADR is accepted.
Implementation proceeds in this order:

1. add the closed semantic protocol and a memory-backend prototype;
2. prove deterministic schedule equivalence to separate FIFO commits for every
   supported overlap, including rejection, cancellation, capacity, and
   same-identity cases;
3. add process-level complete-or-absent and unknown-result recovery tests;
4. implement the redb protocol without a generic transaction escape hatch; and
5. activate bounded production selection only after the memory and redb suites
   pass unchanged command, audit, provenance, event, outbox, authorization, and
   recovery assertions.

Any unsupported shape fails closed to the existing ordinary command path. No
benchmark result may enable the serial path by bypassing these gates.

## Consequences

- Conflicting FIFO commands can amortize one physical flush without pretending
  that aggregate conflict keys commute.
- Later commands receive the same declared state they would see after earlier
  commands committed separately, preserving requirements and existing-row
  mutation semantics.
- The writer holds one transaction while bounded deterministic evaluation runs.
  This increases transaction duration but does not add a writer or block redb
  MVCC readers; telemetry must separate snapshot, evaluation, staging, and
  commit time.
- A physical crash boundary may cover several independent commands, while the
  public and durable identity model remains per command.
- The closed transaction-local snapshot protocol adds substantial type-state
  and recovery complexity and therefore requires the staged activation above.

## Rejected alternatives

- **Keep compatible-only grouping as the production endpoint.** Current
  saturation and modal singleton evidence cannot meet the unchanged alpha gate.
- **Treat all conflicts as commutative.** Conflict keys protect invariants and
  mutable state; this is unsound.
- **Chain non-durable redb commits behind one durable tail.** Intermediate state
  becomes visible and may roll back, violating ADR-0061.
- **Evaluate later commands against the pre-group snapshot.** Existing-row
  mutations and requirements would not be serially equivalent.
- **Expose a public multi-command transaction.** This weakens the command model,
  creates cross-command coupling, and is outside the POC architecture.
- **Add a storage callback or raw transaction handle.** It bypasses the semantic
  storage boundary and allows untracked reads, writes, and sequence assignment.
- **Replace redb.** The observed ceiling is dominated by insufficient physical
  commit amortization under conflict, not evidence that redb cannot provide the
  required transaction.

## Acceptance reference

On 2026-08-05 the maintainer explicitly accepted this record's exact
transaction-local snapshot, FIFO visibility, independent identity, capacity,
cancellation, durability, and recovery rules.
