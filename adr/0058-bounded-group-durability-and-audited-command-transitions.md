# ADR-0058: Bounded Group Durability and Audited Command Transitions

- **Status:** Accepted
- **Date:** 2026-07-29
- **Decision owners:** RiffDB maintainers
- **Related requirements:** `AUD-003`, `IDEM-001`, `PERF-003`, `PERF-004`,
  `REC-002`, `TXN-004`, `TXN-005`
- **Related work packages:** `WP-362`, `WP-364`
- **Amends:** ADR-0005, ADR-0007, ADR-0023, ADR-0057
- **Amended by:** ADR-0059, ADR-0060, ADR-0096, ADR-0097

## Context

WP-362 removed an accidental full-history service-audit scan from every append.
That restored flat command throughput as retained history grows, but one
successful public command still ordinarily pays four independent redb
`Immediate` commits:

1. the service `Started` audit row;
2. the durable `Pending` admission;
3. the authoritative command graph; and
4. the terminal service-audit row.

The individual transitions are semantically correct. Their physical separation
prevents redb from amortizing its durable two-phase commit cost across concurrent
commands and makes the public batch workflow a collection of unrelated fsync
cycles. Replacing redb is not justified by this evidence: redb already exposes
the atomic multi-command transaction needed for bounded group commit.

ADR-0007 intentionally required `Started` before executor-capacity acquisition
and a terminal audit only after actual response filtering. ADR-0023 therefore
kept production on synchronous durability pending a separately accepted
scheduling, crash, fairness, and acknowledgement decision. This record is that
decision. It changes the physical transaction layout without weakening
authorization, idempotency, audit, command atomicity, or acknowledgement
semantics.

## Decision

### One typed global durable-write scheduler

The production process MUST have one commit-owned scheduler for every online
authoritative redb write. Storage remains a semantic port and MUST NOT receive
arbitrary transaction callbacks.

The scheduler:

- admits work in FIFO order;
- waits for at most 200 microseconds to form a compatible group;
- selects at most 16 transitions and MUST also honor the existing 64-command
  and 16 MiB storage transaction ceilings;
- may group only transitions accepted by a closed, typed compatibility rule;
- MUST commit the oldest incompatible item alone rather than allow it to starve;
- assigns every sequence in scheduler order within its independent sequence
  domain; and
- uses redb `Immediate` two-phase durability for the physical transaction.

The 200 microsecond delay is an upper bound, not a mandatory sleep. The
scheduler MUST dispatch immediately when a bound is reached, when no compatible
work is already available, or when shutdown/fencing requires resolution.

ADR-0060 supersedes the ambiguous “when no compatible work is already
available” clause above. Queue emptiness at an intermediate poll does not
require dispatch: after receiving the oldest groupable transition, the
scheduler may wait until that transition's bounded window deadline for
compatible work. ADR-0060 also defines the ordering barriers that may not be
bypassed while a group is formed.

`DurabilityMode::Group` means that a command shared one physical durable commit
with zero or more independently identified commands. It does not weaken the
durable acknowledgement point. `DurabilityMode::Sync` remains an explicit
conformance and recovery oracle; it is not the production default.
`DurabilityMode::Memory` remains test-only.

### Audited command admission is one transition

For an application command, the fresh current-policy authorization check moves
before synchronous writer admission. The first authoritative transition then
atomically:

- appends the invocation's `Started` service-audit row; and
- creates the exact durable `Pending` admission when the idempotency identity is
  absent.

If the identity already names `Pending`, `ExecutionFailed`, or a terminal
outcome, the same transition appends `Started` and returns that state without
creating another admission. The deterministic runtime MUST NOT start until the
combined transition is durably known committed.

This is a narrow amendment to ADR-0007: `Started` still precedes runtime and all
command effects, but it no longer precedes the scheduler's bounded capacity
wait. Cancellation or denial before combined admission is represented by one
standalone terminal audit row. Cancellation after admission preserves the
existing terminal-cancellation behavior, and a durable `Pending` remains
recoverable under ADR-0005.

### Successful command completion is one transition

For a newly committed outcome, the second authoritative transition atomically:

- applies mutations;
- stores the terminal outcome;
- appends durable events and outbox intent;
- stores provenance;
- stores the commit record;
- removes `Pending`; and
- appends the invocation's terminal `Succeeded` service-audit row linked to the
  exact command and commit sequence.

The service passes a private, non-serializable complete-outcome release proof to
the coordinator. Only the compiler/catalog and policy layers may construct this
proof. It proves that:

- the selected command declares a complete finite outcome schema;
- the current authorization permits that complete schema;
- no selected result field requires runtime redaction; and
- every possible encoded result fits the service response bound.

The coordinator still validates the actual declared outcome, canonical values,
and durable bounds before staging it. After the combined commit, response
rendering is mechanical and cannot discover a new policy or size failure.
Transport loss after commit is ordinary idempotent uncertainty, not a reason to
change the durable succeeded audit.

Failed execution, denial, cancellation, replay, or any path lacking a complete
release proof uses the existing explicit typed transition and standalone audit
rules. There is no synthetic succeeded audit and no response release before its
required terminal audit is durable.

### Independent command identity inside a group

Every grouped item retains its own:

- idempotency identity and request hash;
- command and commit sequences;
- outcome and declared business result;
- provenance and event/outbox identities;
- audit lifecycle and link;
- retryability classification; and
- acknowledgement result.

A group is not a transaction exposed to applications. One caller cannot make
another caller's command part of its atomic business operation.

Pre-commit failure proves the whole physical group absent. A post-commit unknown
result MUST fence new writes. Recovery resolves every selected item
independently through its original identity and returns a result only after all
items have a proven terminal classification. The process MUST NOT infer that
one item's presence proves another item's presence.

### Public batch commands

The existing public batch-command workflow remains a bounded collection of
ordinary symbolic command invocations. It MUST use the same application
service, policy, coordinator, and scheduler as unary calls. Generated Rust and
TypeScript bindings MAY provide concurrency, progress, checkpoint, and typed
per-item outcome helpers, but MUST NOT expose a bulk mutation escape hatch or
claim whole-batch atomicity.

### Prohibited shortcuts

This decision does not permit:

- changing redb's production commit durability away from `Immediate`;
- acknowledging before the physical durable commit returns;
- direct service, gRPC, CLI, MCP, SDK, or batch access to redb;
- arbitrary storage transaction callbacks;
- a second authoritative writer or command-sequence allocator;
- cross-command business transactions;
- omitting `Started`, terminal audit, `Pending`, provenance, outcome, event,
  outbox, or commit records;
- weakening current-policy authorization or response redaction; or
- treating a group-wide unknown result as one shared idempotency outcome.

## Consequences

- Concurrent unary and public-batch commands can amortize durable engine work
  without changing their application semantics.
- A normal newly executed command requires two durable transitions rather than
  four.
- Low-concurrency latency may include up to 200 microseconds of scheduler wait.
- Audit and admission storage APIs need compound typed transitions.
- The coordinator becomes responsible for fairness, grouping, fencing, and
  per-item uncertainty resolution, requiring deterministic schedule and
  process-crash coverage.
- The existing synchronous path remains valuable as a semantic oracle and
  recovery test configuration.

## Rejected alternatives

- **Replace redb before measuring grouped writes.** The observed remaining cost
  follows the number of durable transitions, not evidence of an unsuitable
  storage engine.
- **Use redb `Eventual` durability.** This changes acknowledgement semantics and
  violates the durable uncertainty contract.
- **Expose one atomic bulk mutation API.** This bypasses ordinary commands and
  introduces cross-item transaction semantics.
- **Allow the service to submit storage closures.** This dissolves semantic
  storage and coordinator ownership boundaries.
- **Drop or asynchronously append audit records.** This weakens fail-closed
  behavior and makes acknowledged results disagree with their audit history.

## Acceptance reference

The human maintainer explicitly accepted this exact semantic amendment in the
current Codex session on 2026-07-29 after its transaction, authorization,
release-proof, cancellation, uncertainty, and redb durability rules were
presented.
