# ADR-0060: Bounded Scheduler Windows, Activated Storage Lanes, and Parallel Preparation

- **Status:** Accepted
- **Date:** 2026-07-30
- **Decision owners:** RiffDB maintainers
- **Related requirements:** `PERF-004`, `PERF-005`, `PERF-006`, `PERF-007`,
  `PERF-008`, `TXN-004`, `TXN-005`
- **Related work packages:** `WP-367`, `WP-368`, `WP-369`
- **Amends:** ADR-0003, ADR-0004, ADR-0007, ADR-0023, ADR-0033, ADR-0058,
  ADR-0059
- **Amended by:** ADR-0096

## Context

WP-364 and WP-366 established two-transition audited commands, bounded public
batching, and compatible redb group durability. Production nevertheless does
not use the accepted 200-microsecond grouping window. The coordinator drains
only immediately available adjacent messages and stops at the first message of
another type. Its workload capacity equals one maximum command group, while a
separate idempotency inspection message consumes a coordinator turn before
every mutation. Replay audit writes are also reduced one transaction at a
time.

The activated redb port bundle is shared behind one process-wide mutex. That
mutex is held across admission commits and all operational read calls even
though redb supports concurrent MVCC readers. First-commit publication then
re-enters the storage bridge once per committed command to scan no-destination
outbox status.

After durable I/O is amortized, immutable plan/schema cloning, repeated
normalization and lookup, sizing-only Protobuf construction, redundant typed
verification, per-request named-query parsing, and serial deterministic
evaluation become the next bounded costs. We need to remove those costs
without creating a second writer, weakening current authorization, changing
durability, or allowing execution to escape compiler-declared dependencies.

## Decision

### A real bounded grouping window

After receiving the oldest groupable transition, the scheduler MAY wait until
that transition's 200-microsecond window deadline for compatible work even
when no compatible work is currently queued. Queue emptiness at an
intermediate poll MUST NOT by itself force dispatch.

The scheduler MUST dispatch when:

- a command-count or encoded-byte bound is reached;
- the oldest selected transition's window expires;
- a non-bypassable ordering barrier is encountered; or
- shutdown, fencing, or readiness failure requires resolution.

The scheduler MAY drain its complete currently available bounded receiver into
an actor-local ordered deque. It may retain at most 64 selected commands and
MUST preserve the relative order of every deferred message. Deferred messages
MUST be considered before messages received after that drain.

Capability creation or revocation, catalog or query-module deployment,
administrative authoritative writes, shutdown, fencing, and readiness
transitions are hard ordering barriers. A command MUST NOT overtake one of
those messages to fill a group. Read-only observations and idempotency
selection are removed from the authoritative writer lane where possible; any
remaining explicitly classified observational message may be deferred only
under a closed typed rule.

FIFO means stable ingress order within each typed lane plus the hard-barrier
rule above. It does not require a read-only observation to truncate an
otherwise compatible authoritative group. No item may starve, and bounded
fairness evidence MUST cover continuous load, barriers, cancellation, and
shutdown.

### Bounded admission selection

Historical idempotency selection remains necessary because a replay may name
an immutable historical command plan. It becomes a bounded read-only hint:

- one read transaction MAY inspect up to the public transport batch bound;
- the result exposes only the exact plan-selection evidence required by the
  application service;
- the service still validates and authorizes the selected exact plan; and
- authoritative admission MUST recheck the complete identity and canonical
  input inside its write transaction before creating or resolving `Pending`.

The hint never authorizes execution and never proves vacancy at admission.
Replays, pending resumes, and execution failures retain their original
identity and plan rules. Replay `Started` rows MUST use the same bounded
audited admission group path rather than one physical transaction per replay.

Coordinator admission capacity MAY exceed the 64-command transaction ceiling,
but it MUST have independent command-count and retained-byte bounds. Public
transport batches remain capped at 16 ordinary commands. Client concurrency
and server capacity are selected from measured bounded sweeps, not by exposing
an unbounded queue.

### Activated storage lanes

One completed activation MAY split the private redb operational bundle into:

- exactly one non-cloneable authoritative writer handle owned by the commit
  coordinator; and
- cloneable least-authority reader handles that can open independent redb read
  transactions.

The split is private server composition. It does not expose redb, storage
records, transaction callbacks, or an application bypass. The writer remains
the only owner of authoritative transaction sequencing, leases, mutation,
commit sequences, and acknowledgement.

Admission and completion commits MUST NOT hold an outer process-wide storage
mutex across redb transaction work or durable flush. MVCC readers MUST NOT
share a process-wide serialization gate. Startup, maintenance, and activation
proofs still fence every handle, and poisoning or contradictory state fails
authoritative readiness closed.

The no-destination composition seeds outbox readiness from complete startup
recovery. A newly committed outbox intent may update an exact process-local
degraded latch from commit metadata instead of rescanning storage. Because the
composition has no delivery transition, that latch cannot return to ready
during the generation. A future destination-bearing composition must use
exact transition-owned status updates or a durable summary; it may not infer
health from a lossy watermark.

### Immutable prepared artifacts

Validated contract bundles, command plans, schemas, and query programs MAY be
shared through immutable `Arc` ownership under their exact content
identities. Reuse MUST be keyed by complete lineage, version, bundle hash,
plan hash, module hash, and operation name as applicable. Pointer identity
alone is never authority.

Canonical command input may be materialized once for one exact immutable plan
and submitted request. After a capacity wait, the service MUST revalidate the
active/exact catalog identity and freshly authorize at every existing policy
safe point, but it need not clone, parse, or normalize identical immutable
material again. Response-release authorization and complete-outcome proof
rules remain unchanged.

Named deployed RiffQL may cache its parsed immutable document under the exact
contract/module/query identity. Ad-hoc source remains bounded and parsed per
submitted source.

Durable reservation sizing MAY use a non-materializing structural upper-bound
calculator. Final durable records SHOULD be encoded once and their exact
encoded charges reused. Every calculated reservation MUST dominate every
possible final sequence/version encoding, and byte-for-byte compatibility
fixtures remain authoritative. Untyped, recovered, migrated, or externally
supplied bytes retain full decode, canonicality, hash, and envelope checks.
Only sealed typed records created within the checked pipeline may omit a
redundant decode of bytes they just encoded.

### Bounded parallel preparation

The coordinator MAY pipeline already admitted commands through bounded
parallel preparation workers. Conflict ownership is granted in stable
admission order using canonical compiler-derived keys. Workers may perform
snapshot reads, deterministic evaluation, validation, and typed record
preparation concurrently.

Prepared results enter a bounded admission-ordinal reorder buffer. The sole
authoritative writer commits only the earliest contiguous compatible commands
and assigns sequences in that order. A slow earlier command may cause bounded
head-of-line waiting; later work MUST NOT silently overtake it. Every
influential observation is still reread and compared inside the final write
transaction, and existing retry, cancellation, uncertainty, and fencing rules
remain in force.

Parallelism does not relax deterministic-runtime restrictions. Workers receive
only explicit immutable inputs, checked snapshot observations, and owned
capabilities. They perform no network, filesystem, operating-system clock,
process-global mutation, or untracked randomness.

### Performance gate

Evidence MUST decompose queue formation, idempotency selection, admission,
evaluation, validation, encoding, commit/flush, publication, storage-gate wait,
query parsing/cache, and response work using bounded redaction-safe labels.

The public benchmark MUST add 1-, 8-, 32-, and 64-client read, write, and mixed
loads over reused connections. The hard gate is same-run application parity,
not merely a faster internal storage microbenchmark. Missing a gate blocks
WP-370 and MUST NOT be repaired by weakening durability, authorization,
audit, atomicity, idempotency, provenance, boundedness, or snapshot semantics.

## Consequences

- Low-load commands may incur up to the already accepted 200-microsecond
  grouping delay.
- Read traffic can use redb MVCC instead of contending on a process-wide mutex.
- Historical replay remains correct while its initial lookup is amortized and
  removed from the writer lane.
- A bounded preparation pool can use multiple cores without creating another
  authoritative writer or nondeterministic commit order.
- Activation and scheduler tests must now cover more explicit typed states,
  barriers, queue bounds, and shutdown paths.

## Rejected alternatives

- **Delete historical idempotency inspection.** Replays need its immutable
  historical plan selection; admission recheck alone occurs too late for
  service-side plan validation and authorization.
- **Allow commands to overtake capability revocation or deployment.** This
  could execute under authority or catalog state that should already be an
  ordering barrier.
- **Clone the complete operational storage bundle for every consumer.** It
  obscures the sole-writer proof; only narrow reader handles are cloneable.
- **Acknowledge before the durable commit while workers continue.** This
  violates the uncertainty and durability contract.
- **Cache by names or pointers without hashes.** This admits stale or
  substituted contract/query material.
- **Skip validation for recovered or external bytes.** Only sealed typed
  in-process records qualify for redundant-work elimination.

## Acceptance reference

The human maintainer explicitly approved this scheduler, admission, activated
storage-lane, immutable-artifact, parallel-preparation, and concurrent parity
plan in the current Codex session on 2026-07-30. The approval retained one
ordered authoritative writer, redb `Immediate` durability, current-policy
authorization, hard control-plane barriers, transaction-current dependency
validation, and every independent command/audit/idempotency guarantee.
