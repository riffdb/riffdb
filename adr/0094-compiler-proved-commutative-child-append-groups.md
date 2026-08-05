# ADR-0094: Compiler-Proved Commutative Child-Append Groups

- **Status:** Accepted
- **Date:** 2026-08-05
- **Decision owners:** RiffDB maintainers
- **Related requirements:** `TXN-001`, `TXN-013`, `PERF-004`, `PERF-005`,
  `PERF-008`
- **Related work packages:** `WP-451`, `WP-454`
- **Amends:** ADR-0059, ADR-0060

## Context

Fresh write-only evidence showed that the sole writer remained saturated while
TicketDesk `CreateComment` traffic was repeatedly split at the ticket aggregate
conflict key.  Exact entity writes were disjoint, but ADR-0059 required every
shared mutable conflict key to split the physical group.  That preserved safety
but serialized independent comment appends to one ticket and prevented durable
groups from filling under the ordinary hot-aggregate workload.

The conflict key protects more than row overlap: it may represent a root
mutation, uniqueness domain, aggregate invariant, or range dependency.  A
scheduler therefore cannot infer commutativity merely because exact entity keys
differ.  Any relaxation must be a compiler-owned negative proof over the entire
checked command plan and must fail closed as the contract evolves.

## Decision

Commands sharing an aggregate conflict key MAY participate in one physical
completion group only when every sharing command has a compiler-derived
**commutative child-append proof** and the existing bidirectional exact-access
check also succeeds.

The proof exists only when the checked command:

- is an idempotent mutation in exactly one declared aggregate;
- contains at least one `create` binding and every writable binding is a
  `create` of a child entity owned by that aggregate;
- has no aggregate-root or existing-child mutation;
- has no unique-conflict plan, root/range validation read, commit check, or
  `require` predicate;
- uses the existing input-computable complete entity keys; and
- retains every ordinary exact read as transaction-current dependency evidence.

Events, typed outcomes, provenance, authorization, admission, idempotency, and
audit semantics are unchanged.  A future compiler feature that introduces a
new invariant or dependency category MUST make the proof unavailable until an
accepted amendment defines its safe treatment.

The coordinator still rejects exact read/write and write/write overlap in both
FIFO directions.  Thus two appends with the same complete child key do not
co-commit, and a child append cannot co-commit with a root or child mutation.
Commands evaluate against the same pre-group state and cannot observe another
item in the group.

### Capability ownership

For an accepted shared-key group, the coordinator acquires the sorted,
deduplicated union of all compiler-derived conflict keys once.  One sealed,
crate-private group authority owns that move-only lease until every grouped
attempt has committed, rolled back, terminalized, or been dropped.  Individual
attempts may retain reference-counted ownership only through that sealed group
authority; no transport, runtime, generated client, or user code can construct,
clone, serialize, or inspect it.

Any missing proof, failed exact-access check, cancellation, deadline, retry,
storage rejection, or integrity mismatch falls back to the existing strict
per-command conflict ownership.  There is no optimistic execution outside a
held capability and no second writer.

### Ordering and acknowledgement

The FIFO scheduler does not overtake or reorder commands to form these groups.
Hard barriers, count and byte ceilings, durability, commit-sequence assignment,
per-command results, and acknowledgement-after-durability remain unchanged.
The resulting history must be observationally equivalent to a FIFO serial order
of the grouped child appends.

## Consequences

- Independent comments or junction rows under one hot aggregate can amortize a
  physical durable commit without weakening aggregate invariants.
- Adding a root write, uniqueness rule, command requirement, or commit invariant
  automatically removes eligibility without a scheduler change.
- The proof is derived rather than persisted, so no bundle or durable-format
  compatibility boundary changes.
- A sealed shared lease adds internal lifetime complexity and therefore requires
  cancellation, rollback, same-child-key, root-write, invariant, and crash
  regression coverage.

## Rejected alternatives

- **Treat disjoint exact keys as sufficient.** Conflict keys may protect
  non-row invariants and ranges; this would be unsound.
- **Let contract authors annotate commands as commutative.** An assertion is not
  a proof and would drift when a command evolves.
- **Overtake hot commands to find cold keys.** This changes FIFO scheduling and
  is unnecessary for the observed child-append workload.
- **Release the aggregate lease after evaluation.** A root mutation could then
  invalidate grouped observations before commit.
- **General serial micro-batching now.** It is a larger semantic mechanism and
  remains a separately gated fallback if this narrow proof does not meet the
  alpha target.

## Acceptance reference

On 2026-08-05 the maintainer explicitly approved this rule: commands sharing an
aggregate conflict key may co-commit only when the compiler proves disjoint
input-derived child appends with no root mutation, mutable existing row,
cross-command invariant, or range dependency; all other commands retain strict
serial conflict ownership.
