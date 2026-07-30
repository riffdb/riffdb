# ADR-0059: Same-Partition Read Dependencies and Application Write Parity

- **Status:** Accepted
- **Date:** 2026-07-30
- **Decision owners:** RiffDB maintainers
- **Related requirements:** `PERF-004`, `PERF-005`, `TXN-001`, `TXN-004`,
  `TXN-005`
- **Related work packages:** `WP-364`, `WP-366`
- **Amends:** ADR-0004, ADR-0016, ADR-0058

## Context

Bounded group durability removed redundant durable transitions, but the
TicketDesk contract still assigned every row in an organization to one
organization-wide conflict domain. The public batch client consequently kept
the writer busy while almost every command completion remained incompatible
with the next one. Increasing client concurrency could not repair a
compiler-created serialization boundary.

The compiler also rejected a command that read one aggregate and mutated
another, even when both aggregates were provably in the same partition. That
restriction is stronger than ADR-0004: influential reads outside the mutation
domain are safe when they remain explicit snapshot observations and are
revalidated exactly inside the authoritative commit transaction.

We need to recover ordinary row- and business-aggregate concurrency without
introducing SQL transactions, hidden dependencies, cross-partition work, or
multi-aggregate writes.

## Decision

### One mutation aggregate

Every grammar-v1 command containing `create` or `mutate` bindings MUST have
exactly one mutation aggregate. All such bindings MUST be owned by that same
aggregate. The compiler MUST reject a command that creates or mutates entities
owned by different aggregates.

The mutation aggregate's declared conflict-key expressions remain the only
source of logical conflict keys. Every mutation binding derives its key before
runtime execution, and the existing canonical ordering, deduplication, count,
and size bounds remain unchanged.

This rule does not require one mutated entity. Multiple roots or children may
be changed atomically when they belong to the same declared aggregate and its
conflict domain.

A prior exact `create` or `mutate` binding to a relationship target proves the
same existence condition as a prior exact `read`: its failure outcome exits
before the referencing mutation, and its successful record participates in
the same atomic command. Relationship validation MUST still reject a missing,
partial, differently keyed, or later target binding.

### Same-partition external reads

A command MAY read entities owned by other aggregates only when the compiler
proves that every binding's partition expression is identical after
substituting command-input expressions.

Every influential external read:

- remains a declared command binding;
- is captured as exact entity observation evidence;
- is re-read and compared inside the authoritative commit transaction;
- participates in retry decisions exactly as any other influential read; and
- cannot contribute a hidden or runtime-discovered conflict key.

Cross-partition reads remain rejected. Binding keys remain input-computable;
they cannot depend on a prior binding, the clock, ambient state, or an
untracked effect.

Read-only commands MAY span aggregates under the same partition proof. They
derive no mutation conflict key and retain the existing snapshot and
unjournaled read-only semantics.

### Contract modeling

Contracts SHOULD declare the smallest aggregate that owns an actual atomic
business invariant. Parent existence, foreign-key validity, or another
read-only precondition does not by itself justify placing all referenced
entities in one conflict domain.

The TicketDesk evidence contract is remodeled as:

- one organization aggregate;
- one user aggregate per user;
- one project aggregate containing its membership rows;
- one ticket aggregate containing its comments and label links; and
- one label aggregate per label.

This preserves atomic multi-row ticket and project-member commands while
allowing independent tickets, projects, users, and labels to group safely.

### Performance evidence

Server evidence MUST distinguish requested public batch concurrency from the
effective durable completion-group distribution. At minimum it records the
number of physical completion commits by logical group size from 1 through 64,
without identifiers, values, credentials, or other high-cardinality labels.

On the checked reference-machine profile, the full TicketDesk seed MUST:

- use only ordinary public symbolic commands;
- retain redb `Immediate` durability and the two-transition audit protocol;
- publish the effective completion-group distribution; and
- complete in no more than twice the duration of the same-run PostgreSQL seed.

The representative unary mutation p50 MUST also be no more than twice the
same-run PostgreSQL value. A miss blocks Agent Application Alpha and requires
another measured optimization or an explicit product decision; it MUST NOT be
waived by weakening safety or using a direct import/storage escape hatch.

### Bounded command batching

The public application client MAY submit up to 16 ordinary command envelopes
in one transport exchange. This is an admission and durability-grouping hint,
not a bulk mutation primitive:

- every item has its own request ID, idempotency key, compiled command,
  authorization decision, deterministic evaluation, durable outcome,
  provenance, and audit lifecycle;
- items are not one atomic transaction and no item may observe another item in
  the batch;
- one physical completion group MUST reject exact read/write and write/write
  overlap in both FIFO directions, so every accepted command remains valid
  against the transaction's final staged state;
- the server MUST validate the complete bounded wire shape before submitting
  any item;
- results remain input ordered, and transport uncertainty is resolved by
  replaying or looking up each original idempotency identity; and
- the API-neutral application service and commit coordinator remain the only
  execution and authoritative-mutation path.

The adapter may authenticate the shared transport envelope once, but current
policy authorization remains mandatory for every contained command. The
batch surface MUST NOT accept field IDs, write sets, transaction callbacks, or
storage records.

The internal FIFO writer MAY coalesce compatible commands from multiple public
transport exchanges into one physical admission or completion group of at most
64 commands, matching the pre-existing authoritative transaction command
ceiling. This changes neither public batching nor application semantics. Every
item retains FIFO admission, independent authorization, identity, outcome,
provenance, audit, acknowledgement, and uncertainty recovery. Commands in one
physical group MUST still pass exact bidirectional read/write and write/write
compatibility checks, MUST NOT observe one another, and MUST fit the unchanged
16 MiB authoritative transaction ceiling. A group that cannot meet every
bound or compatibility rule MUST NOT be committed as that group.

## Consequences

- Compiler-proven external reads no longer force unrelated entities into one
  conflict domain.
- Ordinary TicketDesk seed work can fill compatible redb durable groups.
- Batch callers amortize transport admission without receiving a privileged
  write path or broader atomicity.
- Aggregate boundaries become a visible correctness and concurrency design
  choice rather than an organization-wide default.
- Commands that truly need atomic writes across two current aggregates must
  remodel those entities into one aggregate or remain rejected.
- Exact transaction-current observation revalidation may still abort and retry
  a command when an externally read entity changes.

## Rejected alternatives

- **Raise client concurrency only.** It cannot overcome organization-wide
  conflict keys and provides no effective-group evidence.
- **Exclude external reads from commit revalidation.** That admits stale
  decisions and violates deterministic command safety.
- **Use read aggregates as conflict domains.** This is safe but recreates
  unnecessary serialization; exact observation validation already detects
  changes.
- **Permit multi-aggregate writes.** It weakens static conflict ownership and
  turns locality into runtime transaction discovery.
- **Use a direct bulk insert path.** It would bypass the command, outcome,
  provenance, authorization, and audit model being measured.
- **Replace redb now.** The current evidence identifies compiler-created
  conflict scope, not an engine limit.

## Acceptance reference

The human maintainer explicitly accepted this exact rule and its performance
gate in the current Codex session on 2026-07-29: same-partition external reads,
one compiler-proven mutation aggregate, mutation-only conflict keys, exact
transaction-current read revalidation, no cross-partition reads, and no
multi-aggregate writes.

On 2026-07-30 the maintainer explicitly approved the narrow internal grouping
amendment from 16 to 64 commands. The public transport batch remains 16; the
two-transition protocol, redb `Immediate` durability, exact compatibility
checks, independent command semantics, and the 16 MiB ceiling remain
normative.
