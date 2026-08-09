# ADR-0107: Compiler-Bounded Collection Mutations

- **Status:** Proposed
- **Direction approved:** 2026-08-09
- **Exact text accepted:** No
- **Decision deadline:** Before WP-560 changes contract grammar or command IR
- **Requires:** ADR-0002, ADR-0003, ADR-0005, ADR-0012, ADR-0031,
  ADR-0055, ADR-0059, ADR-0093, ADR-0095, ADR-0100, and ADR-0104
- **Amends if accepted:** ADR-0100's closed changelog-entry algebra, plus
  `DSL-001`, `PERF-004`, and `PERF-005`
- **Defines or blocks:** WP-559 through WP-562 and WP-570

## Context

The existing public batch is intentionally a bounded collection of independent
commands. It cannot express one atomic OpenFGA tuple set, MLflow metric batch,
Payload document graph, or pipeline-plus-step creation without many round
trips and visible intermediate states. A generic transaction or bulk-write API
would bypass the compiler visibility that makes RiffDB safe.

The current grammar forbids every loop and requires one mutation aggregate.
Those rules must not be relaxed into runtime-discovered access. Alpha instead
needs one narrowly bounded collection construct whose complete maximum access
and mutation shape remains compiler-owned.

## Proposed Decision

The contract language adds an explicit `bulk command` declaration and bounded
collection input types. It is not inferred from an ordinary command and is not
available through a generic request builder.

Illustrative syntax:

```riff
bulk command PutTuples {
  input organization_id: OrganizationId
  input tuples: list<TupleInput, 1..256>

  for tuple in tuples {
    create relation from Relationship(tuple.object, tuple.relation, tuple.user)
    set relation.organization_id = organization_id
    set relation.object = tuple.object
    set relation.relation = tuple.relation
    set relation.user = tuple.user
  }

  retry deduplicated(request_id)
  outcomes Written<Count> | AlreadyPresent<TupleKey> | InvalidInput<FieldErrors>
}
```

`for` is a compiler-owned bounded expansion over exactly one submitted list. It
is not a general loop. Nested iteration, recursion, collection growth, dynamic
dispatch, host callbacks, scans, query-result iteration, and while/until forms
remain forbidden.

### Static access and locality proof

The element body has one closed typed plan. Every key, partition route,
relationship target, unique key, conflict key, mutation, event, and capacity
charge must be computable from scalar command input, the current element, and
the deterministic transaction context before authoritative staging.

All elements route to one exact partition. The compiler derives a bounded set
of mutation aggregate instances from the submitted elements, sorts and
deduplicates their complete conflict capabilities canonically, and rejects the
request before evaluation if count, bytes, partition, duplicate-key, or access
proof fails. This is an explicit amendment to ADR-0059's one-mutation-aggregate
rule only for an accepted `bulk command`; ordinary commands remain unchanged.

The first release caps one collection at 256 elements, one bulk command at 256
distinct mutation aggregate instances, and the complete canonical input plus
write graph at the existing 16 MiB command-transaction ceiling. The compiler
may impose a lower per-command bound. No caller can raise a compiled maximum.

### Atomic semantics and outcomes

One bulk invocation is one command with one idempotency identity, canonical
input hash, authorization decision, deterministic context, declared outcome,
commit sequence, provenance record, audit lifecycle, and atomic durable graph.
It is all complete or absent. It is not the current `ExecuteBatch` surface and
does not return independent per-element commit outcomes.

Element order is the canonical submitted list order and participates in the
input hash. Conflict acquisition uses canonical sorted keys but evaluation and
event ordinals use submitted order. Duplicate elements are rejected unless the
command explicitly declares a compiler-checked idempotent duplicate policy.

A deterministic business failure selects one declared whole-command outcome
and persists no partial mutation. The outcome may identify a bounded symbolic
element key but may not echo the complete submitted element or hidden values.

### Creates, updates, and deletes

Repeated create and update bindings reuse the ordinary checked entity,
relationship, uniqueness, invariant, and transaction-current validation paths.
No blind overwrite or upsert is introduced.

The language also adds explicit checked `delete` bindings. Deletion is not a
generic storage tombstone. A deletable entity must declare a deletion policy.
The first release permits:

- no declared inbound relationship; or
- `restrict`, backed by a compiler-proved bounded reverse-reference index and
  an exact empty dependency revalidated at commit.

Cascade, set-null, orphaning, cross-partition delete, unindexed inbound
reference discovery, history purge, and physical erasure are rejected. A
delete removes current entity/index state atomically while immutable commands,
outcomes, events, provenance, and history remain retained under existing
policy. Delete event emission is explicit and bounded like every other event.

### Replication and durable-validation prerequisite

Checked deletion is not enabled merely by accepting its compiler syntax. The
accepted ADR-0093/ADR-0100 changelog currently derives insert-or-replace entry
classes only; it has no representation for removal of current entity or index
state. Shipping a delete without changing that closed entry algebra would let
the primary advance while a follower retained deleted state.

Before WP-560 can accept a production delete plan, WP-559 must amend ADR-0100
and implement a versioned delete/tombstone changelog entry class through frame
encoding, validation, shipping, follower apply, bootstrap, and compatibility
fixtures. The entry binds the database/history identity, frame and command
sequence, canonical table class and key, prior-value identity, and entity-chain
transition. Repetition is idempotent only for the exact same transition;
missing, stale, reordered, or mismatched tombstones fail closed. A delete is
therefore an ordinary sequence-attributed authoritative transition, not
retention and not physical history purge. Its type and namespace are also
distinct from ADR-0101/ADR-0104 composite-view overlay tombstones and ADR-0085
retention-range tombstones; none can be decoded or accepted as another.

WP-559 also owns a delete-aware audit of the GB validated-prefix checkpoint.
Any proof using `len() == count-at-bound`, or an entity-chain fingerprint that
assumes counted current-state tables only grow or replace, must be amended
before command-time deletes exist. The replacement must distinguish live-row
cardinality from historical transition count, incorporate tombstones into the
canonical entity-chain fingerprint, and prove checkpoint/bootstrap equality
across create-update-delete-recreate histories. Retention deletion below a
validated bound remains a different operation and cannot satisfy this audit.

Until both the changelog entry and validation proof pass, the compiler rejects
`delete` with a typed feature-unavailable diagnostic. Repeated creates and
updates do not depend on the tombstone work and may proceed independently.

### Runtime and recovery

The deterministic runtime receives a sealed collection plan and precharged
input. It cannot allocate proportional work beyond the compiled bound. Storage
receives one complete semantic command graph, not an iterator, callback,
transaction handle, raw key/value batch, or partially validated chunk.

Cancellation before staging leaves the command absent. After staging,
uncertainty resolves the one original idempotency identity. Recovery validates
the complete graph and its per-element ordering/reciprocity; it never resumes
from a partial element checkpoint.

## Options Considered

1. **Raise independent command-batch concurrency:** rejected because it does
   not supply atomic graph creation or eliminate intermediate visible state.
2. **Generic transaction callback or bulk row API:** rejected because access,
   invariants, authority, and outcomes become runtime conventions.
3. **General bounded loops in every command:** rejected because the grammar and
   IR would become a programming language with harder dependency analysis.
4. **A distinct compiler-bounded bulk command:** proposed because the wider
   mutation set remains explicit, local, finite, typed, and reviewable.

## Consequences

- Common adapter batch writes become one typed atomic application operation.
- Conflict scope grows with submitted elements and may serialize large hot-key
  sets; this cost is visible in explain output and hard-bounded.
- The command IR and durable graph gain collection/delete compatibility work.
- Arbitrary transactions, partial success, cascades, and runtime-discovered
  keys remain unavailable.

## Compatibility

Grammar, command IR, plan hash, generated schemas, protocol values, and durable
command-graph formats require additive successors and golden fixtures. Existing
ordinary commands, public batches, outcomes, and stored history retain their
exact meanings. A contract using bulk or delete requires the new language/IR
version and cannot be silently downcompiled.

## Security

Authorization derives the complete maximum field/entity/operation authority
from the sealed plan, then validates the exact instantiated partition and keys.
Diagnostics are bounded and value-free. No element can select another command,
entity type, field, index, partition, or operation dynamically.

## Standing Design Tests

- **Interface safety:** callers can express only compiler-declared collection
  work; they cannot submit mutations, keys, callbacks, SQL, or delete policy
  outside the plan. There is no opt-out from atomicity, constraints, audit,
  idempotency, or provenance.
- **Scale:** one invocation is deliberately single-partition and capped at 256
  elements/16 MiB. It does not require full-state scans or memory residency and
  remains routable to one future partition leader.

## Testing

- Parser/formatter/source-span and IR old/current golden fixtures.
- Property tests comparing bounded expansion with a pure reference evaluator.
- Negative corpus for nesting, dynamic keys, mixed partitions, excess
  aggregates, duplicate elements, missing references, uniqueness, and deletes.
- Deterministic concurrency schedules for overlapping bulk/unary commands.
- Crash arms before/after evaluation, staging, fence, response, and replay.
- OpenFGA, MLflow, Payload, and Woodpecker-shaped acceptance contracts with no
  generic write or handwritten transaction glue.

## Requirements and Work Packages

- **Provisional requirements:** `BLK-001` through `BLK-014`, to be added to
  `SPEC.md` only after exact acceptance.
- **Defines or blocks:** WP-559, WP-560, WP-561, WP-562, and WP-570.
- **Final evidence:** WP-562 and WP-570.

## Decision Deadline

Exact acceptance is required before grammar, IR, deletion policy, multi-
aggregate conflict proof, protocol, or durable graph changes.
