# Atomic indexed selection

An allocation command can choose and change an available indexed row in one
transaction. The caller supplies the pool and business inputs; RiffDB owns the
bounded selection and contention retry. This removes the application-side
query/choose/claim loop for this supported shape.

RiffDB supports two selection modes:

- `mutate first <Entity>.<index>(<prefix_args>) as <alias> else <Outcome> { ... }`
  selects the globally first eligible entry in canonical index order within a
  pool conflict domain.
- `mutate any <Entity>.<index>(<prefix_args>) as <alias> else <Outcome> { ... }`
  allocates any eligible entry from an unowned pool by discovering a bounded
  candidate window (`K <= 16`), acquiring per-candidate root conflict keys
  non-blockingly (`conflict_key (pool_id, slot_id)`), and committing with
  point validation so concurrent callers on disjoint slots do not serialize on a
  single pool lock.

```riff
entity Slot {
  key (pool_id: uuid, slot_id: uuid)
  field available: bool
  field holder: uuid
  index by_available (pool_id, available, slot_id)
}
aggregate Slots {
  root Slot
  partition_by pool_id
  conflict_key (pool_id, slot_id)
}
command AllocateAny {
  input request: uuid
  input pool_id: uuid
  input holder: uuid
  idempotency_key request
  mutate any Slot.by_available(pool_id, true) as slot else NoCapacity {}
  require available: slot.available else NoCapacity {}
  set slot.available = false checked_by available
  set slot.holder = holder checked_by available
  return Allocated { slot: slot }
}
```

Place these declarations inside a versioned contract. `first` and `any` are
contextual; existing entities or inputs named `first` or `any` keep their
meaning.

## What selection guarantees

The compiler proves that the equality prefix fixes one partition. For `mutate first`,
every conflict key must be computable before reading state (such as `conflict_key (pool_id)`),
and same-pool `mutate first` requests participate in transaction-local serial
micro-batching. For `mutate any`, the aggregate's `conflict_key` may include the
selected aggregate root's primary key fields (such as `conflict_key (pool_id, slot_id)`),
allowing per-candidate conflict ownership while still rejecting child-entity keys
in `conflict_key`. Companions in other aggregates are allowed when their exact
targets come from input and they share the proved partition route. Every written
aggregate retains its own invariants.

For `mutate first`, selection seeks the prefix and retains its first complete
index key in ascending canonical order, including the entity-key tie breaker.
For `mutate any`, selection reads a bounded candidate window (`K <= 16` entries)
in canonical index order, derives each candidate's complete conflict-key set
(including companion conflict keys in canonical order), attempts non-blocking
conflict acquisition, and verifies the chosen candidate under its acquired lease.
If a candidate is held in-flight or was concurrently consumed before lease
acquisition, `mutate any` releases any held candidate authority and advances to
the next candidate in the window.

Before committing any outcome, including `NoCapacity`, RiffDB revalidates
dependencies. `mutate first` and empty (`NoCapacity`) outcomes validate the
`(partition, index)` generation epoch alongside row/root observations. A
non-empty `mutate any` selection uses point validation on the selected row's
version and index-prefix membership together with companion, root-validation,
relationship, and row-policy dependencies, so concurrent allocations of disjoint
slots in the same pool do not invalidate one another.

Proven absence (`0` matching index entries), candidate contention (all
candidates in the window locked in-flight), business `require` rejection, and
row-policy denial remain strictly separate outcomes. Only proven absence enters
the selector's `else` branch. Contention retries up to the evaluation budget and
surfaces as retryable contention; business rejection and row-policy denial never
skip past a candidate or report an empty pool.

Selection alone supplies no assignment evidence. Keep explicit business checks,
caller-version fences, or state-derived arithmetic as described in the
[write cookbook](COMMAND-INVARIANT-COOKBOOK.md#declare-the-evidence-for-each-write).

## Retry and uncertainty

An empty result is a durable business result. Repeating that request identity
returns the original `NoCapacity`, even after another command releases a slot.
Start a new business attempt with a new identity to try newly available capacity.
For an unknown commit status, resolve the original identity before making another
allocation. The generated command APIs retain their ordinary uncertainty rules.

## Choosing a pool

A pool conflict key makes selection safe by serializing writes in that pool.
Larger pools can concentrate contention; separate pools can reduce conflict-lock
sharing. Index generations remain conservative per partition and index: an index
change in another pool of the same partition can still invalidate an evaluation.
There is no automatic repartitioning or finer-grained generation guarantee.

## Bounds and compatibility

- One selector in the ordinary binding phase; no collection or decision-arm
  selector, cascade/restrict deletion, or second explicit binding to the selected
  entity type in the same command.
- Prefix arguments use only validated inputs and constants and canonical index
  field encodings. They must prove partition and complete conflict ownership.
  An index containing a secret field cannot drive selection: its membership or
  ordering would disclose that field without an explicit reveal site.
- No residual predicate, cursor, offset, reverse or random order, candidate page,
  skip-locked behavior, or caller-selected scan limit.
- A selected value cannot determine another explicit binding target. Required
  invariant-root reads are compiler-owned and may follow the selected root key.
- Cross-partition writes, cross-aggregate invariants, projection-based write
  decisions, and durable multi-command orchestration remain unavailable.

Selector contracts use grammar/IR/bundle V26 and require a compatible server.
Contracts without selectors keep their historical least-sufficient bytes and
identities. Deploy the exact bundle and regenerate its ordinary bindings and
lock artifacts; SDK, CLI, gRPC, and MCP do not expose a separate raw scan API.
Command explain describes order, prefix expressions, ownership, generation
validation, and bounds without exposing row values.

Selection retains one live candidate after the bounded checkpoint/journal merge.
That bound does not imply constant disk I/O, parallel throughput, or fairness.
