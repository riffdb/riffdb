# Atomic indexed selection

An allocation command can choose and change the first available indexed row in
one transaction. The caller supplies the pool and business inputs; RiffDB owns
the bounded selection and contention retry. This removes the application-side
query/choose/claim loop for this supported shape.

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
  conflict_key (pool_id)
}
command Allocate {
  input request: uuid
  input pool_id: uuid
  input holder: uuid
  idempotency_key request
  mutate first Slot.by_available(pool_id, true) as slot else NoCapacity {}
  require available: slot.available else NoCapacity {}
  set slot.available = false checked_by available
  set slot.holder = holder checked_by available
  return Allocated { slot: slot }
}
```

Place these declarations inside a versioned contract. `first` is contextual;
existing entities or inputs named `first` keep their meaning.

## What selection guarantees

The compiler proves that the equality prefix fixes one partition and that every
conflict key is computable before reading state. The example uses one pool
conflict key. A conflict key including the unknown `slot_id` cannot support this
selection. Companions in other aggregates are allowed when their exact targets
come from input and they share the proved partition route. Every written
aggregate retains its own invariants.

Selection seeks the prefix and retains its first complete index key in ascending
canonical order, including the entity-key tie breaker. It reads at most one
candidate per evaluation. The index entry, row, and required aggregate roots
come from one consistent storage view. There is no application callback inside
the transaction.

Before committing any outcome, including `NoCapacity`, RiffDB revalidates the
partition/index generation and exact row/root observations. Changed evidence
causes complete reevaluation under the same input, logical context, and
idempotency identity. The existing limit is three evaluations, subject to the
request deadline and cancellation. Exhaustion returns the existing typed
retryable failure. Selection promises no fairness or starvation bound.

A requirement failing on the first candidate returns its declared failure. It
does not search further. Row-policy denial likewise returns the redacted denial;
it never skips a candidate or reports it as an empty pool. The usual command,
capability, relationship, and secret-flow controls still apply.

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

## POC bounds and compatibility

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
