# Command and invariant cookbook

These patterns preserve RiffDB's application safety rule: writes are
accepted only through compiled commands whose complete data dependencies,
conflicts, invariants, outcomes, and partition route are known before runtime.

A contract does not pick a grammar version. The compiler assigns the least
sufficient one for the constructs the contract actually uses, so a contract
built only from the patterns below keeps the V1 identity, and one that reaches
for a newer construct — partition-local cross-aggregate writes, high-cardinality
atomic collections — selects the version that admits it. The current maximum is
V23. `SPEC.md` §7 defines the base surface and its changelog records what each
later version added.

## Create an aggregate root

```riff
command CreateCollection {
  input idempotency_key: string<128>
  input collection_id: uuid
  input name: string<96>
  idempotency_key idempotency_key
  create Collection(collection_id) as collection
    else CollectionExists { collection_id: collection_id }
  set collection.name = name
  set collection.created_at = tx.time
  return Created { collection: collection }
}
```

The idempotency input is caller-stable across retries. `create ... else`
declares duplicate handling as a typed business outcome. Every required
non-key field is assigned exactly once.

## Create a related child safely

```riff
command AddArtifact {
  input idempotency_key: string<128>
  input collection_id: uuid
  input artifact_id: uuid
  input title: string<128>
  idempotency_key idempotency_key
  read Collection(collection_id) as collection
    else CollectionMissing { collection_id: collection_id }
  create Artifact(collection_id, artifact_id) as artifact
    else ArtifactExists { artifact_id: artifact_id }
  set artifact.title = title
  set artifact.created_at = tx.time
  return Created { artifact: artifact }
}
```

The dominating exact read proves the required relationship target exists in
the same partition. Do not replace it with an application preflight read:
check-then-write races are exactly what the compiled command prevents.

## Enforce state rules in the command

```riff
require StillOpen: collection.closed == false
  else CollectionClosed { collection_id: collection_id }
```

Place a requirement after the binding that supplies its fields. The condition
is evaluated transaction-current inside the command. The rejection is a
declared typed outcome, not an exception inferred by client code.

Entity or aggregate invariants use:

```riff
invariant NonnegativeQuantity: quantity >= 0
```

An invariant is part of compilation and mutation validation. Do not reproduce
it with a prior RiffQL read or a generated-client branch.

## Change a relationship

Read the complete new target key before mutating relationship fields:

```riff
read Shelf(collection_id, new_shelf_id) as shelf
  else ShelfMissing { shelf_id: new_shelf_id }
mutate Artifact(collection_id, artifact_id) as artifact
  else ArtifactMissing { artifact_id: artifact_id }
overwrite artifact.shelf_id = new_shelf_id
```

The compiler rejects a relationship-changing command without the dominating
target read. Required references use required fields; optional relationships
must use a contract-supported optional shape.

## Say which writes are last-writer-wins

A write is spelled `set` when the command constrains the record's prior value,
and `overwrite` when it does not. The compiler proves which one is true and
refuses the other spelling, so the two cannot be confused and a write's
concurrency semantics are legible where the write is.

A binding is constrained when a `require`, a decision arm's predicate, or the
value of any write to it reads one of its stored fields. That makes it per
record, not per field: a command that bumps `state.revision = state.revision +
1` constrains `state`, so its other writes to `state` are `set` too.

```riff
mutate Slot(tenant_id, slot_id) as slot else SlotMissing {}
require is_free: slot.free else SlotTaken {}
set slot.free = false
set slot.held_by = holder
```

Delete the `require` and the command stops compiling rather than quietly
becoming last-writer-wins. That is the point: two callers would both succeed
and the second would silently replace the first holder.

Not every unconditional write is a mistake. Replacing a display name, or a
relationship the caller names outright as above, is last-writer-wins by intent.
Write `overwrite` and it compiles; a reviewer then sees the choice instead of
having to reconstruct it. An `overwrite` is refused on a `create`, which has no
prior value to overwrite.

## Unique values

Declare uniqueness on the entity:

```riff
unique collection_name (collection_id, name)
```

The complete partition prefix must lead the unique key. Commands that create or
change the tuple receive compiler-derived transaction-current occupancy and
conflict checks. A `query` followed by a write is not a uniqueness mechanism.

## Safe retry rule

Reuse the exact command name, input, role, contract identity, and idempotency
key until RiffDB returns the stored terminal outcome. Never generate a new key
to escape an uncertain response. Generated command helpers own this behavior.

## Patterns intentionally unavailable

- raw insert, update, delete, or SQL;
- arbitrary transaction callbacks;
- network, filesystem, wall-clock, or process-global access in commands;
- client-side check-then-write integrity;
- cross-partition mutation;
- undeclared business exceptions;
- unbounded loops, recursion, scans, or collections; and
- manually supplied conflict keys, plans, IDs, field masks, or capability
  grants.

An unavailable pattern is not an invitation to work around the compiler. Model
the invariant, relationship, aggregate, command, or declared outcome so RiffDB
can prove it.
