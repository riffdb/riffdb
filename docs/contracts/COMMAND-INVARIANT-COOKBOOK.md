# Command and invariant cookbook

These grammar-v1 patterns preserve RiffDB’s application safety rule: writes are
accepted only through compiled commands whose complete data dependencies,
conflicts, invariants, outcomes, and partition route are known before runtime.

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
set artifact.shelf_id = new_shelf_id
```

The compiler rejects a relationship-changing command without the dominating
target read. Required references use required fields; optional relationships
must use a contract-supported optional shape.

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
